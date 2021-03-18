package streams.config

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.logging.internal.LogService
import org.neo4j.plugin.configuration.ConfigurationLifecycle
import org.neo4j.plugin.configuration.ConfigurationLifecycleUtils
import org.neo4j.plugin.configuration.EventType
import org.neo4j.plugin.configuration.listners.ConfigurationLifecycleListener
import streams.extensions.databaseManagementService
import streams.extensions.getDefaultDbName
import streams.extensions.isAvailable
import streams.utils.Neo4jUtils
import streams.utils.ProcedureUtils
import streams.utils.StreamsUtils
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

class StreamsConfig(private val log: Log, private val dbms: DatabaseManagementService) {

    companion object {
        private const val SUN_JAVA_COMMAND = "sun.java.command"
        private const val CONF_DIR_ARG = "config-dir="
        const val SOURCE_ENABLED = "streams.source.enabled"
        const val SOURCE_ENABLED_VALUE = true
        const val PROCEDURES_ENABLED = "streams.procedures.enabled"
        const val PROCEDURES_ENABLED_VALUE = true
        const val SINK_ENABLED = "streams.sink.enabled"
        const val SINK_ENABLED_VALUE = false
        const val CHECK_APOC_TIMEOUT = "streams.check.apoc.timeout"
        const val CHECK_APOC_INTERVAL = "streams.check.apoc.interval"
        const val CLUSTER_ONLY = "streams.cluster.only"
        const val CHECK_WRITEABLE_INSTANCE_INTERVAL = "streams.check.writeable.instance.interval"
        const val SYSTEM_DB_WAIT_TIMEOUT = "streams.systemdb.wait.timeout"
        const val SYSTEM_DB_WAIT_TIMEOUT_VALUE = 10000L
        const val POLL_INTERVAL = "streams.sink.poll.interval"
        const val INSTANCE_WAIT_TIMEOUT = "streams.wait.timeout"
        const val INSTANCE_WAIT_TIMEOUT_VALUE = 120000L

        private const val DEFAULT_TRIGGER_PERIOD: Int = 10000

        private const val DEFAULT_PATH = "."

        @JvmStatic private val cache = ConcurrentHashMap<String, StreamsConfig>()

        private fun getNeo4jConfFolder(): String { // sun.java.command=com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/myid/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=/home/myid/neo4j-enterprise-4.0.0-alpha09mr02/conf
            val command = System.getProperty(SUN_JAVA_COMMAND, "")
            return command.split("--")
                .map(String::trim)
                .filter { it.startsWith(CONF_DIR_ARG) }
                .map { it.substring(CONF_DIR_ARG.length) }
                .firstOrNull() ?: DEFAULT_PATH
        }

        fun getInstance(db: GraphDatabaseAPI): StreamsConfig = cache.computeIfAbsent(StreamsUtils.getName(db)) {
            StreamsConfig(log = db.dependencyResolver
                .resolveDependency(LogService::class.java)
                .getUserLog(StreamsConfig::class.java), db.databaseManagementService())
        }

        fun removeInstance(db: GraphDatabaseAPI) {
            val instance = cache.remove(StreamsUtils.getName(db))
            instance?.stop(true)
        }

        fun isSourceGloballyEnabled(config: Map<String, Any?>) = config.getOrDefault(SOURCE_ENABLED, SOURCE_ENABLED_VALUE).toString().toBoolean()

        fun isSourceEnabled(config: Map<String, Any?>, dbName: String) = config.getOrDefault("${SOURCE_ENABLED}.from.$dbName", isSourceGloballyEnabled(config)).toString().toBoolean()

        fun hasProceduresGloballyEnabled(config: Map<String, Any?>) = config.getOrDefault(PROCEDURES_ENABLED, PROCEDURES_ENABLED_VALUE).toString().toBoolean()

        fun hasProceduresEnabled(config: Map<String, Any?>, dbName: String) = config.getOrDefault("${PROCEDURES_ENABLED}.$dbName", hasProceduresGloballyEnabled(config)).toString().toBoolean()

        fun isSinkGloballyEnabled(config: Map<String, Any?>) = config.getOrDefault(SINK_ENABLED, SINK_ENABLED_VALUE).toString().toBoolean()

        fun isSinkEnabled(config: Map<String, Any?>, dbName: String) = config.getOrDefault("${SINK_ENABLED}.to.$dbName", isSinkGloballyEnabled(config)).toString().toBoolean()

        fun getSystemDbWaitTimeout(config: Map<String, Any?>) = config.getOrDefault(SYSTEM_DB_WAIT_TIMEOUT, SYSTEM_DB_WAIT_TIMEOUT_VALUE).toString().toLong()

        fun getInstanceWaitTimeout(config: Map<String, Any?>) = config.getOrDefault(INSTANCE_WAIT_TIMEOUT, INSTANCE_WAIT_TIMEOUT_VALUE).toString().toLong()
    }

    private val configLifecycle: ConfigurationLifecycle

    private enum class Status {RUNNING, STOPPED, CLOSED, UNKNOWN}

    private val status = AtomicReference(Status.UNKNOWN)

    private val mutex = Mutex()

    init {
        val neo4jConfFolder = System.getenv().getOrDefault("NEO4J_CONF", getNeo4jConfFolder())
        configLifecycle = ConfigurationLifecycle(DEFAULT_TRIGGER_PERIOD,
            "$neo4jConfFolder${File.separator}streams.conf",
            true, log, true, "streams.", "kafka.")
    }

    fun start() = runBlocking {
        if (log.isDebugEnabled) {
            log.debug("Starting StreamsConfig")
        }
        mutex.withLock {
            if (status.get() == Status.RUNNING) return@runBlocking
            try {
                // wait for all database to be ready
                val isInstanceReady = StreamsUtils.blockUntilFalseOrTimeout(getInstanceWaitTimeout()) {
                    if (log.isDebugEnabled) {
                        log.debug("Waiting for the Neo4j instance to be ready...")
                    }
                    dbms.isAvailable(100)
                }
                if (!isInstanceReady) {
                    log.warn("${getInstanceWaitTimeout()} ms have passed and the instance is not online, the Streams plugin will not started")
                    return@runBlocking
                }
                if (ProcedureUtils.isCluster(dbms)) {
                    log.info("We're in cluster instance waiting for the ${StreamsUtils.LEADER}s to be elected in each database")
                    // in case is a cluster we wait for the correct cluster formation => LEADER elected
                    Neo4jUtils.waitForTheLeaders(dbms, log) { configStart() }
                } else {
                    configStart()
                }
            } catch (e: Exception) {
                log.warn("Cannot start StreamsConfig because of the following exception:", e)
            }
        }
    }

    private fun configStart() = try {
        configLifecycle.start()
        status.set(Status.RUNNING)
        log.info("StreamsConfig started")
    } catch (e: Exception) {
        log.error("Cannot start the StreamsConfig because of the following exception", e)
    }

    fun stop(shutdown: Boolean = false) = runBlocking {
        if (log.isDebugEnabled) {
            log.debug("Stopping StreamsConfig")
        }
        mutex.withLock {
            val status = getStopStatus(shutdown)
            if (this@StreamsConfig.status.get() == status) return@runBlocking
            configStop(shutdown, status)
        }
    }

    private fun configStop(shutdown: Boolean, status: Status) = try {
        configLifecycle.stop(shutdown)
        this.status.set(status)
        log.info("StreamsConfig stopped")
    } catch (e: Exception) {
        log.error("Cannot stop the StreamsConfig because of the following exception", e)
    }

    private fun getStopStatus(shutdown: Boolean) = when (shutdown) {
        true -> Status.CLOSED
        else -> Status.STOPPED
    }

    fun setProperty(key: String, value: Any, save: Boolean = true) {
        configLifecycle.setProperty(key, value, save)
    }

    fun setProperties(map: Map<String, Any>, save: Boolean = true) {
        configLifecycle.setProperties(map, save)
    }

    fun removeProperty(key: String, save: Boolean = true) {
        configLifecycle.removeProperty(key, save)
    }

    fun removeProperties(keys: Collection<String>, save: Boolean = true) {
        configLifecycle.removeProperties(keys, save)
    }

    fun reload() {
        configLifecycle.reload()
    }

    fun addConfigurationLifecycleListener(evt: EventType,
                                          listener: ConfigurationLifecycleListener) {
        if (log.isDebugEnabled) {
            log.debug("Adding listener for event: $evt")
        }
        configLifecycle.addConfigurationLifecycleListener(evt, listener)
    }

    fun removeConfigurationLifecycleListener(evt: EventType,
                                             listener: ConfigurationLifecycleListener) {
        if (log.isDebugEnabled) {
            log.debug("Removing listener for event: $evt")
        }
        configLifecycle.removeConfigurationLifecycleListener(evt, listener)
    }

    fun getConfiguration(): Map<String, Any> = ConfigurationLifecycleUtils.toMap(configLifecycle.configuration)

    fun defaultDbName() = this.dbms.getDefaultDbName()

    fun isDefaultDb(dbName: String) = this.defaultDbName() == dbName

    fun isSourceGloballyEnabled() = Companion.isSourceGloballyEnabled(getConfiguration())

    fun isSourceEnabled(dbName: String) = Companion.isSourceEnabled(getConfiguration(), dbName)

    fun hasProceduresGloballyEnabled() = Companion.hasProceduresGloballyEnabled(getConfiguration())

    fun hasProceduresEnabled(dbName: String) = Companion.hasProceduresEnabled(getConfiguration(), dbName)

    fun isSinkGloballyEnabled() = Companion.isSinkGloballyEnabled(getConfiguration())

    fun isSinkEnabled(dbName: String) = Companion.isSinkEnabled(getConfiguration(), dbName)

    fun getSystemDbWaitTimeout() = Companion.getSystemDbWaitTimeout(getConfiguration())

    fun getInstanceWaitTimeout() = Companion.getInstanceWaitTimeout(getConfiguration())

}