package streams

import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.rnorth.ducttape.unreliables.Unreliables
import org.slf4j.LoggerFactory
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy
import org.testcontainers.containers.wait.strategy.WaitAllStrategy
import org.testcontainers.containers.wait.strategy.WaitStrategy
import org.testcontainers.utility.MountableFile
import streams.utils.StreamsUtils
import java.io.File
import java.time.Duration
import java.util.concurrent.TimeUnit

private class DatabasesWaitStrategy(private val auth: AuthToken) : AbstractWaitStrategy() {
    private var databases = arrayOf<String>()

    fun forDatabases(vararg databases: String): DatabasesWaitStrategy {
        this.databases += databases
        return this
    }

    override fun waitUntilReady() {
        val boltUrl = "bolt://${waitStrategyTarget.containerIpAddress}:${waitStrategyTarget.getMappedPort(7687)}"
        val driver = GraphDatabase.driver(boltUrl, auth)
        val systemSession = driver.session(SessionConfig.forDatabase(StreamsUtils.SYSTEM_DATABASE_NAME))
        systemSession.beginTransaction().use { tx ->
            databases.forEach { tx.run("CREATE DATABASE $it IF NOT EXISTS") }
            tx.commit()
        }
        Unreliables.retryUntilSuccess(startupTimeout.seconds.toInt(), TimeUnit.SECONDS) {
            rateLimiter.doWhenReady {
                if (databases.isNotEmpty()) {
                    val databasesStatus = systemSession.beginTransaction()
                        .use { tx ->
                            tx.run("SHOW DATABASES").list()
                                .map { it.get("name").asString() to it.get("currentStatus").asString() }.toMap()
                        }
                    val notOnline = databasesStatus.filterValues { it != "online" }
                    if (databasesStatus.size < databases.size || notOnline.isNotEmpty()) {
                        throw RuntimeException("Cannot started because of the following databases: ${notOnline.keys}")
                    }
                }
            }
            true
        }
        systemSession.close()
        driver.close()
    }

}

class Neo4jContainerExtension(dockerImage: String) : Neo4jContainer<Neo4jContainerExtension>(dockerImage) {
    constructor() : this(System.getenv("NEO4J_IMAGE") ?: "neo4j:5-enterprise")

    private val logger = LoggerFactory.getLogger(Neo4jContainerExtension::class.java)
    var driver: Driver? = null
    var session: Session? = null

    private var cypher: String? = null

    private var withDriver = true
    private var withLogger = false
    private var withStreamsPlugin = false
    private var forcePluginRebuild = true

    private var databases = arrayOf<String>()

    private val waitStrategies = mutableListOf<WaitStrategy>()

    fun withWaitStrategy(waitStrategy: WaitStrategy): Neo4jContainerExtension {
        this.waitStrategies += waitStrategy
        return this
    }


    fun withFixture(cypher: String): Neo4jContainerExtension {
        this.cypher = cypher
        return this
    }

    fun withoutDriver(): Neo4jContainerExtension {
        this.withDriver = false
        return this
    }

    fun withStreamsPlugin(): Neo4jContainerExtension {
        this.withStreamsPlugin = true
        return this
    }

    fun withoutForcePluginRebuild(): Neo4jContainerExtension {
        this.forcePluginRebuild = false
        return this
    }

    fun withKafka(kafka: KafkaContainer): Neo4jContainerExtension {
        return withKafka(kafka.network!!, kafka.networkAliases.map { "$it:9092" }.joinToString(","))
    }

    fun withKafka(network: Network, bootstrapServers: String): Neo4jContainerExtension {
        withNetwork(network)
        withNeo4jConfig("kafka.bootstrap.servers", bootstrapServers)
        return this
    }

    fun withDatabases(vararg databases: String): Neo4jContainerExtension {
        this.databases += databases
        return this
    }

    private fun createAuth(): AuthToken {
        return if (!adminPassword.isNullOrBlank()) AuthTokens.basic("neo4j", adminPassword) else AuthTokens.none();
    }

    override fun start() {
        withNeo4jConfig("dbms.security.auth_enabled", "false")
        if (databases.isNotEmpty()) {
            withWaitStrategy(
                DatabasesWaitStrategy(createAuth())
                    .forDatabases(*databases)
                    .withStartupTimeout(Duration.ofMinutes(2))
            )
        }
        if (waitStrategies.isNotEmpty()) {
            val waitAllStrategy = waitStrategy as WaitAllStrategy
            waitStrategies.reversed()
                .forEach { waitStrategy -> waitAllStrategy.withStrategy(waitStrategy) }
        }
        if (withLogger) {
            withLogConsumer(Slf4jLogConsumer(logger))
        }
        addEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        if (withStreamsPlugin) {
            mountStreamsPlugin()
        }
        super.start()
        if (withDriver) {
            createDriver()
        }
    }

    private fun createDriver() {
        driver = GraphDatabase.driver(boltUrl, createAuth())
        session = driver!!.session()
        cypher?.split(";")
            ?.forEach { query -> session!!.beginTransaction().use { it.run(query) } }
    }

    private fun mountStreamsPlugin() {
        var distrFile = findDistrFile()
        if (forcePluginRebuild || distrFile == null) {
            MavenUtils.mvnw("../", if (withLogger) logger else null)
        }
        distrFile = findDistrFile()!!
        this.withPlugins(MountableFile.forHostPath(distrFile.path))
    }

    private fun findDistrFile(): File? {
        try {
            return File("../target/containerPlugins").listFiles()
                .filter { it.extension == "jar" }
                .firstOrNull()
        } catch (e: Exception) {
            return null
        }
    }

    override fun stop() {
        session?.close()
        driver?.close()
        super.stop()
        if (withStreamsPlugin && forcePluginRebuild) {
            findDistrFile()!!.delete()
        }
    }

    fun withLogging(): Neo4jContainerExtension {
        this.withLogger = true
        return this
    }
}