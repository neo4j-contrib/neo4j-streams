package streams.config

import org.neo4j.configuration.Config
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Matcher
import java.util.regex.Pattern

class StreamsConfig(private val neo4jConfig: Config, logService: LogService) : LifecycleAdapter() {

    val config = ConcurrentHashMap<String, String>()

    private val log = logService.getInternalLog(StreamsConfig::class.java)

    private val SUN_JAVA_COMMAND = "sun.java.command"
    private val CONF_DIR_PATTERN = Pattern.compile("--config-dir=(\\S+)")

    companion object {
        private var afterInitListeners = mutableListOf<((MutableMap<String, String>) -> Unit)>()

        fun registerListener(after: (MutableMap<String, String>) -> Unit) {
            afterInitListeners.add(after)
        }
    }

    override fun init() {
        log.debug("Init StreamsConfig")

        loadConfiguration()
        afterInitListeners.forEach { it(config) }
    }



    private fun loadConfiguration() {
        val neo4jConfFolder = System.getenv().getOrDefault("NEO4J_CONF", determineNeo4jConfFolder())

        val properties = Properties()
        try {
            log.info("the retrieved NEO4J_CONF dirs is $neo4jConfFolder")
            properties.load(FileInputStream("$neo4jConfFolder/neo4j.conf"))
        } catch (e: FileNotFoundException) {
            log.error("the neo4j.conf file is not under the directory defined into the directory $neo4jConfFolder, please set the NEO4J_CONF env correctly")
        }

        val filteredValues = properties
                .filterKeys { it.toString().startsWith("streams") || it.toString().startsWith("kafka") }
                .mapNotNull {
                    if (it.value == null) {
                        null
                    } else {
                        it.key.toString() to it.value.toString()
                    }
                }
                .toMap()
        log.debug("Neo4j Streams configuration from neo4j.conf file: $filteredValues")

        config.putAll(filteredValues)
    }

    private fun determineNeo4jConfFolder(): String? { // sun.java.command=com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/myid/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=/home/myid/neo4j-enterprise-4.0.0-alpha09mr02/conf
        val command = System.getProperty(SUN_JAVA_COMMAND)
        val matcher: Matcher = CONF_DIR_PATTERN.matcher(command)
        return if (matcher.find()) {
            val neo4jConfFolder = matcher.group(1)
            log.info("from system properties: NEO4J_CONF=%s", neo4jConfFolder)
            neo4jConfFolder
        } else {
            log.info("cannot determine conf folder from sys property %s, assuming '.' ", command)
            "."
        }
    }

}