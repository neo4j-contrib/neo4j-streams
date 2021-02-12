package extension

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseBuilder
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.configuration.StreamsConfig
import streams.events.StreamsPluginStatus
import streams.procedures.StreamsSinkProcedures
import streams.utils.StreamsUtils

fun GraphDatabaseBuilder.newDatabase(initialPluginStatus: StreamsPluginStatus = StreamsPluginStatus.RUNNING): GraphDatabaseService {
    val configField = try {
        this.javaClass.superclass.getDeclaredField("config")
    } catch (e: Exception) {
        this.javaClass.getDeclaredField("config")
    }
    configField.isAccessible = true
    val config: Map<String, Any> = configField.get(this) as Map<String, Any>
    val db = newGraphDatabase() as GraphDatabaseAPI
    // as the Configuration System now lives "outside" Neo4j
    // in order to apply the configuration we wait for the
    // database to be available and the we apply the configuration
    // taken from the GraphDatabaseBuilder `config` param
    if (db.isAvailable(30000)) {
        println("Setting Config from APIs: $config")
        if (config.isNotEmpty()) {
            StreamsConfig.getInstance(db).setProperties(config, false)
        }
    } else {
        throw RuntimeException("Cannot set Config because database is not available")
    }
    // the status check is important because allows us to define the expected
    // initial state of the streams plugin
    // for instance in same cases i.e procedures test we don't subscribe any topics
    // this means that the expected status of the plugin at the startup is STOPPED
    // because we don't allow to start the database with the plugin in a inconsistent state
    val success = StreamsUtils.blockUntilFalseOrTimeout(180000, 1000) {
        StreamsSinkProcedures.hasStatus(db, initialPluginStatus)
    }
    if (!success && StreamsSinkProcedures.isRegistered(db)) {
        db.shutdown()
        throw RuntimeException("Cannot start the Sink properly")
    }
    return db
}