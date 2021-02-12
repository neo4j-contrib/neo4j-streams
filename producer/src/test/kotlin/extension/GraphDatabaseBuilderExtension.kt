package extension

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseBuilder
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.configuration.StreamsConfig
import streams.events.StreamsPluginStatus
import streams.procedures.StreamsProcedures
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
    if (db.isAvailable(30000)) {
        println("Setting Config from APIs: $config")
        if (config.isNotEmpty()) {
            StreamsConfig.getInstance(db).setProperties(config, false)
        }
    } else {
        throw RuntimeException("Cannot set Config because database is not available")
    }
    val success = StreamsUtils.blockUntilFalseOrTimeout(100000, 1000) {
        StreamsProcedures.hasStatus(db, initialPluginStatus)
    }
    if (!success && StreamsProcedures.isRegistered(db)) {
        db.shutdown()
        throw RuntimeException("Cannot start the Sink properly")
    }
    return db
}