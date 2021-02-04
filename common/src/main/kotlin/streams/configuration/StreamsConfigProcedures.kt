package streams.configuration

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import streams.events.KeyValueResult
import java.util.stream.Stream

data class StreamsConfigProceduresConfiguration(val save: Boolean) {
    constructor(map: Map<String, Any>?): this(map.orEmpty()
        .getOrDefault("save", "true")
        .toString()
        .toBoolean())
}

class StreamsConfigProcedures {

    @JvmField @Context
    var log: Log? = null

    @JvmField @Context
    var db: GraphDatabaseService? = null

    @Admin
    @Procedure
    @Description("""
        streams.configuration.set(<properties_map>, <config_map>) YIELD name, value
    """)
    fun set(@Name(value = "properties") properties: Map<String, Any>?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<KeyValueResult> {
        if (properties.isNullOrEmpty()) {
            throw RuntimeException("Property must be not empty")
        }
        val map = properties.mapValues { it.value.toString() }
        val instance = StreamsConfig.getInstance(db as GraphDatabaseAPI)
        val cfg = StreamsConfigProceduresConfiguration(config)
        instance.setProperties(map, cfg.save)
        return get()
    }

    @Admin
    @Procedure
    @Description("""
        streams.configuration.remove(<properties_list>, <config_map>) YIELD name, value
    """)
    fun remove(@Name(value = "keys") properties: List<String>,
            @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<KeyValueResult> {
        if (properties.isNullOrEmpty()) {
            throw RuntimeException("Property must be not empty")
        }
        val instance = StreamsConfig.getInstance(db as GraphDatabaseAPI)
        val cfg = StreamsConfigProceduresConfiguration(config)
        instance.removeProperties(properties, cfg.save)
        return get()
    }

    @Admin
    @Procedure
    @Description("""
        streams.configuration.get() YIELD name, value
    """)
    fun get(): Stream<KeyValueResult> = StreamsConfig.getInstance(db as GraphDatabaseAPI)
            .getConfiguration()
            .entries
            .map { KeyValueResult(it.key, it.value) }
            .stream()
}