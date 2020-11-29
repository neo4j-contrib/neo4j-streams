package streams.configuration

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import streams.events.KeyValueResult
import java.util.stream.Stream

class StreamsConfigProcedures {

    @JvmField @Context
    var log: Log? = null

    @JvmField @Context
    var db: GraphDatabaseService? = null

    @Procedure
    @Description("""
        streams.configuration.set(<properties_map>, <config_map>) YIELD configuration
    """)
    fun set(@Name(value = "properties") properties: Map<String, Any>?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<KeyValueResult> {
        if (properties.isNullOrEmpty()) {
            throw RuntimeException("Property must be not empty")
        }
        val map = properties.mapValues { it.value.toString() }
        val instance = StreamsConfig.getInstance(db as GraphDatabaseAPI)
        val save = config.orEmpty()
                .getOrDefault("save", "true")
                .toString()
                .toBoolean()
        instance.setProperties(map, save)
        return get()
    }

    @Procedure(mode = Mode.READ)
    @Description("""
        streams.configuration.get() YIELD configuration
    """)
    fun get(): Stream<KeyValueResult> = StreamsConfig.getInstance(db as GraphDatabaseAPI)
            .getConfiguration()
            .entries
            .map { KeyValueResult(it.key, it.value) }
            .stream()
}