package streams.serialization

import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.JsonProcessingException
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.Version
import org.codehaus.jackson.map.JsonSerializer
import org.codehaus.jackson.map.SerializationConfig
import org.codehaus.jackson.map.SerializerProvider
import org.codehaus.jackson.map.module.SimpleModule
import org.neo4j.graphdb.spatial.CRS
import org.neo4j.graphdb.spatial.Geometry
import java.io.IOException
import java.time.temporal.TemporalAccessor


data class InternalPoint(val coordinates: List<Double>, val crs: CRS)
class PointSerializer : JsonSerializer<Geometry>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: Geometry?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        if (value == null) {
            return
        }
        val point = InternalPoint(value!!.coordinates[0].coordinate, value!!.crs)
        jgen.writeObject(point)
    }
}

class TemporalAccessorSerializer : JsonSerializer<TemporalAccessor>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: TemporalAccessor?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        if (value == null) {
            return
        }
        jgen.writeString(value.toString())
    }
}


object JacksonUtil {

    private val OBJECT_MAPPER: ObjectMapper = ObjectMapper()

    init {
        val module = SimpleModule("Neo4jKakfaSerializer", Version(1, 0, 0, ""))
        module.addSerializer(Geometry::class.java, PointSerializer())
        module.addSerializer(TemporalAccessor::class.java, TemporalAccessorSerializer())
        OBJECT_MAPPER.registerModule(module)
        OBJECT_MAPPER.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS)
    }

    fun getMapper(): ObjectMapper {
        return OBJECT_MAPPER
    }


}