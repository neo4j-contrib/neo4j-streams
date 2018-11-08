package streams.serialization

import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.JsonProcessingException
import org.codehaus.jackson.Version
import org.codehaus.jackson.map.JsonSerializer
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.SerializationConfig
import org.codehaus.jackson.map.SerializerProvider
import org.codehaus.jackson.map.module.SimpleModule
import org.neo4j.graphdb.spatial.Point
import org.neo4j.values.storable.CoordinateReferenceSystem
import java.io.IOException
import java.time.temporal.TemporalAccessor

abstract class StreamsPoint { abstract val crs: String }
data class StreamsPointCartesian(override val crs: String, val x: Double, val y: Double, val z: Double? = null): StreamsPoint()
data class StreamsPointWgs(override val crs: String, val latitude: Double, val longitude: Double, val height: Double? = null): StreamsPoint()

fun Point.toStreamsPoint(): StreamsPoint {
    val crsType = this.crs.type
    val coordinate = this.coordinates[0].coordinate
    return when (this.crs) {
        CoordinateReferenceSystem.Cartesian -> StreamsPointCartesian(crsType, coordinate[0], coordinate[1])
        CoordinateReferenceSystem.Cartesian_3D -> StreamsPointCartesian(crsType, coordinate[0], coordinate[1], coordinate[2])
        CoordinateReferenceSystem.WGS84 -> StreamsPointWgs(crsType, coordinate[0], coordinate[1])
        CoordinateReferenceSystem.WGS84_3D -> StreamsPointWgs(crsType, coordinate[0], coordinate[1], coordinate[2])
        else -> throw IllegalArgumentException("Point type $crsType not supported")
    }
}

class PointSerializer : JsonSerializer<Point>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: Point?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        if (value == null) {
            return
        }
        jgen.writeObject(value.toStreamsPoint())
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
        val module = SimpleModule("Neo4jKafkaSerializer", Version(1, 0, 0, ""))
        module.addSerializer(Point::class.java, PointSerializer())
        module.addSerializer(TemporalAccessor::class.java, TemporalAccessorSerializer())
        OBJECT_MAPPER.registerModule(module)
        OBJECT_MAPPER.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS)
    }

    fun getMapper(): ObjectMapper {
        return OBJECT_MAPPER
    }

}