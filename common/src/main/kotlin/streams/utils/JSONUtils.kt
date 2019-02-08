package streams.serialization

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.neo4j.driver.internal.value.PointValue
import org.neo4j.graphdb.spatial.Point
import org.neo4j.values.storable.CoordinateReferenceSystem
import streams.events.*
import streams.utils.StreamsUtils
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

fun PointValue.toStreamsPoint(): StreamsPoint {
    val point = this.asPoint()
    val crsType = point.srid()
    return when (crsType) {
        CoordinateReferenceSystem.Cartesian.code -> StreamsPointCartesian(CoordinateReferenceSystem.Cartesian.name, point.x(), point.y())
        CoordinateReferenceSystem.Cartesian_3D.code -> StreamsPointCartesian(CoordinateReferenceSystem.Cartesian_3D.name, point.x(), point.y(), point.z())
        CoordinateReferenceSystem.WGS84.code -> StreamsPointWgs(CoordinateReferenceSystem.WGS84.name, point.x(), point.y())
        CoordinateReferenceSystem.WGS84_3D.code -> StreamsPointWgs(CoordinateReferenceSystem.WGS84_3D.name, point.x(), point.y(), point.z())
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

class PointValueSerializer : JsonSerializer<PointValue>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: PointValue?, jgen: JsonGenerator,
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


object JSONUtils {

    private val OBJECT_MAPPER: ObjectMapper = jacksonObjectMapper()

    init {
        val module = SimpleModule("Neo4jKafkaSerializer")
        StreamsUtils.ignoreExceptions({ module.addSerializer(Point::class.java, PointSerializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        StreamsUtils.ignoreExceptions({ module.addSerializer(PointValue::class.java, PointValueSerializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        module.addSerializer(TemporalAccessor::class.java, TemporalAccessorSerializer())
        OBJECT_MAPPER.registerModule(module)
        OBJECT_MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    }

    fun getObjectMapper(): ObjectMapper {
        return OBJECT_MAPPER
    }

    fun asMap(any: Any): Map<String, Any?> {
        return OBJECT_MAPPER.convertValue(any, Map::class.java)
                .mapKeys { it.key.toString() }
    }

    fun writeValueAsString(any: Any): String {
        return OBJECT_MAPPER.writeValueAsString(any)
    }

    fun writeValueAsBytes(any: Any): ByteArray {
        return OBJECT_MAPPER.writeValueAsBytes(any)
    }

    inline fun <reified T> readValue(value: ByteArray): T {
        return getObjectMapper().readValue(value, T::class.java)
    }

    inline fun <reified T> readValue(value: Any): T {
        val strValue = when (value) {
            is String -> value
            else -> getObjectMapper().writeValueAsString(value)
        }
        return getObjectMapper().readValue(strValue)
    }

    inline fun <reified T> convertValue(value: Any): T {
        return getObjectMapper().convertValue(value)
    }

    fun asStreamsTransactionEvent(obj: Any): StreamsTransactionEvent {
        val value = when (obj) {
            is String -> readValue<Map<String, Map<String, Any>>>(obj)
            is Map<*, *> -> obj as Map<String, Map<String, Any>>
            else -> convertValue<Map<String, Map<String, Any>>>(obj)
        }
        val meta = convertValue<Meta>(value.getValue("meta"))

        val schema = convertValue<Schema>(value.getValue("schema"))

        val payloadMap = value.getValue("payload")
        val type = payloadMap.getValue("type").toString()
        val id = payloadMap.getValue("id").toString()

        val payload = if (type == "node") {
            val before = if (payloadMap["before"] != null) convertValue<NodeChange>(payloadMap["before"]!!) else null
            val after = if (payloadMap["after"] != null) convertValue<NodeChange>(payloadMap["after"]!!) else null
            NodePayload(id, before, after)
        } else {
            val label= payloadMap.getValue("label").toString()
            val start = convertValue<RelationshipNodeChange>(payloadMap.getValue("start"))
            val end = convertValue<RelationshipNodeChange>(payloadMap.getValue("end"))
            val before = if (payloadMap["before"] != null) convertValue<RelationshipChange>(payloadMap["before"]!!) else null
            val after = if (payloadMap["after"] != null) convertValue<RelationshipChange>(payloadMap["after"]!!) else null
            RelationshipPayload(id, start, end, before, after, label)
        }
        return StreamsTransactionEvent(meta, payload, schema)
    }
}