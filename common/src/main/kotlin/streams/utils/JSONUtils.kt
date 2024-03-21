package streams.utils

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.neo4j.driver.internal.value.PointValue
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.graphdb.spatial.Point
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import streams.events.EntityType
import streams.events.Meta
import streams.events.NodePayload
import streams.events.Payload
import streams.events.RecordChange
import streams.events.RelationshipPayload
import streams.events.Schema
import streams.events.StreamsTransactionEvent
import streams.events.StreamsTransactionNodeEvent
import streams.events.StreamsTransactionRelationshipEvent
import streams.extensions.asStreamsMap
import java.io.IOException
import java.time.temporal.TemporalAccessor
import kotlin.reflect.full.isSubclassOf

abstract class StreamsPoint { abstract val crs: String }
data class StreamsPointCartesian(override val crs: String, val x: Double, val y: Double): StreamsPoint()
data class StreamsPointCartesian3D(override val crs: String, val x: Double, val y: Double, val z: Double?): StreamsPoint()
data class StreamsPointWgs(override val crs: String, val latitude: Double, val longitude: Double): StreamsPoint()
data class StreamsPointWgs3D(override val crs: String, val latitude: Double, val longitude: Double, val height: Double): StreamsPoint()


fun Point.toStreamsPoint(): StreamsPoint {
    val crsType = this.crs.type
    val coordinate = this.coordinates[0].coordinate
    return when (this.crs) {
        CoordinateReferenceSystem.Cartesian -> StreamsPointCartesian(crsType, coordinate[0], coordinate[1])
        CoordinateReferenceSystem.Cartesian_3D -> StreamsPointCartesian3D(crsType, coordinate[0], coordinate[1], coordinate[2])
        CoordinateReferenceSystem.WGS84 -> StreamsPointWgs(crsType, coordinate[0], coordinate[1])
        CoordinateReferenceSystem.WGS84_3D -> StreamsPointWgs3D(crsType, coordinate[0], coordinate[1], coordinate[2])
        else -> throw IllegalArgumentException("Point type $crsType not supported")
    }
}

fun Map<String, Any>.toMapValue(): MapValue {
    val map = this
    val builder = MapValueBuilder()
    map.forEach { (t, u) -> builder.add(t, Values.of(u)) }
    return builder.build()
}

fun PointValue.toStreamsPoint(): StreamsPoint {
    val point = this.asPoint()
    return when (val crsType = point.srid()) {
        CoordinateReferenceSystem.Cartesian.code -> StreamsPointCartesian(CoordinateReferenceSystem.Cartesian.name, point.x(), point.y())
        CoordinateReferenceSystem.Cartesian_3D.code -> StreamsPointCartesian3D(CoordinateReferenceSystem.Cartesian_3D.name, point.x(), point.y(), point.z())
        CoordinateReferenceSystem.WGS84.code -> StreamsPointWgs(CoordinateReferenceSystem.WGS84.name, point.x(), point.y())
        CoordinateReferenceSystem.WGS84_3D.code -> StreamsPointWgs3D(CoordinateReferenceSystem.WGS84_3D.name, point.x(), point.y(), point.z())
        else -> throw IllegalArgumentException("Point type $crsType not supported")
    }
}

fun org.neo4j.driver.types.Point.toStreamsPoint(): StreamsPoint {
    val point = this
    return when (val crsType = point.srid()) {
        CoordinateReferenceSystem.Cartesian.code -> StreamsPointCartesian(CoordinateReferenceSystem.Cartesian.name, point.x(), point.y())
        CoordinateReferenceSystem.Cartesian_3D.code -> StreamsPointCartesian3D(CoordinateReferenceSystem.Cartesian_3D.name, point.x(), point.y(), point.z())
        CoordinateReferenceSystem.WGS84.code -> StreamsPointWgs(CoordinateReferenceSystem.WGS84.name, point.x(), point.y())
        CoordinateReferenceSystem.WGS84_3D.code -> StreamsPointWgs3D(CoordinateReferenceSystem.WGS84_3D.name, point.x(), point.y(), point.z())
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

class DriverPointSerializer : JsonSerializer<org.neo4j.driver.types.Point>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: org.neo4j.driver.types.Point?, jgen: JsonGenerator,
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

class DriverNodeSerializer : JsonSerializer<org.neo4j.driver.types.Node>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: org.neo4j.driver.types.Node?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        if (value == null) {
            return
        }
        jgen.writeObject(value.asStreamsMap())
    }
}

class DriverRelationshipSerializer : JsonSerializer<org.neo4j.driver.types.Relationship>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: org.neo4j.driver.types.Relationship?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        if (value == null) {
            return
        }
        jgen.writeObject(value.asStreamsMap())
    }
}

class StreamsTransactionRelationshipEventDeserializer : StreamsTransactionEventDeserializer<StreamsTransactionRelationshipEvent, RelationshipPayload>() {
    override fun createEvent(meta: Meta, payload: RelationshipPayload, schema: Schema): StreamsTransactionRelationshipEvent {
        return StreamsTransactionRelationshipEvent(meta, payload, schema)
    }

    override fun convertPayload(payloadMap: JsonNode): RelationshipPayload {
        return JSONUtils.convertValue<RelationshipPayload>(payloadMap)
    }

    override fun fillPayload(payload: RelationshipPayload,
                             beforeProps: Map<String, Any>?,
                             afterProps: Map<String, Any>?): RelationshipPayload {
        return payload.copy(
            before = payload.before?.copy(properties = beforeProps),
            after = payload.after?.copy(properties = afterProps)
        )
    }

    override fun deserialize(parser: JsonParser, context: DeserializationContext): StreamsTransactionRelationshipEvent {
        val deserialized = super.deserialize(parser, context)
        if (deserialized.payload.type == EntityType.node) {
            throw IllegalArgumentException("Relationship event expected, but node type found")
        }
        return deserialized
    }

}

class StreamsTransactionNodeEventDeserializer : StreamsTransactionEventDeserializer<StreamsTransactionNodeEvent, NodePayload>() {
    override fun createEvent(meta: Meta, payload: NodePayload, schema: Schema): StreamsTransactionNodeEvent {
        return StreamsTransactionNodeEvent(meta, payload, schema)
    }

    override fun convertPayload(payloadMap: JsonNode): NodePayload {
        return JSONUtils.convertValue<NodePayload>(payloadMap)
    }

    override fun fillPayload(payload: NodePayload,
                             beforeProps: Map<String, Any>?,
                             afterProps: Map<String, Any>?): NodePayload {
        return payload.copy(
            before = payload.before?.copy(properties = beforeProps),
            after = payload.after?.copy(properties = afterProps)
        )
    }

    override fun deserialize(parser: JsonParser, context: DeserializationContext): StreamsTransactionNodeEvent {
        val deserialized = super.deserialize(parser, context)
        if (deserialized.payload.type == EntityType.relationship) {
            throw IllegalArgumentException("Node event expected, but relationship type found")
        }
        return deserialized
    }

}

abstract class StreamsTransactionEventDeserializer<EVENT, PAYLOAD: Payload> : JsonDeserializer<EVENT>() {

    abstract fun createEvent(meta: Meta, payload: PAYLOAD, schema: Schema): EVENT
    abstract fun convertPayload(payloadMap: JsonNode): PAYLOAD
    abstract fun fillPayload(payload: PAYLOAD,
                             beforeProps: Map<String, Any>?,
                             afterProps: Map<String, Any>?): PAYLOAD

    @Throws(IOException::class, JsonProcessingException::class)
    override fun deserialize(parser: JsonParser, context: DeserializationContext): EVENT {
        val root: JsonNode = parser.codec.readTree(parser)
        val meta = JSONUtils.convertValue<Meta>(root["meta"])
        val schema = JSONUtils.convertValue<Schema>(root["schema"])
        val points = schema.properties.filterValues { it == "PointValue" }.keys
        var payload = convertPayload(root["payload"])
        if (points.isNotEmpty()) {
            val beforeProps = convertPoints(payload.before, points)
            val afterProps = convertPoints(payload.after, points)
            payload = fillPayload(payload, beforeProps, afterProps)
        }
        return createEvent(meta, payload, schema)
    }

    private fun convertPoints(
        recordChange: RecordChange?,
        points: Set<String>
    ) = recordChange
        ?.properties
        ?.mapValues {
            if (points.contains(it.key)) {
                org.neo4j.values.storable.PointValue.fromMap((it.value as Map<String, Any>).toMapValue())
            } else {
                it.value
            }
        }

}

object JSONUtils {

    private val OBJECT_MAPPER: ObjectMapper = jacksonObjectMapper()
    private val STRICT_OBJECT_MAPPER: ObjectMapper = jacksonObjectMapper()

    init {
        val module = SimpleModule("Neo4jKafkaSerializer")
        StreamsUtils.ignoreExceptions({ module.addSerializer(Point::class.java, PointSerializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        StreamsUtils.ignoreExceptions({ module.addSerializer(PointValue::class.java, PointValueSerializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        StreamsUtils.ignoreExceptions({ module.addSerializer(org.neo4j.driver.types.Point::class.java, DriverPointSerializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        StreamsUtils.ignoreExceptions({ module.addSerializer(org.neo4j.driver.types.Node::class.java, DriverNodeSerializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        StreamsUtils.ignoreExceptions({ module.addSerializer(org.neo4j.driver.types.Relationship::class.java, DriverRelationshipSerializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        StreamsUtils.ignoreExceptions({ module.addDeserializer(StreamsTransactionRelationshipEvent::class.java, StreamsTransactionRelationshipEventDeserializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        StreamsUtils.ignoreExceptions({ module.addDeserializer(StreamsTransactionNodeEvent::class.java, StreamsTransactionNodeEventDeserializer()) }, NoClassDefFoundError::class.java) // in case is loaded from
        module.addSerializer(TemporalAccessor::class.java, TemporalAccessorSerializer())
        OBJECT_MAPPER.registerModule(module)
        OBJECT_MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        STRICT_OBJECT_MAPPER.registerModule(module)
    }

    fun getObjectMapper(): ObjectMapper = OBJECT_MAPPER

    fun getStrictObjectMapper(): ObjectMapper = STRICT_OBJECT_MAPPER

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

    inline fun <reified T> readValue(value: Any,
                                     stringWhenFailure: Boolean = false,
                                     objectMapper: ObjectMapper = getObjectMapper()): T {
        return try {
            when (value) {
                is String -> objectMapper.readValue(value)
                is ByteArray -> objectMapper.readValue(value)
                else -> objectMapper.convertValue(value)
            }
        } catch (e: JsonParseException) {
            if (stringWhenFailure && String::class.isSubclassOf(T::class)) {
                val strValue = when (value) {
                    is ByteArray -> String(value)
                    null -> ""
                    else -> value.toString()
                }
                strValue.trimStart().let {
                    if (it[0] == '{' || it[0] == '[') throw e
                    else it as T
                }
            }
            else throw e
        }
    }

    inline fun <reified T> convertValue(value: Any, objectMapper: ObjectMapper = getObjectMapper()): T {
        return objectMapper.convertValue(value)
    }

    fun asStreamsTransactionEvent(obj: Any): StreamsTransactionEvent {
        return try {
            val evt = when (obj) {
                is String, is ByteArray -> readValue<StreamsTransactionNodeEvent>(value = obj,
                        objectMapper = STRICT_OBJECT_MAPPER)
                else -> convertValue<StreamsTransactionNodeEvent>(value = obj,
                        objectMapper = STRICT_OBJECT_MAPPER)
            }
            evt.toStreamsTransactionEvent()
        } catch (e: Exception) {
            val evt = when (obj) {
                is String, is ByteArray -> readValue<StreamsTransactionRelationshipEvent>(value = obj,
                        objectMapper = STRICT_OBJECT_MAPPER)
                else -> convertValue<StreamsTransactionRelationshipEvent>(value = obj,
                        objectMapper = STRICT_OBJECT_MAPPER)
            }
            evt.toStreamsTransactionEvent()
        }
    }
}