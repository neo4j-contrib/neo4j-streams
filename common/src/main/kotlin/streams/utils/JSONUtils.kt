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
import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.neo4j.driver.internal.value.PointValue
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Point
import org.neo4j.driver.types.Relationship
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
data class StreamsPointCartesian(override val crs: String, val x: Double, val y: Double, val z: Double? = null): StreamsPoint()
data class StreamsPointWgs(override val crs: String, val latitude: Double, val longitude: Double, val height: Double? = null): StreamsPoint()

fun PointValue.toStreamsPoint(): StreamsPoint {
    val point = this.asPoint()
    return point.toStreamsPoint()
}

fun Point.toStreamsPoint(): StreamsPoint {
    val point = this
    return when (val crsType = point.srid()) {
        7203 -> StreamsPointCartesian("cartesian", point.x(), point.y())
        9157 -> StreamsPointCartesian("cartesian-3d", point.x(), point.y(), point.z())
        4326 -> StreamsPointWgs("wgs-84", point.x(), point.y())
        4979 -> StreamsPointWgs("wgs-84-3d", point.x(), point.y(), point.z())
        else -> throw IllegalArgumentException("Point type $crsType not supported")
    }
}

class ValueSerializer : JsonSerializer<Value>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: Value?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        value?.let { jgen.writeObject(it.asObject()) }

    }
}

class PointSerializer : JsonSerializer<Point>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: Point?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        value?.let { jgen.writeObject(it.toStreamsPoint()) }
    }
}

class PointValueSerializer : JsonSerializer<PointValue>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: PointValue?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        value?.let { jgen.writeObject(it.toStreamsPoint()) }
    }
}

class TemporalAccessorSerializer : JsonSerializer<TemporalAccessor>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: TemporalAccessor?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        value?.let { jgen.writeString(it.toString()) }
    }
}

class DriverNodeSerializer : JsonSerializer<Node>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: Node?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        value?.let { jgen.writeObject(it.asStreamsMap()) }
    }
}

class DriverRelationshipSerializer : JsonSerializer<Relationship>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: Relationship?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        value?.let { jgen.writeObject(it.asStreamsMap()) }
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
                val pointMap = it.value as Map<String, Any>
                when (pointMap["crs"]) {
                    "cartesian" -> Values.point(
                        7203,
                        pointMap["x"].toString().toDouble(),
                        pointMap["y"].toString().toDouble()
                    )
                    "cartesian-3d" -> Values.point(
                        9157,
                        pointMap["x"].toString().toDouble(),
                        pointMap["y"].toString().toDouble(),
                        pointMap["z"].toString().toDouble()
                    )
                    "wgs-84" -> Values.point(
                        4326,
                        pointMap["longitude"].toString().toDouble(),
                        pointMap["latitude"].toString().toDouble()
                    )
                    "wgs-84-3d" -> Values.point(
                        4979,
                        pointMap["longitude"].toString().toDouble(),
                        pointMap["latitude"].toString().toDouble(),
                        pointMap["height"].toString().toDouble()
                    )
                    else -> throw IllegalArgumentException("CRS value: ${pointMap["crs"]} not found")
                }
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
        module.addSerializer(Value::class.java, ValueSerializer())
        module.addSerializer(Point::class.java, PointSerializer())
        module.addSerializer(PointValue::class.java, PointValueSerializer())
        module.addSerializer(Node::class.java, DriverNodeSerializer())
        module.addSerializer(Relationship::class.java, DriverRelationshipSerializer())
        module.addDeserializer(StreamsTransactionRelationshipEvent::class.java, StreamsTransactionRelationshipEventDeserializer())
        module.addDeserializer(StreamsTransactionNodeEvent::class.java, StreamsTransactionNodeEventDeserializer())
        module.addSerializer(TemporalAccessor::class.java, TemporalAccessorSerializer())
        OBJECT_MAPPER.registerModule(module)
        OBJECT_MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        STRICT_OBJECT_MAPPER.registerModule(module)
    }

    fun getObjectMapper(): ObjectMapper = OBJECT_MAPPER

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