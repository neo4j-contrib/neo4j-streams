package streams.utils

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.neo4j.driver.Value
import org.neo4j.driver.internal.value.PointValue
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Point
import org.neo4j.driver.types.Relationship
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
        module.addSerializer(TemporalAccessor::class.java, TemporalAccessorSerializer())
        OBJECT_MAPPER.registerModule(module)
        OBJECT_MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
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