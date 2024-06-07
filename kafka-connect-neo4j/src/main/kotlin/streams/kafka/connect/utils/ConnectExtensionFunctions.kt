package streams.kafka.connect.utils

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.driver.Record
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Point
import org.neo4j.driver.types.Relationship
import streams.extensions.asStreamsMap
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import streams.utils.JSONUtils
import streams.service.StreamsSinkEntity
import java.time.temporal.TemporalAccessor

fun SinkRecord.toStreamsSinkEntity(): StreamsSinkEntity = StreamsSinkEntity(
    convertData(this.key(), true),
    convertData(this.value())
)

private val converter = Neo4jValueConverter()

private fun convertData(data: Any?, stringWhenFailure: Boolean = false) = when (data) {
    is Struct -> converter.convert(data)
    null -> null
    else -> JSONUtils.readValue<Any>(data, stringWhenFailure)
}

fun Record.asJsonString(): String = JSONUtils.writeValueAsString(this.asMap())

fun Record.schema(asMap: Map<String, Any> = this.asMap()): Schema {
    val structBuilder = SchemaBuilder.struct()
    asMap.forEach { structBuilder.field(it.key, neo4jValueSchema(it.value)) }
    return structBuilder.build()
}

fun Record.asStruct(): Struct {
    val asMap = this.asMap()
    val schema = schema(asMap)
    val struct = Struct(schema)
    schema.fields().forEach {
        struct.put(it, neo4jToKafka(it.schema(), asMap[it.name()]))
    }
    return struct
}

private fun neo4jToKafka(schema: Schema, value: Any?): Any? {
    if (value == null) {
        return null
    } else {
        return when (schema.type()) {
            Schema.Type.ARRAY -> when (value) {
                is Collection<*> -> value.map { neo4jToKafka(schema.valueSchema(), it) }
                is Array<*> -> value.map { neo4jToKafka(schema.valueSchema(), it) }.toTypedArray()
                else -> throw IllegalArgumentException("For Schema.Type.ARRAY we support only Collection and Array")
            }

            Schema.Type.MAP -> when (value) {
                is Map<*, *> -> value.mapValues { neo4jToKafka(schema.valueSchema(), it.value) }
                else -> throw IllegalArgumentException("For Schema.Type.MAP we support only Map")
            }

            Schema.Type.STRUCT -> when (value) {
                is Map<*, *> -> {
                    val struct = Struct(schema)
                    schema.fields().forEach {
                        val field = it
                        neo4jToKafka(field.schema(), value[field.name()])?.let {
                            struct.put(field, it)
                        }
                    }
                    struct
                }

                is Point -> {
                    val map = JSONUtils.readValue<Map<String, Any>>(value)
                    neo4jToKafka(schema, map)
                }

                is Node -> {
                    val map = value.asStreamsMap()
                    neo4jToKafka(schema, map)
                }

                is Relationship -> {
                    val map = value.asStreamsMap()
                    neo4jToKafka(schema, map)
                }

                else -> throw IllegalArgumentException("For Schema.Type.STRUCT we support only Map and Point")
            }

            else -> when (value) {
                is TemporalAccessor -> {
                    val temporalValue = JSONUtils.readValue<String>(value)
                    neo4jToKafka(schema, temporalValue)
                }

                else -> when {
                    Schema.Type.STRING == schema.type() && value !is String -> value.toString()
                    else -> value
                }
            }
        }
    }
}

private fun neo4jValueSchema(value: Any?): Schema? =
    when (value) {
        null -> SchemaBuilder.struct().optional().build()
        is Long -> Schema.OPTIONAL_INT64_SCHEMA
        is Double -> Schema.OPTIONAL_FLOAT64_SCHEMA
        is Boolean -> Schema.OPTIONAL_BOOLEAN_SCHEMA
        is Collection<*> -> {
            val schema =
                value.firstNotNullOfOrNull { neo4jValueSchema(it) } ?: SchemaBuilder.struct().optional().build()
            SchemaBuilder.array(schema).optional()
        }

        is Array<*> -> {
            val schema =
                value.firstNotNullOfOrNull { neo4jValueSchema(it) } ?: SchemaBuilder.struct().optional().build()
            SchemaBuilder.array(schema).optional()
        }

        is Map<*, *> -> {
            if (value.isEmpty()) {
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build()
            } else {
                val valueTypes = value.values
                    .mapNotNull { elem -> elem?.let { it::class.java.simpleName } }
                    .filter { !it.lowercase().startsWith("empty") }
                    .toSet()
                if (valueTypes.size == 1) {
                    neo4jValueSchema(value.values.first())
                        ?.let {
                            SchemaBuilder.map(Schema.STRING_SCHEMA, it)
                                .optional()
                                .build()
                        }
                } else {
                    val structMap = SchemaBuilder
                        .struct()
                        .optional()
                    value.forEach {
                        val entry = it
                        neo4jValueSchema(entry.value)?.let {
                            structMap.field(entry.key.toString(), it)
                        }
                    }
                    if (structMap.fields().isEmpty()) null
                    else structMap.build()
                }
            }
        }

        is Point -> neo4jValueSchema(JSONUtils.readValue<Map<String, Any>>(value))
        is Node -> neo4jValueSchema(value.asStreamsMap())
        is Relationship -> neo4jValueSchema(value.asStreamsMap())
        else -> Schema.OPTIONAL_STRING_SCHEMA
    }
