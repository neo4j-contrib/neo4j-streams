package streams.kafka.connect.utils

import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import streams.kafka.connect.sink.converters.MapValueConverter
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import streams.serialization.JSONUtils
import streams.service.StreamsSinkEntity

fun SinkRecord.toStreamsSinkEntity(): StreamsSinkEntity = StreamsSinkEntity(convertData(this.key()), convertData(this.value()))

private val converter = MapValueConverter<Any>()

private fun convertData(data: Any?) = when (data) {
    is Struct -> converter.convert(data)
    else -> if (data == null) null else JSONUtils.readValue<Any>(data)
}