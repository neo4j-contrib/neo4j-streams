package streams.kafka.connect.sink.converters

import org.neo4j.driver.v1.Value
import org.neo4j.driver.v1.Values
import java.time.LocalTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.TimeUnit


class Neo4jValueConverter: MapValueConverter<Value>() {

    companion object {
        @JvmStatic private val UTC = ZoneId.of("UTC")
    }

    override fun setValue(result: MutableMap<String, Value?>?, fieldName: String?, value: Any?) {
        if (result != null && fieldName != null) {
            result[fieldName] = Values.value(value) ?: Values.NULL
        }
    }

    override fun newValue(): MutableMap<String, Value?> {
        return mutableMapOf()
    }

    override fun setTimestampField(result: MutableMap<String, Value?>?, fieldName: String?, value: Date?) {
        if (value != null) {
            val localDate = value.toInstant().atZone(UTC).toLocalDateTime()
            setValue(result, fieldName, localDate)
        } else {
            setNullField(result, fieldName)
        }

    }

    override fun setTimeField(result: MutableMap<String, Value?>?, fieldName: String?, value: Date?) {
        if (value != null) {
            val time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(value.time))
            setValue(result, fieldName, time)
        } else {
            setNullField(result, fieldName)
        }
    }

    override fun setDateField(result: MutableMap<String, Value?>?, fieldName: String?, value: Date?) {
        if (value != null) {
            val localDate = value.toInstant().atZone(UTC).toLocalDate()
            setValue(result, fieldName, localDate)
        } else {
            setNullField(result, fieldName)
        }
    }
}