package streams.kafka.connect.sink.converters

import org.neo4j.driver.Value
import org.neo4j.driver.Values
import java.math.BigDecimal
import java.time.LocalTime
import java.time.ZoneId
import java.util.Date
import java.util.concurrent.TimeUnit


class Neo4jValueConverter: MapValueConverter<Value>() {

    companion object {
        @JvmStatic private val UTC = ZoneId.of("UTC")
    }

    override fun setValue(result: MutableMap<String, Value?>?, fieldName: String, value: Any?) {
        if (result != null) {
            result[fieldName] = Values.value(value) ?: Values.NULL
        }
    }

    override fun newValue(): MutableMap<String, Value?> {
        return mutableMapOf()
    }

    override fun setDecimalField(result: MutableMap<String, Value?>?, fieldName: String, value: BigDecimal) {
        val doubleValue = value.toDouble()
        val fitsScale = doubleValue != Double.POSITIVE_INFINITY
                && doubleValue != Double.NEGATIVE_INFINITY
                && value.compareTo(doubleValue.let { BigDecimal.valueOf(it) }) == 0
        if (fitsScale) {
            setValue(result, fieldName, doubleValue)
        } else {
            setValue(result, fieldName, value.toPlainString())
        }
    }

    override fun setTimestampField(result: MutableMap<String, Value?>?, fieldName: String, value: Date) {
        val localDate = value.toInstant().atZone(UTC).toLocalDateTime()
        setValue(result, fieldName, localDate)

    }

    override fun setTimeField(result: MutableMap<String, Value?>?, fieldName: String, value: Date) {
        val time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(value.time))
        setValue(result, fieldName, time)
    }

    override fun setDateField(result: MutableMap<String, Value?>?, fieldName: String, value: Date) {
        val localDate = value.toInstant().atZone(UTC).toLocalDate()
        setValue(result, fieldName, localDate)
    }
}