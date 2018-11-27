package streams.kafka.connect.sink

import com.github.jcustenborder.kafka.connect.utils.data.AbstractConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.neo4j.driver.v1.Values
import java.math.BigDecimal
import java.time.LocalTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.TimeUnit


class ValueConverter: AbstractConverter<MutableMap<String, Any>>() {

    private val UTC = ZoneId.of("UTC")

    private fun setValue(result: MutableMap<String, Any>?, fieldName: String?, value: Any?) {
        if (result != null && fieldName != null && value != null) {
            result[fieldName] = Values.value(value)
        }
    }

    override fun newValue(): MutableMap<String, Any> {
        return mutableMapOf()
    }

    override fun setBytesField(result: MutableMap<String, Any>?, fieldName: String?, value: ByteArray?) {
        setValue(result, fieldName, value)
    }

    override fun setStringField(result: MutableMap<String, Any>?, fieldName: String?, value: String?) {
        setValue(result, fieldName, value)
    }

    override fun setFloat32Field(result: MutableMap<String, Any>?, fieldName: String?, value: Float?) {
        setValue(result, fieldName, value)
    }

    override fun setInt32Field(result: MutableMap<String, Any>?, fieldName: String?, value: Int?) {
        setValue(result, fieldName, value)
    }

    override fun setArray(result: MutableMap<String, Any>?, fieldName: String?, schema: Schema?, array: MutableList<Any?>?) {
        setValue(result, fieldName, array)
    }

    override fun setTimestampField(result: MutableMap<String, Any>?, fieldName: String?, value: Date?) {
        if (value != null) {
            val localDate = value.toInstant().atZone(UTC).toLocalDateTime()
            setValue(result, fieldName, localDate)
        }

    }

    override fun setTimeField(result: MutableMap<String, Any>?, fieldName: String?, value: Date?) {
        if (value != null) {
            val time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(value.time))
            setValue(result, fieldName, time)
        }
    }

    override fun setInt8Field(result: MutableMap<String, Any>?, fieldName: String?, value: Byte?) {
        setValue(result, fieldName, value)
    }

    override fun setStructField(result: MutableMap<String, Any>?, fieldName: String?, value: Struct?) {
        if (value != null) {
            val converted = convert(value) as Map<String, Any>
            setValue(result, fieldName, converted)
        }
    }

    override fun setMap(result: MutableMap<String, Any>?, fieldName: String?, schema: Schema?, map: MutableMap<Any?, Any?>?) {
        setValue(result, fieldName, map)
    }

    override fun setNullField(result: MutableMap<String, Any>?, fieldName: String?) {}

    override fun setFloat64Field(result: MutableMap<String, Any>?, fieldName: String?, value: Double?) {
        setValue(result, fieldName, value)
    }

    override fun setInt16Field(result: MutableMap<String, Any>?, fieldName: String?, value: Short?) {
        setValue(result, fieldName, value)
    }

    override fun setInt64Field(result: MutableMap<String, Any>?, fieldName: String?, value: Long?) {
        setValue(result, fieldName, value)
    }

    override fun setBooleanField(result: MutableMap<String, Any>?, fieldName: String?, value: Boolean?) {
        setValue(result, fieldName, value)
    }

    override fun setDecimalField(result: MutableMap<String, Any>?, fieldName: String?, value: BigDecimal?) {
        setValue(result, fieldName, value)
    }

    override fun setDateField(result: MutableMap<String, Any>?, fieldName: String?, value: Date?) {
        if (value != null) {
            val localDate = value.toInstant().atZone(UTC).toLocalDate()
            setValue(result, fieldName, localDate)
        }
    }

}