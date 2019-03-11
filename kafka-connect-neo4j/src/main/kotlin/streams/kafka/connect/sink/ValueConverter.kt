package streams.kafka.connect.sink

import com.github.jcustenborder.kafka.connect.utils.data.AbstractConverter
import com.google.common.base.Preconditions
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.neo4j.driver.v1.Values
import java.math.BigDecimal
import java.math.BigInteger
import java.time.LocalTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.TimeUnit


class ValueConverter: AbstractConverter<MutableMap<String, Any?>>() {

    private val UTC = ZoneId.of("UTC")

    private fun setValue(result: MutableMap<String, Any?>?, fieldName: String?, value: Any?) {
        if (result != null && fieldName != null) {
            result[fieldName] = Values.value(value) ?: Values.NULL
        }
    }

    override fun newValue(): MutableMap<String, Any?> {
        return mutableMapOf()
    }

    override fun setBytesField(result: MutableMap<String, Any?>?, fieldName: String?, value: ByteArray?) {
        setValue(result, fieldName, value)
    }

    override fun setStringField(result: MutableMap<String, Any?>?, fieldName: String?, value: String?) {
        setValue(result, fieldName, value)
    }

    override fun setFloat32Field(result: MutableMap<String, Any?>?, fieldName: String?, value: Float?) {
        setValue(result, fieldName, value)
    }

    override fun setInt32Field(result: MutableMap<String, Any?>?, fieldName: String?, value: Int?) {
        setValue(result, fieldName, value)
    }

    override fun setArray(result: MutableMap<String, Any?>?, fieldName: String?, schema: Schema?, array: MutableList<Any?>?) {
        val convertedArray = array?.map { convertInner(it) }
        setValue(result, fieldName, convertedArray)
    }

    override fun setTimestampField(result: MutableMap<String, Any?>?, fieldName: String?, value: Date?) {
        if (value != null) {
            val localDate = value.toInstant().atZone(UTC).toLocalDateTime()
            setValue(result, fieldName, localDate)
        } else {
            setNullField(result, fieldName)
        }

    }

    override fun setTimeField(result: MutableMap<String, Any?>?, fieldName: String?, value: Date?) {
        if (value != null) {
            val time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(value.time))
            setValue(result, fieldName, time)
        } else {
            setNullField(result, fieldName)
        }
    }

    override fun setInt8Field(result: MutableMap<String, Any?>?, fieldName: String?, value: Byte?) {
        setValue(result, fieldName, value)
    }

    override fun setStructField(result: MutableMap<String, Any?>?, fieldName: String?, value: Struct?) {
        if (value != null) {
            val converted = convert(value) as MutableMap<Any?, Any?>
            setMap(result, fieldName, null, converted)
        } else {
            setNullField(result, fieldName)
        }
    }

    override fun setMap(result: MutableMap<String, Any?>?, fieldName: String?, schema: Schema?, map: MutableMap<Any?, Any?>?) {
        val newMap = map
                ?.mapKeys { it.key.toString() }
                ?.mapValues { convertInner(it.value) }
        setValue(result, fieldName, newMap)
    }

    override fun setNullField(result: MutableMap<String, Any?>?, fieldName: String?) {
        setValue(result, fieldName, null)
    }

    override fun setFloat64Field(result: MutableMap<String, Any?>?, fieldName: String?, value: Double?) {
        setValue(result, fieldName, value)
    }

    override fun setInt16Field(result: MutableMap<String, Any?>?, fieldName: String?, value: Short?) {
        setValue(result, fieldName, value)
    }

    override fun setInt64Field(result: MutableMap<String, Any?>?, fieldName: String?, value: Long?) {
        setValue(result, fieldName, value)
    }

    override fun setBooleanField(result: MutableMap<String, Any?>?, fieldName: String?, value: Boolean?) {
        setValue(result, fieldName, value)
    }

    override fun setDecimalField(result: MutableMap<String, Any?>?, fieldName: String?, value: BigDecimal?) {
        setValue(result, fieldName, value)
    }

    override fun setDateField(result: MutableMap<String, Any?>?, fieldName: String?, value: Date?) {
        if (value != null) {
            val localDate = value.toInstant().atZone(UTC).toLocalDate()
            setValue(result, fieldName, localDate)
        } else {
            setNullField(result, fieldName)
        }
    }

    private fun convertInner(value: Any?): Any? {
        return when (value) {
            is Struct, is Map<*, *> -> convert(value)
            else -> value
        }
    }
}