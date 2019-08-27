package streams.kafka.connect.sink.converters

import com.github.jcustenborder.kafka.connect.utils.data.AbstractConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import java.math.BigDecimal
import java.util.*

open class MapValueConverter<T>: AbstractConverter<MutableMap<String, T?>>() {

    open fun setValue(result: MutableMap<String, T?>?, fieldName: String?, value: Any?) {
        if (result != null && fieldName != null) {
            result[fieldName] = value as T
        }
    }

    override fun newValue(): MutableMap<String, T?> {
        return mutableMapOf()
    }

    override fun setBytesField(result: MutableMap<String, T?>?, fieldName: String?, value: ByteArray?) {
        setValue(result, fieldName, value)
    }

    override fun setStringField(result: MutableMap<String, T?>?, fieldName: String?, value: String?) {
        setValue(result, fieldName, value)
    }

    override fun setFloat32Field(result: MutableMap<String, T?>?, fieldName: String?, value: Float?) {
        setValue(result, fieldName, value)
    }

    override fun setInt32Field(result: MutableMap<String, T?>?, fieldName: String?, value: Int?) {
        setValue(result, fieldName, value)
    }

    override fun setArray(result: MutableMap<String, T?>?, fieldName: String?, schema: Schema?, array: MutableList<Any?>?) {
        val convertedArray = array?.map { convertInner(it) }
        setValue(result, fieldName, convertedArray)
    }

    override fun setTimestampField(result: MutableMap<String, T?>?, fieldName: String?, value: Date?) {
        setValue(result, fieldName, value)

    }

    override fun setTimeField(result: MutableMap<String, T?>?, fieldName: String?, value: Date?) {
        setValue(result, fieldName, value)
    }

    override fun setInt8Field(result: MutableMap<String, T?>?, fieldName: String?, value: Byte?) {
        setValue(result, fieldName, value)
    }

    override fun setStructField(result: MutableMap<String, T?>?, fieldName: String?, value: Struct?) {
        if (value != null) {
            val converted = convert(value) as MutableMap<Any?, Any?>
            setMap(result, fieldName, null, converted)
        } else {
            setNullField(result, fieldName)
        }
    }

    override fun setMap(result: MutableMap<String, T?>?, fieldName: String?, schema: Schema?, value: MutableMap<Any?, Any?>?) {
        if (value != null) {
            val converted = convert(value) as MutableMap<Any?, Any?>
            setValue(result, fieldName, converted)
        } else {
            setNullField(result, fieldName)
        }
    }

    override fun setNullField(result: MutableMap<String, T?>?, fieldName: String?) {
        setValue(result, fieldName, null)
    }

    override fun setFloat64Field(result: MutableMap<String, T?>?, fieldName: String?, value: Double?) {
        setValue(result, fieldName, value)
    }

    override fun setInt16Field(result: MutableMap<String, T?>?, fieldName: String?, value: Short?) {
        setValue(result, fieldName, value)
    }

    override fun setInt64Field(result: MutableMap<String, T?>?, fieldName: String?, value: Long?) {
        setValue(result, fieldName, value)
    }

    override fun setBooleanField(result: MutableMap<String, T?>?, fieldName: String?, value: Boolean?) {
        setValue(result, fieldName, value)
    }

    override fun setDecimalField(result: MutableMap<String, T?>?, fieldName: String?, value: BigDecimal?) {
        setValue(result, fieldName, value)
    }

    override fun setDateField(result: MutableMap<String, T?>?, fieldName: String?, value: Date?) {
        setValue(result, fieldName, value)
    }

    open fun convertInner(value: Any?): Any? {
        return when (value) {
            is Struct, is Map<*, *> -> convert(value)
            is Collection<*> -> value.map(::convertInner)
            is Array<*> -> if (value.javaClass.componentType.isPrimitive) value else value.map(::convertInner)
            else -> value
        }
    }
}