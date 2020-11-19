package streams.kafka.connect.sink.converters

import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.BigInteger
import java.time.LocalTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.TimeUnit


class Neo4jValueConverter: MapValueConverter<Value>() {
    private val log = LoggerFactory.getLogger(Neo4jValueConverter::class.java)

    companion object {
        @JvmStatic private val UTC = ZoneId.of("UTC")
    }


    override fun convert(data: Any?): MutableMap<String, Value?> {
        val dataManipulatedForNeo4j = if(data is Map<*, *>)
            data.mapValues {
                val value = it.value
                val errorMsg = "Cannot convert %s key into Neo4j Type, it exceeds the Neo4j %s value, it will be converted into a String"
                when (value) {
                    is BigDecimal -> {
                        val doubleValue = value.toDouble()
                        val fitsScale = doubleValue != Double.POSITIVE_INFINITY
                                && doubleValue != Double.NEGATIVE_INFINITY
                                && value.compareTo(BigDecimal.valueOf(doubleValue)) == 0
                        if (fitsScale) {
                            doubleValue
                        } else {
                            log.warn(errorMsg.format(it.key, "Double"))
                            value.toPlainString()
                        }
                    }
                    is BigInteger -> {
                        try {
                            value.longValueExact()
                        } catch (e: java.lang.ArithmeticException) {
                            log.warn(errorMsg.format(it.key, "Long"))
                            value.toString()
                        }
                    }
                    else -> value
                }
            } else data

        return super.convert(dataManipulatedForNeo4j)
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