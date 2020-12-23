package streams.kafka.connect.sink

import org.apache.kafka.connect.data.*
import org.junit.Test
import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.neo4j.driver.internal.value.*
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import java.math.BigDecimal
import java.time.Instant
import java.time.ZoneId
import java.util.Date
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Neo4jValueConverterTest {

    @Test
    fun `should convert tree struct into map of neo4j values`() {
        // given
        // this test generates a simple tree structure like this
        //           body
        //          /    \
        //         p     ul
        //               |
        //               li
        val body = getTreeStruct()

        // when
        val result = Neo4jValueConverter().convert(body) as Map<*, *>

        // then
        val expected = getExpectedMap()
        assertEquals(expected, result)
    }

    @Test
    fun `should convert tree simple map into map of neo4j values`() {
        // given
        // this test generates a simple tree structure like this
        //           body
        //          /    \
        //         p     ul
        //               |
        //               li
        val body = getTreeMap()

        // when
        val result = Neo4jValueConverter().convert(body) as Map<*, *>

        // then
        val expected = getExpectedMap()
        assertEquals(expected, result)
    }

    @Test
    fun `should convert tree with mixes types into map of neo4j values`() {

        val utc = ZoneId.of("UTC")
        val result = Neo4jValueConverter().convert(Struct(TEST_SCHEMA)) as Map<String, Value?>

        val target = result["target"]
        assertTrue{ target is FloatValue }
        assertEquals(123.4, target?.asDouble())

        val largeDecimal = result["largeDecimal"]
        assertTrue{ largeDecimal is StringValue }
        assertEquals(BigDecimal.valueOf(Double.MAX_VALUE).pow(2).toPlainString(), largeDecimal?.asString())

        val byteArray = result["byteArray"]
        assertTrue{ byteArray is BytesValue }
        assertEquals("Foo".toByteArray().map { it }, byteArray?.asByteArray()?.map { it })

        val int64 = result["int64"]
        assertTrue{ int64 is IntegerValue }
        assertEquals(Long.MAX_VALUE, int64?.asLong())

        val int64Timestamp = result["int64Timestamp"]
        assertTrue{ int64Timestamp is LocalDateTimeValue }
        assertEquals(Date.from(Instant.ofEpochMilli(789)).toInstant().atZone(utc).toLocalDateTime(), int64Timestamp?.asLocalDateTime())

        val int32 = result["int32"]
        assertTrue{ int32 is IntegerValue }
        assertEquals(123, int32?.asInt())

        val int32Date = result["int32Date"]
        assertTrue{ int32Date is DateValue }
        assertEquals(Date.from(Instant.ofEpochMilli(456)).toInstant().atZone(utc).toLocalDate(), int32Date?.asLocalDate())

        val int32Time = result["int32Time"]
        assertTrue{ int32Time is LocalTimeValue }
        assertEquals(Date.from(Instant.ofEpochMilli(123)).toInstant().atZone(utc).toLocalTime(), int32Time?.asLocalTime())

    }

    @Test
    fun `should convert BigDecimal into String neo4j value if is a positive less than Double MIN_VALUE`() {

        val number = BigDecimal.valueOf(Double.MIN_VALUE).pow(2)
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is StringValue }
        assertEquals(number.toPlainString(), item?.asString())

        val number2 = BigDecimal.valueOf(Double.MIN_VALUE).pow(2)
        val result2 = Neo4jValueConverter().convert(mapOf(null to number2))
        val item2 = result2["item"]

        assertTrue{ item2 is StringValue }
        assertEquals(number.toPlainString(), item?.asString())
    }

    @Test
    fun `should convert BigDecimal into String neo4j value if is a negative less than Double MAX_VALUE`() {

        val number = - (BigDecimal.valueOf(Double.MAX_VALUE)).multiply(BigDecimal.valueOf(2))
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is StringValue }
        assertEquals(number.toPlainString(), item?.asString())
    }

    @Test
    fun `should convert BigDecimal into String neo4j value if is greater than Double MAX_VALUE`() {

        val number = BigDecimal.valueOf(Double.MAX_VALUE).pow(2)
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is StringValue }
        assertEquals(number.toPlainString(), item?.asString())
    }

    @Test
    fun `should convert BigDecimal into Double neo4j value if is less than Double MAX_VALUE and greater than Double MIN_VALUE`() {

        val number = 3456.78
        val result =  Neo4jValueConverter().convert(getItemElement(BigDecimal.valueOf(number)))
        val item = result["item"]

        assertTrue{ item is FloatValue }
        assertEquals(number, item?.asDouble())
    }

    @Test
    fun `should convert BigDecimal into Double neo4j value if is equal to Double MAX_VALUE`() {

        val number = Double.MAX_VALUE
        val result = Neo4jValueConverter().convert(getItemElement(BigDecimal.valueOf(number)))
        val item = result["item"]

        assertTrue{ item is FloatValue }
        assertEquals(number, item?.asDouble())
    }

    @Test
    fun `should convert properly mixed items`() {

        val double = Double.MAX_VALUE
        val long = Long.MAX_VALUE
        val bigDouble =  BigDecimal.valueOf(Double.MAX_VALUE).pow(2)
        val string =  "FooBar"
        val date = Date()
        val result = Neo4jValueConverter().convert(mapOf(
                "double" to double,
                "long" to long,
                "bigDouble" to bigDouble,
                "string" to string,
                "date" to date))

        assertEquals(double, result["double"]?.asDouble())
        assertEquals(long, result["long"]?.asLong())
        assertEquals(bigDouble.toPlainString(), result["bigDouble"]?.asString())
        assertEquals(string, result["string"]?.asString())
        assertEquals(date.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime(), result["date"]?.asLocalDateTime())
    }

    companion object {
        private val LI_SCHEMA = SchemaBuilder.struct().name("org.neo4j.example.html.LI")
                .field("value", Schema.OPTIONAL_STRING_SCHEMA)
                .field("class", SchemaBuilder.array(Schema.STRING_SCHEMA).optional())
                .build()

        private val UL_SCHEMA = SchemaBuilder.struct().name("org.neo4j.example.html.UL")
                .field("value", SchemaBuilder.array(LI_SCHEMA))
                .build()

        private val P_SCHEMA = SchemaBuilder.struct().name("org.neo4j.example.html.P")
                .field("value", Schema.OPTIONAL_STRING_SCHEMA)
                .build()

        private val BODY_SCHEMA = SchemaBuilder.struct().name("org.neo4j.example.html.BODY")
                .field("ul", SchemaBuilder.array(UL_SCHEMA).optional())
                .field("p", SchemaBuilder.array(P_SCHEMA).optional())
                .build()

        private val TEST_SCHEMA = SchemaBuilder.struct().name("test.schema")
                .field("target",
                        ConnectSchema(Schema.Type.BYTES,
                                false,
                                BigDecimal.valueOf(123.4),
                                Decimal.LOGICAL_NAME,
                                null, null))
                .field("largeDecimal",
                        ConnectSchema(Schema.Type.BYTES,
                                false,
                                BigDecimal.valueOf(Double.MAX_VALUE).pow(2),
                                Decimal.LOGICAL_NAME,
                                null, null))
                .field("byteArray",
                        ConnectSchema(Schema.Type.BYTES,
                                false,
                                "Foo".toByteArray(),
                                "name.byteArray",
                                null, null))
                .field("int64",
                        ConnectSchema(Schema.Type.INT64,
                                false,
                                Long.MAX_VALUE,
                                "name.int64",
                                null, null))
                .field("int64Timestamp",
                        ConnectSchema(Schema.Type.INT64,
                                false,
                                Date.from(Instant.ofEpochMilli(789)),
                                Timestamp.LOGICAL_NAME,
                                null, null))
                .field("int32",
                        ConnectSchema(Schema.Type.INT32,
                                false,
                                123,
                                "name.int32",
                                null, null))
                .field("int32Date",
                        ConnectSchema(Schema.Type.INT32,
                                false,
                                Date.from(Instant.ofEpochMilli(456)),
                                org.apache.kafka.connect.data.Date.LOGICAL_NAME,
                                null, null))
                .field("int32Time",
                        ConnectSchema(Schema.Type.INT32,
                                false,
                                Date.from(Instant.ofEpochMilli(123)),
                                Time.LOGICAL_NAME,
                                null, null))
                .field("nullField",
                        ConnectSchema(Schema.Type.INT64,
                                false,
                                null,
                                Time.LOGICAL_NAME,
                                null, null))
                .field("nullFieldBytes",
                        ConnectSchema(Schema.Type.BYTES,
                                false,
                                null,
                                Time.LOGICAL_NAME,
                                null, null))
                .build()

        fun getTreeStruct(): Struct? {
            val firstUL = Struct(UL_SCHEMA).put("value", listOf(
                    Struct(LI_SCHEMA).put("value", "First UL - First Element"),
                    Struct(LI_SCHEMA).put("value", "First UL - Second Element")
                            .put("class", listOf("ClassA", "ClassB"))
            ))
            val secondUL = Struct(UL_SCHEMA).put("value", listOf(
                    Struct(LI_SCHEMA).put("value", "Second UL - First Element"),
                    Struct(LI_SCHEMA).put("value", "Second UL - Second Element")
            ))
            val ulList = listOf(firstUL, secondUL)
            val pList = listOf(
                    Struct(P_SCHEMA).put("value", "First Paragraph"),
                    Struct(P_SCHEMA).put("value", "Second Paragraph")
            )
            return Struct(BODY_SCHEMA)
                    .put("ul", ulList)
                    .put("p", pList)
        }

        fun getExpectedMap(): Map<String, Value> {
            val firstULMap = mapOf("value" to listOf(
                    mapOf("value" to Values.value("First UL - First Element"), "class" to Values.NULL),
                    mapOf("value" to Values.value("First UL - Second Element"), "class" to Values.value(listOf("ClassA", "ClassB")))))
            val secondULMap = mapOf("value" to listOf(
                    mapOf("value" to Values.value("Second UL - First Element"), "class" to Values.NULL),
                    mapOf("value" to Values.value("Second UL - Second Element"), "class" to Values.NULL)))
            val ulListMap = Values.value(listOf(firstULMap, secondULMap))
            val pListMap = Values.value(listOf(mapOf("value" to Values.value("First Paragraph")),
                    mapOf("value" to Values.value("Second Paragraph"))))
            return mapOf("ul" to ulListMap, "p" to pListMap)
        }

        fun getTreeMap(): Map<String, Any?> {
            val firstULMap = mapOf("value" to listOf(
                    mapOf("value" to "First UL - First Element", "class" to null),
                    mapOf("value" to "First UL - Second Element", "class" to listOf("ClassA", "ClassB"))))
            val secondULMap = mapOf("value" to listOf(
                    mapOf("value" to "Second UL - First Element", "class" to null),
                    mapOf("value" to "Second UL - Second Element", "class" to null)))
            val ulListMap = listOf(firstULMap, secondULMap)
            val pListMap = listOf(mapOf("value" to "First Paragraph"),
                    mapOf("value" to "Second Paragraph"))
            return mapOf("ul" to ulListMap, "p" to pListMap)
        }

        fun getItemElement(number: Any): Map<String, Any> = mapOf("item" to number)
    }

}

