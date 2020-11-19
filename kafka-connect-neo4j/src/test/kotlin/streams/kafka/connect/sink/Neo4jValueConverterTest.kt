package streams.kafka.connect.sink

import org.apache.kafka.connect.data.*
import org.junit.Test
import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.neo4j.driver.internal.value.*
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import java.math.BigDecimal
import java.math.BigInteger
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

        val result = Neo4jValueConverter().convert(Struct(TEST_SCHEMA)) as Map<*, *>

        assertTrue{ result["target"] is FloatValue }
        assertTrue{ result["largeDecimal"] is StringValue }
        assertTrue{ result["byteArray"] is BytesValue }
        assertTrue{ result["int64"] is IntegerValue }
        assertTrue{ result["int64Timestamp"] is LocalDateTimeValue }
        assertTrue{ result["int32"] is IntegerValue }
        assertTrue{ result["int32Date"] is DateValue }
        assertTrue{ result["int32Time"] is LocalTimeValue }
    }

    @Test
    fun `should convert BigInteger into String value if is less then Long MIN_VALUE`() {

        val number = BigInteger.valueOf(Long.MIN_VALUE).pow(2)
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is StringValue }
        assertEquals(number.toString(), item?.asString())
    }

    @Test
    fun `should convert BigInteger into String value if is greater then Long MAX_VALUE`() {

        val number = BigInteger.valueOf(Long.MAX_VALUE).pow(2)
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is StringValue }
        assertEquals(number.toString(), item?.asString())
    }

    @Test
    fun `should convert BigInteger into Long neo4j value if is equal to Long MAX_VALUE`() {

        val number = Long.MAX_VALUE
        val result = Neo4jValueConverter().convert(getItemElement(BigInteger.valueOf(number)))
        val item = result["item"]

        assertTrue{ item is IntegerValue }
        assertEquals(number, item?.asLong())
    }

    @Test
    fun `should convert BigInteger into Long neo4j value if is less then Long MAX_VALUE and greater then Long MIN_VALUE`() {

        val number: Long = 12345
        val result = Neo4jValueConverter().convert(getItemElement(BigInteger.valueOf(number)))
        val item = result["item"]

        assertTrue{ item is IntegerValue }
        assertEquals(number, item?.asLong())
    }

    @Test
    fun `should convert BigDecimal into String neo4j value if is a positive less then Double MIN_VALUE`() {

        val number = BigDecimal.valueOf(Double.MIN_VALUE).pow(2)
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is StringValue }
        assertEquals(number.toPlainString(), item?.asString())
    }

    @Test
    fun `should convert BigDecimal into String neo4j value if is a negative less then Double MAX_VALUE`() {

        val number = - (BigDecimal.valueOf(Double.MAX_VALUE)).multiply(BigDecimal.valueOf(2))
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is StringValue }
        assertEquals(number.toPlainString(), item?.asString())
    }

    @Test
    fun `should convert BigDecimal into String neo4j value if is greater then Double MAX_VALUE`() {

        val number = BigDecimal.valueOf(Double.MAX_VALUE).pow(2)
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is StringValue }
        assertEquals(number.toPlainString(), item?.asString())
    }

    @Test
    fun `should convert BigDecimal into Double neo4j value if is less then Double MAX_VALUE and greater then Double MIN_VALUE`() {

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
                                Date(),
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
                                Date(),
                                org.apache.kafka.connect.data.Date.LOGICAL_NAME,
                                null, null))
                .field("int32Time",
                        ConnectSchema(Schema.Type.INT32,
                                false,
                                Date.from(Instant.ofEpochMilli(123)),
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

