package streams.kafka.connect.sink

import org.apache.kafka.connect.data.ConnectSchema
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.junit.Test
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.util.*
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
        val result = Neo4jValueConverter().convert(Struct(TEST_SCHEMA)) as Map<String, Any?>

        val target = result["target"]
        assertTrue { target is Double }
        assertEquals(123.4, target)

        val largeDecimal = result["largeDecimal"]
        assertTrue{ largeDecimal is String }
        assertEquals(BigDecimal.valueOf(Double.MAX_VALUE).pow(2).toPlainString(), largeDecimal)

        val byteArray = result["byteArray"]
        assertTrue{ byteArray is ByteArray }
        assertEquals("Foo", String(byteArray as ByteArray))

        val int64 = result["int64"]
        assertTrue{ int64 is Long }
        assertEquals(Long.MAX_VALUE, int64)

        val int64Timestamp = result["int64Timestamp"]
        assertTrue{ int64Timestamp is LocalDateTime }
        assertEquals(Date.from(Instant.ofEpochMilli(789)).toInstant().atZone(utc).toLocalDateTime(), int64Timestamp)

        val int32 = result["int32"]
        assertTrue{ int32 is Int }
        assertEquals(123, int32)

        val int32Date = result["int32Date"]
        assertTrue{ int32Date is LocalDate }
        assertEquals(Date.from(Instant.ofEpochMilli(456)).toInstant().atZone(utc).toLocalDate(), int32Date)

        val int32Time = result["int32Time"]
        assertTrue{ int32Time is LocalTime }
        assertEquals(Date.from(Instant.ofEpochMilli(123)).toInstant().atZone(utc).toLocalTime(), int32Time)

        val nullField = result["nullField"]
        assertTrue{ nullField == null }

        val nullFieldBytes = result["nullFieldBytes"]
        assertTrue{ nullFieldBytes == null }

    }

    @Test
    fun `should convert BigDecimal into String neo4j value if is a positive less than Double MIN_VALUE`() {

        val number = BigDecimal.valueOf(Double.MIN_VALUE).pow(2)
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is String }
        assertEquals(number.toPlainString(), item)

        val result2 = Neo4jValueConverter().convert(getItemElement(null))
        val item2 = result2["item"]

        assertTrue{ item2 == null }
    }

    @Test
    fun `should convert BigDecimal into String neo4j value if is a negative less than Double MAX_VALUE`() {

        val number = - (BigDecimal.valueOf(Double.MAX_VALUE)).multiply(BigDecimal.valueOf(2))
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is String }
        assertEquals(number.toPlainString(), item)
    }

    @Test
    fun `should convert BigDecimal into String neo4j value if is greater than Double MAX_VALUE`() {

        val number = BigDecimal.valueOf(Double.MAX_VALUE).pow(2)
        val result = Neo4jValueConverter().convert(getItemElement(number))
        val item = result["item"]

        assertTrue{ item is String }
        assertEquals(number.toPlainString(), item)
    }

    @Test
    fun `should convert BigDecimal into Double neo4j value if is less than Double MAX_VALUE and greater than Double MIN_VALUE`() {

        val number = 3456.78
        val result =  Neo4jValueConverter().convert(getItemElement(BigDecimal.valueOf(number)))
        val item = result["item"]

        assertTrue{ item is Double }
        assertEquals(number, item)
    }

    @Test
    fun `should convert BigDecimal into Double neo4j value if is equal to Double MAX_VALUE`() {

        val number = Double.MAX_VALUE
        val result = Neo4jValueConverter().convert(getItemElement(BigDecimal.valueOf(number)))
        val item = result["item"]

        assertEquals(number, item)
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

        assertEquals(double, result["double"])
        assertEquals(long, result["long"])
        assertEquals(bigDouble.toPlainString(), result["bigDouble"])
        assertEquals(string, result["string"])
        assertEquals(date.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime(), result["date"])
    }

    @Test
    fun `should be able to process a CUD node format AVRO structure`() {
        val idsStruct = SchemaBuilder.struct()
            .field("ID_NUM", Schema.INT64_SCHEMA)
            .build()
        val propertiesStruct = SchemaBuilder.struct()
            .field("ID_NUM", Schema.INT64_SCHEMA)
            .field("EMP_NAME", Schema.STRING_SCHEMA)
            .field("PHONE", Schema.STRING_SCHEMA)
            .build()
        val cudSchema = SchemaBuilder.struct()
            .field("type", Schema.STRING_SCHEMA)
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("op", Schema.STRING_SCHEMA)
            .field("ids", idsStruct)
            .field("properties", propertiesStruct)
            .build()

        val cudStruct = Struct(cudSchema)
            .put("type", "node")
            .put("labels", listOf("EMPLOYEE"))
            .put("op", "merge")
            .put("ids", Struct(idsStruct).put("ID_NUM", 36950L))
            .put("properties", Struct(propertiesStruct)
                .put("ID_NUM", 36950L)
                .put("EMP_NAME", "Edythe")
                .put("PHONE", "3333")
            )

        val actual = Neo4jValueConverter().convert(cudStruct) as Map<*, *>
        val expected = mapOf<String, Any?>(
            "type" to "node",
            "labels" to listOf("EMPLOYEE"),
            "op" to "merge",
            "ids" to mapOf<String, Any?>("ID_NUM" to 36950L),
            "properties" to mapOf<String, Any?>(
                "ID_NUM" to 36950L,
                "EMP_NAME" to "Edythe",
                "PHONE" to "3333"
            )
        )
        assertEquals(expected, actual)
    }

    @Test
    fun `should be able to process a CUD rel format AVRO structure`() {
        val fromIdStruct = SchemaBuilder.struct()
            .field("person_id", Schema.INT64_SCHEMA)
            .build()
        val toIdStruct = SchemaBuilder.struct()
            .field("product_id", Schema.INT64_SCHEMA)
            .build()
        val fromNodeStruct = SchemaBuilder.struct()
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("op", Schema.STRING_SCHEMA)
            .field("ids", fromIdStruct)
            .build()
        val toNodeStruct = SchemaBuilder.struct()
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("op", Schema.STRING_SCHEMA)
            .field("ids", toIdStruct)
            .build()
        val propertiesStruct = SchemaBuilder.struct()
            .field("quantity", Schema.INT32_SCHEMA)
            .field("year", Schema.STRING_SCHEMA)
            .build()
        val cudSchema = SchemaBuilder.struct()
            .field("type", Schema.STRING_SCHEMA)
            .field("rel_type", Schema.STRING_SCHEMA)
            .field("op", Schema.STRING_SCHEMA)
            .field("properties", propertiesStruct)
            .field("from", fromNodeStruct)
            .field("to", toNodeStruct)
            .build()

        val cudStruct = Struct(cudSchema)
            .put("type", "relationship")
            .put("rel_type", "BOUGHT")
            .put("op", "merge")
            .put("from", Struct(fromNodeStruct)
                .put("ids", Struct(fromIdStruct).put("person_id", 1L))
                .put("op", "match")
                .put("labels", listOf("Person"))
            )
            .put("to", Struct(toNodeStruct)
                .put("ids", Struct(toIdStruct).put("product_id", 10L))
                .put("op", "merge")
                .put("labels", listOf("Product"))
            )
            .put("properties", Struct(propertiesStruct)
                .put("quantity", 10)
                .put("year", "2023")
            )

        val actual = Neo4jValueConverter().convert(cudStruct) as Map<*, *>
        val expected = mapOf<String, Any?>(
            "type" to "relationship",
            "rel_type" to "BOUGHT",
            "op" to "merge",
            "from" to mapOf<String, Any?>(
                "ids" to mapOf<String, Any?>("person_id" to 1L),
                "labels" to listOf("Person"),
                "op" to "match"
            ),
            "to" to mapOf<String, Any?>(
                "ids" to mapOf<String, Any?>("product_id" to 10L),
                "labels" to listOf("Product"),
                "op" to "merge"
            ),
            "properties" to mapOf<String, Any?>(
                "quantity" to 10,
                "year" to "2023"
            )
        )
        assertEquals(expected, actual)
    }

    @Test
    fun `should be able to process a nested AVRO structure`() {
        val trainSchema = SchemaBuilder.struct()
            .field("internationalTrainNumber", Schema.STRING_SCHEMA)
            .field("trainDate", Schema.STRING_SCHEMA).build()
        val mySchema = SchemaBuilder.struct()
            .field("trainId", trainSchema)
            .field("coreId", Schema.STRING_SCHEMA).build()

        val trainIdStruct = Struct(trainSchema)
            .put("internationalTrainNumber", "46261")
            .put("trainDate", "2021-05-20")
        val rootStruct = Struct(mySchema)
            .put("trainId", trainIdStruct)
            .put("coreId", "000000046261")

        val result = Neo4jValueConverter().convert(rootStruct) as Map<*, *>
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

        fun getExpectedMap(): Map<String, Any?> {
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

        fun getItemElement(number: Any?): Map<String, Any?> = mapOf("item" to number)
    }

}

