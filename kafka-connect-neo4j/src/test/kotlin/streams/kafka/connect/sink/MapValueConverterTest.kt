package streams.kafka.connect.sink

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.Test
import streams.kafka.connect.sink.converters.MapValueConverter
import kotlin.test.assertEquals

class MapValueConverterTest {

    @Test
    fun `should convert tree struct into map of String,Any?`() {
        // given
        // this test generates a simple tree structure like this
        //           body
        //          /    \
        //         p     ul
        //               |
        //               li
        val body = getTreeStruct()

        // when
        val result = MapValueConverter<Any>().convert(body) as Map<*, *>

        // then
        val expected = getTreeMap()
        assertEquals(expected, result)
    }

    @Test
    fun `should convert tree simple map into map of String,Any?`() {
        // given
        // this test generates a simple tree structure like this
        //           body
        //          /    \
        //         p     ul
        //               |
        //               li
        val body = getTreeMap()

        // when
        val result = MapValueConverter<Any>().convert(body) as Map<*, *>

        // then
        val expected = getTreeMap()
        assertEquals(expected, result)
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
    }

}

