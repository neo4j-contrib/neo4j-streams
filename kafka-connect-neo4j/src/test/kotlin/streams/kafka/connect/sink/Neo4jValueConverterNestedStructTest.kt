package streams.kafka.connect.sink

import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.Test
import org.neo4j.driver.v1.Value
import org.neo4j.driver.v1.Values
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import streams.serialization.JSONUtils
import java.time.Instant
import java.time.ZonedDateTime
import java.util.*
import kotlin.test.assertEquals

class Neo4jValueConverterNestedStructTest {

    @Test
    fun `should convert nested map into map of neo4j values`() {
        // given
        val body = JSONUtils.readValue<Map<String, Any?>>(data).mapValues(::convertDate)

        // when
        val result = Neo4jValueConverter().convert(body) as Map<*, *>

        // then
        val expected = getExpectedMap()
        assertEquals(expected, result)
    }

    @Test
    fun `should convert nested struct into map of neo4j values`() {

        val body = getTreeStruct()

        // when
        val result = Neo4jValueConverter().convert(body) as Map<*, *>

        // then
        val expected = getExpectedMap()
        assertEquals(expected, result)
    }

    companion object {

        private val PREF_SCHEMA = SchemaBuilder.struct().name("org.neo4j.example.email.Preference")
                .field("preferenceType", SchemaBuilder.string())
                .field("endEffectiveDate", org.apache.kafka.connect.data.Timestamp.SCHEMA)
                .build()

        private val EMAIL_SCHEMA = SchemaBuilder.struct().name("org.neo4j.example.email.Email")
                .field("email", SchemaBuilder.string())
                .field("preferences", SchemaBuilder.array(PREF_SCHEMA))
                .build()

        private val TN_SCHEMA = SchemaBuilder.struct().name("org.neo4j.example.email.Transaction")
                .field("tn", SchemaBuilder.string())
                .field("preferences", SchemaBuilder.array(PREF_SCHEMA))
                .build()

        private val EVENT_SCHEMA = SchemaBuilder.struct().name("org.neo4j.example.email.Event")
                .field("eventId", SchemaBuilder.string())
                .field("eventTimestamp", org.apache.kafka.connect.data.Timestamp.SCHEMA)
                .field("emails", SchemaBuilder.array(EMAIL_SCHEMA).optional())
                .field("tns", SchemaBuilder.array(TN_SCHEMA).optional())
                .build()

        fun getTreeStruct(): Struct? {
            val source = JSONUtils.readValue<Map<String, Any?>>(data).mapValues(::convertDate)

            val emails = source["emails"] as List<Map<String,Any>>
            val email = Struct(EMAIL_SCHEMA)
                    .put("email",emails[0]["email"])
                    .put("preferences",
                            (emails[0]["preferences"] as List<Map<String,Any>>).map { Struct(PREF_SCHEMA).put("preferenceType",  it["preferenceType"]).put("endEffectiveDate",it["endEffectiveDate"]) })

            val emailList = listOf(email)
            val tnsList =
                    (source["tns"] as List<Map<String,Any>>).map {
                        Struct(TN_SCHEMA).put("tn",it["tn"])
                                .put("preferences", (it["preferences"] as List<Map<String,Any>>).map{ Struct(PREF_SCHEMA).put("preferenceType",  it["preferenceType"]).put("endEffectiveDate",it["endEffectiveDate"]) }) }

            return Struct(EVENT_SCHEMA)
                    .put("eventId", source["eventId"])
                    .put("eventTimestamp", source["eventTimestamp"])
                    .put("emails", emailList)
                    .put("tns", tnsList)
        }

        fun getExpectedMap(): Map<String, Value> {
            return JSONUtils.readValue<Map<String, Any?>>(data).mapValues(::convertDateNew).mapValues { Values.value(it.value) }
        }

        fun convertDate(it: Map.Entry<String,Any?>) : Any? =
                when {
                    it.value is Map<*,*> -> (it.value as Map<String,Any?>).mapValues(::convertDate)
                    it.value is Collection<*> -> (it.value as Collection<Any?>).map{ x-> convertDate(AbstractMap.SimpleEntry(it.key, x)) }
                    it.key.endsWith("Date") -> Date.from(Instant.parse(it.value.toString()))
                    it.key.endsWith("Timestamp") -> Date.from(Instant.parse(it.value.toString()))
                    else -> it.value
                }
        fun convertDateNew(it: Map.Entry<String,Any?>) : Any? =
                when {
                    it.value is Map<*,*> -> (it.value as Map<String,Any?>).mapValues(::convertDateNew)
                    it.value is Collection<*> -> (it.value as Collection<Any?>).map{ x-> convertDateNew(AbstractMap.SimpleEntry(it.key, x)) }
                    it.key.endsWith("Date") -> ZonedDateTime.parse(it.value.toString()).toLocalDateTime()
                    it.key.endsWith("Timestamp") -> ZonedDateTime.parse(it.value.toString()).toLocalDateTime()
                    else -> it.value
                }

        val data : String = """
{
  "eventId": "d70f306a-71d2-48d9-aea3-87b3808b764b",
  "eventTimestamp": "2019-08-21T22:29:22.151Z",
  "emails": [
    {
      "email": "century@gmail.com",
      "preferences": [
        {
          "preferenceType": "repair_subscription",
          "endEffectiveDate": "2019-05-08T14:51:26.116Z"
        },
        {
          "preferenceType": "ordering_subscription",
          "endEffectiveDate": "2019-05-08T14:51:26.116Z"
        },
        {
          "preferenceType": "marketing_subscription",
          "endEffectiveDate": "2019-05-08T14:51:26.116Z"
        }
      ]
    }
  ],
  "tns": [
    {
      "tn": "1122334455",
      "preferences": [
        {
          "preferenceType": "billing_subscription",
          "endEffectiveDate": "2019-10-22T14:51:26.116Z"
        },
        {
          "preferenceType": "repair_subscription",
          "endEffectiveDate": "2019-10-22T14:51:26.116Z"
        },
        {
          "preferenceType": "sms",
          "endEffectiveDate": "2019-10-22T14:51:26.116Z"
        }
      ]
    },
    {
      "tn": "5544332211",
      "preferences": [
        {
          "preferenceType": "acct_lookup",
          "endEffectiveDate": "2019-10-22T14:51:26.116Z"
        }
      ]
    }
  ]
}
    """.trimIndent()

    }

}

