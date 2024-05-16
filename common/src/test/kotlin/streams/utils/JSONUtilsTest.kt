package streams.utils

import org.junit.Test
import org.neo4j.driver.Values
import org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian
import org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D
import org.neo4j.values.storable.CoordinateReferenceSystem.WGS84
import org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D
import streams.events.EntityType
import streams.events.Meta
import streams.events.NodeChange
import streams.events.NodePayload
import streams.events.OperationType
import streams.events.Schema
import streams.events.StreamsTransactionEvent
import java.time.OffsetTime
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import kotlin.test.assertEquals
import kotlin.test.assertFails

class JSONUtilsTest {

    @Test
    fun `should serialize driver Point Data Types`() {
        // Given
        val expected = "{\"point2dCartesian\":{\"crs\":\"cartesian\",\"x\":1.0,\"y\":2.0}," +
                "\"point3dCartesian\":{\"crs\":\"cartesian-3d\",\"x\":1.0,\"y\":2.0,\"z\":3.0}," +
                "\"point2dWgs84\":{\"crs\":\"wgs-84\",\"latitude\":1.0,\"longitude\":2.0}," +
                "\"point3dWgs84\":{\"crs\":\"wgs-84-3d\",\"latitude\":1.0,\"longitude\":2.0,\"height\":3.0}," +
                "\"time\":\"14:01:01.000000001Z\",\"dateTime\":\"2017-12-17T17:14:35.123456789Z\"}"

        val map = linkedMapOf<String, Any>("point2dCartesian" to Values.point(Cartesian.code, 1.0, 2.0),
                "point3dCartesian" to Values.point(Cartesian_3D.code, 1.0, 2.0, 3.0),
                "point2dWgs84" to Values.point(WGS84.code, 1.0, 2.0),
                "point3dWgs84" to Values.point(WGS84_3D.code, 1.0, 2.0, 3.0),
                "time" to Values.value(OffsetTime.of(14, 1, 1, 1, UTC)),
                "dateTime" to Values.value(ZonedDateTime.of(2017, 12, 17, 17, 14, 35, 123456789, UTC)))

        // When
        val jsonString = JSONUtils.writeValueAsString(map)

        // Then
        assertEquals(expected, jsonString)
    }

    @Test
    fun `should convert data to StreamsTransactionEvent`() {
        // given
        val timestamp = System.currentTimeMillis()
        val cdcData = StreamsTransactionEvent(
                meta = Meta(timestamp = timestamp,
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 1,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("prop1" to "foo", "bar" to 1), labels = listOf("LabelCDC")),
                        type = EntityType.node
                ),
                schema = Schema()
        )
        val cdcMap = mapOf<String, Any>(
                "meta" to mapOf("timestamp" to timestamp,
                        "username" to "user",
                        "txId" to 1,
                        "txEventId" to 0,
                        "txEventsCount" to 1,
                        "operation" to OperationType.created),
                "payload" to mapOf("id" to "0",
                        "before" to null,
                        "after" to NodeChange(properties = mapOf("prop1" to "foo", "bar" to 1), labels = listOf("LabelCDC")),
                        "type" to EntityType.node),
                "schema" to emptyMap<String, Any>()
        )
        val cdcString = """{
            |"meta":{"timestamp":$timestamp,"username":"user","txId":1,"txEventId":0,"txEventsCount":1,"operation":"created"},
            |"payload":{"id":"0","before":null,"after":{"properties":{"prop1":"foo","bar":1},"labels":["LabelCDC"]},"type":"node"},
            |"schema":{}
            |}""".trimMargin()

        val fromMap = JSONUtils.asStreamsTransactionEvent(cdcMap)
        val fromString = JSONUtils.asStreamsTransactionEvent(cdcString)
        assertEquals(cdcData, fromMap)
        assertEquals(cdcData, fromString)
    }

    @Test
    fun `should convert cdcMap wgs2D with height null to PointValue`() {
        // given
        val timestamp = System.currentTimeMillis()
        val expectedPointValue = Values.point(4326, 12.78, 56.7)

        //when
        val cdcMap = mapOf<String, Any>(
            "meta" to mapOf("timestamp" to timestamp,
                "username" to "user",
                "txId" to 1,
                "txEventId" to 0,
                "txEventsCount" to 1,
                "operation" to OperationType.created),
            "payload" to mapOf("id" to "0",
                "before" to null,
                "after" to NodeChange(properties = mapOf("location" to mapOf("crs" to "wgs-84", "longitude" to 12.78, "latitude" to 56.7, "height" to null)),
                    labels = listOf("LabelCDC")),
                "type" to EntityType.node),
            "schema" to mapOf("properties" to mapOf("location" to "PointValue"))
        )

        //then
        val actualEvent = JSONUtils.asStreamsTransactionEvent(cdcMap)
        val actualPointValue = actualEvent.payload.after?.properties?.get("location")
        assertEquals(expectedPointValue, actualPointValue)
    }

    @Test
    fun `should convert cdcString wgs2D with height null to PointValue`() {
        // given
        val timestamp = System.currentTimeMillis()
        val expectedPointValue = Values.point(4326, 12.78, 56.7)

        //when
        val cdcString = """{
            |"meta":{"timestamp":$timestamp,"username":"user","txId":1,"txEventId":0,"txEventsCount":1,"operation":"created"},
            |"payload":{"id":"0","before":null,"after":{"properties":{"location":{"crs":"wgs-84","longitude":12.78,"latitude":56.7,"height":null}},
            |"labels":["LabelCDC"]},"type":"node"},
            |"schema":{"properties":{"location":"PointValue"}}
            |}""".trimMargin()

        //then
        val actualEvent = JSONUtils.asStreamsTransactionEvent(cdcString)
        val actualPointValue = actualEvent.payload.after?.properties?.get("location")
        assertEquals(expectedPointValue, actualPointValue)
    }

    @Test
    fun `should convert cdcString cartesian2D with z null to PointValue`() {
        // given
        val timestamp = System.currentTimeMillis()
        val expectedPointValue = Values.point(7203, 12.78, 56.7)

        //when
        val cdcString = """{
            |"meta":{"timestamp":$timestamp,"username":"user","txId":1,"txEventId":0,"txEventsCount":1,"operation":"created"},
            |"payload":{"id":"0","before":null,"after":{"properties":{"location":{"crs":"cartesian","x":12.78,"y":56.7,"z":null}},
            |"labels":["LabelCDC"]},"type":"node"},
            |"schema":{"properties":{"location":"PointValue"}}
            |}""".trimMargin()

        //then
        val actualEvent = JSONUtils.asStreamsTransactionEvent(cdcString)
        val actualPointValue = actualEvent.payload.after?.properties?.get("location")
        assertEquals(expectedPointValue, actualPointValue)
    }

    @Test
    fun `should convert cdcMap cartesian2D with z null to PointValue`() {
        // given
        val timestamp = System.currentTimeMillis()
        val expectedPointValue = Values.point(7203, 12.78, 56.7)

        //when
        val cdcMap = mapOf<String, Any>(
            "meta" to mapOf("timestamp" to timestamp,
                "username" to "user",
                "txId" to 1,
                "txEventId" to 0,
                "txEventsCount" to 1,
                "operation" to OperationType.created),
            "payload" to mapOf("id" to "0",
                "before" to null,
                "after" to NodeChange(properties = mapOf("location" to mapOf("crs" to "cartesian", "x" to 12.78, "y" to 56.7, "z" to null)),
                    labels = listOf("LabelCDC")),
                "type" to EntityType.node),
            "schema" to mapOf("properties" to mapOf("location" to "PointValue"))
        )

        //then
        val actualEvent = JSONUtils.asStreamsTransactionEvent(cdcMap)
        val actualPointValue = actualEvent.payload.after?.properties?.get("location")
        assertEquals(expectedPointValue, actualPointValue)
    }

    @Test
    fun `should deserialize plain values`() {
        assertEquals("a", JSONUtils.readValue("a", stringWhenFailure = true))
        assertEquals(42, JSONUtils.readValue("42"))
        assertEquals(true, JSONUtils.readValue("true"))
        assertFails { JSONUtils.readValue("a") }
        // assertEquals(null, JSONUtils.readValue("null"))
    }
}