package streams.serialization

import org.junit.Test
import org.neo4j.values.storable.CoordinateReferenceSystem.*
import org.neo4j.values.storable.DateTimeValue.datetime
import org.neo4j.values.storable.DateValue.date
import org.neo4j.values.storable.TimeValue.time
import org.neo4j.values.storable.Values.pointValue
import java.time.ZoneOffset.UTC
import kotlin.test.assertEquals

class JacksonUtilTest {

    @Test
    fun shouldSerializeGeometryAndTemporalDataTypes() {
        // Given
        val expected = "{\"point2dCartesian\":{\"crs\":\"cartesian\",\"x\":1.0,\"y\":2.0,\"z\":null}," +
                "\"point3dCartesian\":{\"crs\":\"cartesian-3d\",\"x\":1.0,\"y\":2.0,\"z\":3.0}," +
                "\"point2dWgs84\":{\"crs\":\"wgs-84\",\"latitude\":1.0,\"longitude\":2.0,\"height\":null}," +
                "\"point3dWgs84\":{\"crs\":\"wgs-84-3d\",\"latitude\":1.0,\"longitude\":2.0,\"height\":3.0}," +
                "\"time\":\"14:00:00Z\",\"dateTime\":\"2017-12-17T17:14:35.123456789Z\"}"
        val map = linkedMapOf<String, Any>("point2dCartesian" to pointValue(Cartesian, 1.0, 2.0),
                "point3dCartesian" to pointValue(Cartesian_3D, 1.0, 2.0, 3.0),
                "point2dWgs84" to pointValue(WGS84, 1.0, 2.0),
                "point3dWgs84" to pointValue(WGS84_3D, 1.0, 2.0, 3.0),
                "time" to time( 14, 0, 0, 0, UTC),
                "dateTime" to datetime(date( 2017, 12, 17), time( 17, 14, 35, 123456789, UTC)))

        // When
        val jsonString = JacksonUtil.getMapper().writeValueAsString(map)

        // Then
        assertEquals(expected, jsonString)
    }
}