package streams.serialization

import org.apache.commons.lang3.StringUtils
import org.junit.Test
import org.neo4j.values.storable.CoordinateReferenceSystem.*
import org.neo4j.values.storable.DateTimeValue.*
import org.neo4j.values.storable.DateValue.*
import org.neo4j.values.storable.TimeValue.*
import org.neo4j.values.storable.Values.*
import java.time.ZoneOffset.*
import kotlin.test.assertEquals

class JacksoUtilTest {

    @Test
    fun shouldSerializeGeometryAndTemporalDataTypes() {
        // Given
        val expected = "{\"point2d\":{\"coordinates\":[1.0,2.0],\"crs\":{\"name\":\"cartesian\",\"table\":\"SR_ORG\",\"code\":7203,\"href\":\"http://spatialreference.org/ref/sr-org/7203/\",\"dimension\":2,\"geographic\":false,\"calculator\":{},\"type\":\"cartesian\"}}," +
                "\"point3d\":{\"coordinates\":[1.0,2.0,3.0],\"crs\":{\"name\":\"cartesian-3d\",\"table\":\"SR_ORG\",\"code\":9157,\"href\":\"http://spatialreference.org/ref/sr-org/9157/\",\"dimension\":3,\"geographic\":false,\"calculator\":{},\"type\":\"cartesian-3d\"}}," +
                "\"time\":\"14:00:00Z\",\"dateTime\":\"2017-12-17T17:14:35.123456789Z\"}"
        val map = linkedMapOf<String, Any>("point2d" to pointValue(Cartesian, 1.0, 2.0 ),
                "point3d" to pointValue(Cartesian_3D, 1.0, 2.0, 3.0 ),
                "time" to time( 14, 0, 0, 0, UTC ),
                "dateTime" to datetime(date( 2017, 12, 17 ), time( 17, 14, 35, 123456789, UTC)))

        // When
        val jsonString = JacksonUtil.getMapper().writeValueAsString(map)

        // Then
        assertEquals(expected, jsonString)
    }
}