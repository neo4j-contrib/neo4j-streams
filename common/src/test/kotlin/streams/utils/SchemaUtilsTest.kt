package streams.utils

import org.junit.Test
import streams.events.Constraint
import streams.events.StreamsConstraintType
import streams.utils.SchemaUtils.getNodeKeys
import kotlin.test.assertEquals

class SchemaUtilsTest {

    @Test
    fun `getNodeKeys should select the constraint with lowest properties`() {
        val props = mapOf("LabelA" to setOf("foo", "bar"),
                "LabelB" to setOf("foo", "bar", "fooBar"),
                "LabelC" to setOf("foo"))
        val constraints = props.map {
            Constraint(label = it.key, properties = it.value, type = StreamsConstraintType.UNIQUE)
        }
        val keys = getNodeKeys(props.keys.toList(), setOf("prop", "foo", "bar"), constraints)
        assertEquals(setOf("foo"), keys)
    }

    @Test
    fun `getNodeKeys should return empty in case it didn't match anything`() {
        val props = mapOf("LabelA" to setOf("foo", "bar"),
                "LabelB" to setOf("foo", "bar", "fooBar"),
                "LabelC" to setOf("foo"))
        val constraints = props.map {
            Constraint(label = it.key, properties = it.value, type = StreamsConstraintType.UNIQUE)
        }
        val keys = getNodeKeys(props.keys.toList(), setOf("prop", "key"), constraints)
        assertEquals(emptySet(), keys)
    }
}