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
    fun `getNodeKeys should select (with multiple labels) on of the constraints with lowest properties and, with same size, the first in alphabetical order`() {
        val listNodeKeys = (1..50).map {
            val pair1 = "LabelA" to setOf("foo", "aab")
            val pair2 = "LabelB" to setOf("bar", "foo")
            val pair3 = "LabelC" to setOf("baz", "bar")
            val pair4 = "LabelB" to setOf("bar", "bez")
            val pair5 = "LabelA" to setOf("bar", "baa", "xcv")
            val pair6 = "LabelC" to setOf("aaa", "baa", "xcz")
            val pair7 = "LabelA" to setOf("foo", "aac")
            val props = mutableListOf(pair1, pair2, pair3, pair4, pair5, pair6, pair7)
                    .shuffled()
            val constraints = props.map {
                Constraint(label = it.first, properties = it.second, type = StreamsConstraintType.UNIQUE)
            }.shuffled()
            getNodeKeys(props.map { it.first }.shuffled(), setOf("prop", "prop2", "foo", "bar", "baz", "bez", "aaa", "aab", "baa", "aac", "xcz", "xcv").shuffled().toSet(), constraints)
        }
        val setNodeKeys = listNodeKeys.toSet()
        assertEquals(1, setNodeKeys.size)
        assertEquals(setOf("aab", "foo"), setNodeKeys.first())
    }

    @Test
    fun `getNodeKeys should select (with one label) on of the constraints with lowest properties and, with same size, the first in alphabetical order`() {
        val listNodeKeys = (1..50).map {
            val pair1 = "LabelA" to setOf("foo", "bar")
            val pair2 = "LabelA" to setOf("bar", "foo")
            val pair3 = "LabelA" to setOf("baz", "bar")
            val pair4 = "LabelA" to setOf("bar", "bez")
            val props = mutableListOf(pair1, pair2, pair3, pair4)
                    .shuffled()
            val constraints = props.map {
                Constraint(label = it.first, properties = it.second, type = StreamsConstraintType.UNIQUE)
            }.shuffled()
            getNodeKeys(listOf("LabelA"), setOf("prop", "foo", "bar", "baz", "bez").shuffled().toSet(), constraints)
        }
        val setNodeKeys = listNodeKeys.toSet()
        assertEquals(1, setNodeKeys.size)
        assertEquals(setOf("bar", "baz"), setNodeKeys.first())
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