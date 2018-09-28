package kafka

import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * @author mh
 * *
 * @since 20.03.18
 */
class NodePatternTest {

    @Test fun testParseSingleEntry() {
        fun parse(pattern:String) = NodePattern.parse(pattern, topics = listOf("neo4j"))
        assertEquals(listOf(NodePattern("neo4j", labels = emptyList(), all = true)), parse("neo4j:*"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true)), parse("neo4j:Person"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true)), parse("neo4j:Person{*}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, include = listOf("name"))), parse("neo4j:Person{*,name}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = false, include = listOf("name"))), parse("neo4j:Person{name}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, include = listOf("name", "age"))), parse("neo4j:Person{*,name, age}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, exclude = listOf("name"))), parse("neo4j:Person{*,-name}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, exclude = listOf("name"))), parse("neo4j:Person{-name}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, exclude = listOf("name", "age"))), parse("neo4j:Person{*,-name, -age}"))
    }

    @Test fun testParseMultipleEntry() {
        fun parse(pattern:String) = NodePattern.parse(pattern, topics = listOf("neo4j","movie"))
        assertEquals(listOf(NodePattern("neo4j", labels = emptyList(), all = true)), parse("neo4j:*"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true), NodePattern("movie", labels = listOf("Movie"), all = true)), parse("neo4j:Person;movie :Movie"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true), NodePattern("neo4j", labels = listOf("Movie"), all = true)), parse("neo4j : Person ; neo4j : Movie"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true), NodePattern("movie", labels = listOf("Movie"), all = true)), parse("neo4j:Person{*};movie:Movie{*}"))

        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, include = listOf("name")),
                            NodePattern("neo4j", labels = listOf("Movie"), all = true, include = listOf("title"))),
                parse("neo4j:Person{*,name};neo4j:Movie{*,title}"))

        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = false, include = listOf("name")),
                NodePattern("movie", labels = listOf("Movie"), all = false, include = listOf("title"))),
                parse("neo4j:Person{name} ;movie:Movie{title}"))

        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, include = listOf("name", "age")),
                NodePattern("movie", labels = listOf("Movie"), all = true, include = listOf("title", "released"))),
                parse("neo4j:Person{*,name, age} ; movie:Movie{*,title, released}"))

        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, exclude = listOf("name")),
                NodePattern("movie", labels = listOf("Movie"), all = true, exclude = listOf("title"))),
                parse("neo4j:Person{*,-name}; movie:Movie{*,-title}"))

        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, exclude = listOf("name")),
                NodePattern("movie", labels = listOf("Movie"), all = true, exclude = listOf("title"))),
                parse("neo4j:Person{-name};movie:Movie{-title}"))

        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, exclude = listOf("name", "age")),
                NodePattern("movie", labels = listOf("Movie"), all = true, exclude = listOf("title","released"))),
                parse("neo4j:Person{*,-name, -age};movie:Movie{*,-title, -released}"))
    }

    @Test fun testParseMultipleTopics() {
        fun parse(pattern:String) = NodePattern.parse(pattern, topics = listOf("test","neo4j"))
        assertEquals(listOf(NodePattern("neo4j", labels = emptyList(), all = true)), parse("neo4j:*"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true)), parse("neo4j:Person"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true)), parse("neo4j:Person{*}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, include = listOf("name"))), parse("neo4j:Person{*,name}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = false, include = listOf("name"))), parse("neo4j:Person{name}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, include = listOf("name", "age"))), parse("neo4j:Person{*,name, age}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, exclude = listOf("name"))), parse("neo4j:Person{*,-name}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, exclude = listOf("name"))), parse("neo4j:Person{-name}"))
        assertEquals(listOf(NodePattern("neo4j", labels = listOf("Person"), all = true, exclude = listOf("name", "age"))), parse("neo4j:Person{*,-name, -age}"))
    }

    @Test fun testParseDifferentEntry() {
        fun parse(pattern:String) = NodePattern.parse(pattern, topics = listOf("test"))
        assertEquals(listOf(NodePattern("test", labels = emptyList(), all = true)), parse("neo4j:*"))
        assertEquals(listOf(NodePattern("test", labels = listOf("neo4j","Person"), all = true)), parse("neo4j:Person"))
        assertEquals(listOf(NodePattern("test", labels = listOf("neo4j","Person"), all = true)), parse("neo4j:Person{*}"))
        assertEquals(listOf(NodePattern("test", labels = listOf("neo4j","Person"), all = true, include = listOf("name"))), parse("neo4j:Person{*,name}"))
        assertEquals(listOf(NodePattern("test", labels = listOf("neo4j","Person"), all = false, include = listOf("name"))), parse("neo4j:Person{name}"))
        assertEquals(listOf(NodePattern("test", labels = listOf("neo4j","Person"), all = true, include = listOf("name", "age"))), parse("neo4j:Person{*,name, age}"))
        assertEquals(listOf(NodePattern("test", labels = listOf("neo4j","Person"), all = true, exclude = listOf("name"))), parse("neo4j:Person{*,-name}"))
        assertEquals(listOf(NodePattern("test", labels = listOf("neo4j","Person"), all = true, exclude = listOf("name"))), parse("neo4j:Person{-name}"))
        assertEquals(listOf(NodePattern("test", labels = listOf("neo4j","Person"), all = true, exclude = listOf("name", "age"))), parse("neo4j:Person{*,-name, -age}"))
    }
}

