package integrations.kafka

import extension.newDatabase
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.assertion.Assert
import streams.serialization.JSONUtils
import streams.service.sink.strategy.CUDNode
import streams.service.sink.strategy.CUDNodeRel
import streams.service.sink.strategy.CUDOperations
import streams.service.sink.strategy.CUDRelationship
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.streams.toList
import kotlin.test.assertEquals

class KafkaEventSinkCUDFormat : KafkaEventSinkBase() {
    @Test
    fun `should ingest node data from CUD Events`() {
        // given
        val mergeMarkers = listOf(2, 5, 7)
        val key = "key"
        val list = (1..10).map {
            val labels = if (it % 2 == 0) listOf("Foo", "Bar") else listOf("Foo", "Bar", "Label")
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val (op, ids) = when (it) {
                in mergeMarkers -> CUDOperations.merge to mapOf(key to it)
                else -> CUDOperations.create to emptyMap()
            }
            val cudNode = CUDNode(op = op,
                    labels = labels,
                    ids = ids,
                    properties = properties)
            JSONUtils.writeValueAsBytes(cudNode)
        }
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cud", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI

        // when
        list.forEach {
            var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val fooBar = db.execute("""
                MATCH (n:Foo:Bar)
                RETURN count(n) AS count
            """.trimIndent()).columnAs<Long>("count")
            val fooBarLabel = db.execute("""
                MATCH (n:Foo:Bar:Label)
                RETURN count(n) AS count
            """.trimIndent()).columnAs<Long>("count")
            fooBar.hasNext() && fooBar.next() == 10L && !fooBar.hasNext()
            fooBarLabel.hasNext() && fooBarLabel.next() == 5L && !fooBarLabel.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val nodes = db.beginTx().use {
                db.allNodes.stream().map { it.allProperties }.toList()
            }
            nodes.size == 10 && nodes.all { props ->
                val id = props.getValue("id").toString().toLong()
                val foo = props.getValue("foo").toString()
                foo == "foo-value-$id"
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should update node data from CUD Events with internal id resolution`() {
        // given
        val key = "_id"
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cud", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        val idList = db.beginTx().use {
            db.execute("UNWIND range(1, 10) AS id CREATE (:Foo:Bar {key: id})").close()
            assertEquals(10, db.allNodes.count())
            it.success()
            db.allNodes.stream()
                    .limit(5)
                    .toList()
                    .map { it.id }
        }
        val list = idList.map {
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val cudNode = CUDNode(op = CUDOperations.update,
                    labels = listOf("Foo", "Bar"),
                    ids = mapOf(key to it),
                    properties = properties)
            JSONUtils.writeValueAsBytes(cudNode)
        }

        // when
        list.forEach {
            var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val fooBar = db.execute("""
                MATCH (n:Foo:Bar)
                RETURN count(n) AS count
            """.trimIndent()).columnAs<Long>("count")
            fooBar.hasNext() && fooBar.next() == 10L && !fooBar.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val nodes = db.beginTx().use {
                db.allNodes.stream().map { it.allProperties }.toList()
            }
            val nodesUpdated = nodes.filter { props ->
                when (props.containsKey("id")) {
                    true -> {
                        val id = props.getValue("id").toString().toLong()
                        val foo = props.getValue("foo").toString()
                        foo == "foo-value-$id" && props.containsKey("key")
                    }
                    else -> false
                }
            }
            nodes.size == 10 && nodesUpdated.size == 5
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should delete node data from CUD Events`() {
        // given
        val key = "key"
        val deleteEvents = (1..5).map {
            val labels = listOf("Foo", "Bar")
            val cudNode = CUDNode(op = CUDOperations.delete,
                    labels = labels,
                    ids = mapOf(key to it))
            JSONUtils.writeValueAsBytes(cudNode)
        }
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cud", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        db.beginTx().use {
            db.execute("UNWIND range(1, 10) AS id CREATE (n:Foo:Bar {key: id})").close()
            assertEquals(10, db.allNodes.count())
            it.success()
        }

        // when
        deleteEvents.forEach {
            var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val fooBar = db.execute("""
                MATCH (n:Foo:Bar)
                WHERE n.key > 5
                RETURN n.key AS key
            """.trimIndent()).columnAs<Long>("key").stream().toList()
            fooBar == (6L..10L).toList()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should not delete node data from CUD Events without detach false`() {
        // given
        val key = "key"
        val deleteEvents = (1..5).map {
            val labels = listOf("Foo", "Bar")
            val cudNode = CUDNode(op = CUDOperations.delete,
                    labels = labels,
                    ids = mapOf(key to it),
                    detach = false)
            JSONUtils.writeValueAsBytes(cudNode)
        }
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cud", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        db.beginTx().use {
            db.execute("UNWIND range(1, 5) AS id CREATE (s:Foo:Bar {key: id})-[:MY_REL]->(u:Foo:Bar {key: id + 1})").close()
            assertEquals(10, db.allNodes.count())
            it.success()
        }

        // when
        deleteEvents.forEach {
            var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val fooBar = db.execute("""
                MATCH (n:Foo:Bar)
                RETURN count(n) AS count
            """.trimIndent()).columnAs<Long>("count")
            fooBar.hasNext() && fooBar.next() == 10L && !fooBar.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should ingest relationship data from CUD Events`() {
        // given
        val key = "key"
        val rel_type = "MY_REL"
        val list = (1..10).map {
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val start = CUDNodeRel(ids = mapOf(key to it), labels = listOf("Foo", "Bar"))
            val end = CUDNodeRel(ids = mapOf(key to it), labels = listOf("FooBar"))
            val rel = CUDRelationship(op = CUDOperations.create, properties = properties, from = start, to = end, rel_type = rel_type)
            JSONUtils.writeValueAsBytes(rel)
        }
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cud", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        db.beginTx().use {
            db.execute("""
                UNWIND range(1, 10) AS id
                CREATE (:Foo:Bar {key: id})
                CREATE (:FooBar {key: id})
            """.trimIndent()).close()
            assertEquals(20, db.allNodes.count())
            it.success()
        }

        // when
        list.forEach {
            var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val fooBar = db.execute("""
                MATCH p = (:Foo:Bar)-[:$rel_type]->(:FooBar)
                RETURN count(p) AS count
            """.trimIndent()).columnAs<Long>("count")
            fooBar.hasNext() && fooBar.next() == 10L && !fooBar.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val rels = db.beginTx().use {
                db.allRelationships.stream().map { it.allProperties }.toList()
            }
            rels.size == 10 && rels.all { props ->
                val id = props.getValue("id").toString().toLong()
                val foo = props.getValue("foo").toString()
                foo == "foo-value-$id"
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should delete relationship data from CUD Events`() {
        // given
        val key = "key"
        val rel_type = "MY_REL"
        val list = (1..5).map {
            val properties = emptyMap<String, Any>()
            val start = CUDNodeRel(ids = mapOf(key to it), labels = emptyList())
            val end = CUDNodeRel(ids = mapOf(key to it), labels = emptyList())
            val rel = CUDRelationship(op = CUDOperations.delete, properties = properties, from = start, to = end, rel_type = rel_type)
            JSONUtils.writeValueAsBytes(rel)
        }
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cud", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        db.beginTx().use {
            db.execute("UNWIND range(1, 10) AS id CREATE (:Foo:Bar {key: id})-[:$rel_type{id: id}]->(:FooBar{key: id})").close()
            assertEquals(10, db.allRelationships.count())
            it.success()
        }

        // when
        list.forEach {
            var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val fooBar = db.execute("""
                MATCH (:Foo:Bar)-[r:$rel_type]->(:FooBar)
                RETURN count(r) AS count
            """.trimIndent()).columnAs<Long>("count")
            fooBar.hasNext() && fooBar.next() == 5L && !fooBar.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val ids = db.execute("""
                        MATCH (:Foo:Bar)-[r:$rel_type]->(:FooBar)
                        RETURN r.id AS id
                    """.trimIndent())
                    .columnAs<Long>("id")
                    .stream()
                    .toList()
            ids == (6L..10L).toList()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should update relationship data from CUD Events with internal id resolution`() {
        // given
        val key = "_id"
        val rel_type = "MY_REL"
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cud", topic)
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        val idMap = db.beginTx().use {
            db.execute("UNWIND range(1, 10) AS id CREATE (:Foo:Bar {key: id})-[:$rel_type{id: id}]->(:FooBar{key: id})").close()
            assertEquals(10, db.allRelationships.count())
            it.success()
            db.allRelationships.stream()
                    .limit(5)
                    .toList()
                    .map { it.startNodeId to it.endNodeId }
                    .toMap()
        }
        val list = idMap.map {
            val properties = emptyMap<String, Any>()
            val start = CUDNodeRel(ids = mapOf(key to it.key), labels = emptyList())
            val end = CUDNodeRel(ids = mapOf(key to it.value), labels = emptyList())
            val rel = CUDRelationship(op = CUDOperations.delete, properties = properties, from = start, to = end, rel_type = rel_type)
            JSONUtils.writeValueAsBytes(rel)
        }

        // when
        list.forEach {
            var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val fooBar = db.execute("""
                MATCH (:Foo:Bar)-[r:$rel_type]->(:FooBar)
                RETURN count(r) AS count
            """.trimIndent()).columnAs<Long>("count")
            fooBar.hasNext() && fooBar.next() == 5L && !fooBar.hasNext()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val ids = db.execute("""
                        MATCH (:Foo:Bar)-[r:$rel_type]->(:FooBar)
                        RETURN r.id AS id
                    """.trimIndent())
                    .columnAs<Long>("id")
                    .stream()
                    .toList()
            ids == (6L..10L).toList()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }
}