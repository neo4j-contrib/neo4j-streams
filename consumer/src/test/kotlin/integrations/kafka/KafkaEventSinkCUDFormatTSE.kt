package integrations.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import streams.Assert
import streams.extensions.execute
import streams.utils.JSONUtils
import streams.service.sink.strategy.CUDNode
import streams.service.sink.strategy.CUDNodeRel
import streams.service.sink.strategy.CUDOperations
import streams.service.sink.strategy.CUDRelationship
import streams.setConfig
import streams.start
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.streams.toList
import kotlin.test.assertEquals

class KafkaEventSinkCUDFormatTSE : KafkaEventSinkBaseTSE() {
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
        db.setConfig("streams.sink.topic.cud", topic)
        db.start()

        // when
        list.forEach {
            var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val hasFooBar = db.execute("""
                MATCH (n:Foo:Bar)
                RETURN count(n) AS count
            """.trimIndent()) {
                val fooBar = it.columnAs<Long>("count")
                fooBar.hasNext() && fooBar.next() == 10L && !fooBar.hasNext()
            }
            val hasFooBarLabel = db.execute("""
                MATCH (n:Foo:Bar:Label)
                RETURN count(n) AS count
            """.trimIndent()) {
                val fooBarLabel = it.columnAs<Long>("count")
                fooBarLabel.hasNext() && fooBarLabel.next() == 5L && !fooBarLabel.hasNext()
            }
            hasFooBar && hasFooBarLabel
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val nodes = db.beginTx().use {
                it.allNodes.stream().map { it.allProperties }.toList()
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
        db.setConfig("streams.sink.topic.cud", topic)
        db.start()
        val idList = db.beginTx().use {
            db.execute("UNWIND range(1, 10) AS id CREATE (:Foo:Bar {key: id})")
            assertEquals(10, it.allNodes.count())
            val ret = it.allNodes.stream()
                    .limit(5)
                    .toList()
                    .map { it.id }
            it.commit()
            ret
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
            db.execute("""
                MATCH (n:Foo:Bar)
                RETURN count(n) AS count
            """.trimIndent()) {
                val fooBar = it.columnAs<Long>("count")
                fooBar.hasNext() && fooBar.next() == 10L && !fooBar.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val nodes = db.beginTx().use {
                it.allNodes.stream().map { it.allProperties }.toList()
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
        db.setConfig("streams.sink.topic.cud", topic)
        db.start()
        db.beginTx().use {
            db.execute("UNWIND range(1, 10) AS id CREATE (n:Foo:Bar {key: id})")
            assertEquals(10, it.allNodes.count())
            it.commit()
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
            """.trimIndent()) { it.columnAs<Long>("key").stream().toList() }
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
        db.setConfig("streams.sink.topic.cud", topic)
        db.start()
        db.beginTx().use {
            db.execute("UNWIND range(1, 5) AS id CREATE (s:Foo:Bar {key: id})-[:MY_REL]->(u:Foo:Bar {key: id + 1})")
            assertEquals(10, it.allNodes.count())
            it.commit()
        }

        // when
        deleteEvents.forEach {
            var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            db.execute("""
                MATCH (n:Foo:Bar)
                RETURN count(n) AS count
            """.trimIndent()) {
                val fooBar = it.columnAs<Long>("count")
                fooBar.hasNext() && fooBar.next() == 10L && !fooBar.hasNext()
            }
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
        db.setConfig("streams.sink.topic.cud", topic)
        db.start()
        db.beginTx().use {
            db.execute("""
                UNWIND range(1, 10) AS id
                CREATE (:Foo:Bar {key: id})
                CREATE (:FooBar {key: id})
            """.trimIndent())
            assertEquals(20, it.allNodes.count())
            it.commit()
        }

        // when
        list.forEach {
            var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            db.execute("""
                MATCH p = (:Foo:Bar)-[:$rel_type]->(:FooBar)
                RETURN count(p) AS count
            """.trimIndent()) {
                val fooBar = it.columnAs<Long>("count")
                fooBar.hasNext() && fooBar.next() == 10L && !fooBar.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val rels = db.beginTx().use {
                it.allRelationships.stream().map { it.allProperties }.toList()
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
        db.setConfig("streams.sink.topic.cud", topic)
        db.start()
        db.beginTx().use {
            db.execute("UNWIND range(1, 10) AS id CREATE (:Foo:Bar {key: id})-[:$rel_type{id: id}]->(:FooBar{key: id})")
            assertEquals(10, it.allRelationships.count())
            it.commit()
        }

        // when
        list.forEach {
            val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            db.execute("""
                MATCH (:Foo:Bar)-[r:$rel_type]->(:FooBar)
                RETURN count(r) AS count
            """.trimIndent()) {
                val fooBar = it.columnAs<Long>("count")
                fooBar.hasNext() && fooBar.next() == 5L && !fooBar.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val ids = db.execute("""
                        MATCH (:Foo:Bar)-[r:$rel_type]->(:FooBar)
                        RETURN r.id AS id
                    """.trimIndent()) {
                it.columnAs<Long>("id")
                        .stream()
                        .toList()
            }
            ids == (6L..10L).toList()
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should update relationship data from CUD Events with internal id resolution`() {
        // given
        val key = "_id"
        val rel_type = "MY_REL"
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.cud", topic)
        db.start()
        val idMap = db.beginTx().use { tx ->
            tx.execute("UNWIND range(1, 10) AS id CREATE (:Foo:Bar {key: id})-[:$rel_type{id: id}]->(:FooBar{key: id})")
            assertEquals(10, tx.allRelationships.count())
            val res = tx.allRelationships.stream()
                    .toList()
                    .sortedBy { it.id }
                    .subList(0, 5)
                    .map { it.startNodeId to it.endNodeId }
                    .toMap()
            tx.commit()
            res
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
            db.execute("""
                MATCH (:Foo:Bar)-[r:$rel_type]->(:FooBar)
                RETURN count(r) AS count
            """.trimIndent()) {
                val fooBar = it.columnAs<Long>("count")
                fooBar.hasNext() && fooBar.next() == 5L && !fooBar.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
        val ids = db.execute("""
                        MATCH (:Foo:Bar)-[r:$rel_type]->(:FooBar)
                        RETURN r.id AS id
                    """.trimIndent()) {
            it.columnAs<Long>("id")
                    .stream()
                    .toList()
        }
        assertEquals(ids, (6L..10L).toList())
    }

    @Test
    fun `should create nodes and relationship, if one or both nodes doesn't exist from CUD Events`() {
        // given
        val key = "key"
        val topic = UUID.randomUUID().toString()
        val relType = "MY_REL"
        val list = (1..10).map {
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val start = CUDNodeRel(ids = mapOf(key to it), labels = listOf("Foo", "Bar"), op = CUDOperations.merge)
            val end = CUDNodeRel(ids = mapOf(key to it), labels = listOf("FooBar"), op = CUDOperations.merge)
            val rel = CUDRelationship(op = CUDOperations.create, properties = properties, from = start, to = end, rel_type = relType)
            JSONUtils.writeValueAsBytes(rel)
        }
        db.setConfig("streams.sink.topic.cud", topic)
        db.start()

        // when
        list.forEach {
            val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            db.execute("""
                MATCH p = (:Foo:Bar)-[:$relType]->(:FooBar)
                RETURN count(p) AS count
            """.trimIndent()) {
                val fooBar = it.columnAs<Long>("count")
                fooBar.hasNext() && fooBar.next() == 10L && !fooBar.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

        Assert.assertEventually(ThrowingSupplier {
            val rels = db.beginTx().use {
                it.allRelationships.stream().map { it.allProperties }.toList()
            }
            rels.size == 10 && rels.all { props ->
                val id = props.getValue("id").toString().toLong()
                val foo = props.getValue("foo").toString()
                foo == "foo-value-$id"
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

        // now, I create only start nodes
        val listWithStartPresent = (11..20).map {
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val start = CUDNodeRel(ids = mapOf(key to it), labels = listOf("Foo", "Bar"))
            val end = CUDNodeRel(ids = mapOf(key to it), labels = listOf("FooBar"), op = CUDOperations.create)
            val rel = CUDRelationship(op = CUDOperations.create, properties = properties, from = start, to = end, rel_type = relType)
            JSONUtils.writeValueAsBytes(rel)
        }

        db.beginTx().use {
            db.execute("""
                UNWIND range(11, 20) AS id
                CREATE (:Foo:Bar {key: id})
            """.trimIndent())
            assertEquals(30, it.allNodes.count())
            it.commit()
        }

        // when
        listWithStartPresent.forEach {
            val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier {
            db.execute("""
                MATCH p = (:Foo:Bar)-[:$relType]->(:FooBar)
                RETURN count(p) AS count
            """.trimIndent()) {
                val fooBar = it.columnAs<Long>("count")
                fooBar.hasNext() && fooBar.next() == 20L && !fooBar.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

        Assert.assertEventually(ThrowingSupplier {
            val rels = db.beginTx().use {
                it.allRelationships.stream().map { it.allProperties }.toList()
            }
            rels.size == 20 && rels.all { props ->
                val id = props.getValue("id").toString().toLong()
                val foo = props.getValue("foo").toString()
                foo == "foo-value-$id"
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

        // now, I create only end nodes
        val listWithEndPresent = (21..30).map {
            val properties = mapOf("foo" to "foo-value-$it", "id" to it)
            val start = CUDNodeRel(ids = mapOf(key to it), labels = listOf("Foo", "Bar"), op = CUDOperations.create)
            val end = CUDNodeRel(ids = mapOf(key to it), labels = listOf("FooBar"))
            val rel = CUDRelationship(op = CUDOperations.create, properties = properties, from = start, to = end, rel_type = relType)
            JSONUtils.writeValueAsBytes(rel)
        }

        db.beginTx().use {
            db.execute("""
                UNWIND range(21, 30) AS id
                CREATE (:FooBar {key: id})
            """.trimIndent())
            assertEquals(50, it.allNodes.count())
            it.commit()
        }

        // when
        listWithEndPresent.forEach {
            val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), it)
            kafkaProducer.send(producerRecord).get()
        }

        // then
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            db.execute("""
                MATCH p = (:Foo:Bar)-[:$relType]->(:FooBar)
                RETURN count(p) AS count
            """.trimIndent()) {
                val fooBar = it.columnAs<Long>("count")
                fooBar.hasNext() && fooBar.next() == 30L && !fooBar.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

        Assert.assertEventually(ThrowingSupplier {
            val rels = db.beginTx().use {
                it.allRelationships.stream().map { it.allProperties }.toList()
            }
            rels.size == 30 && rels.all { props ->
                val id = props.getValue("id").toString().toLong()
                val foo = props.getValue("foo").toString()
                foo == "foo-value-$id"
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

    }


}