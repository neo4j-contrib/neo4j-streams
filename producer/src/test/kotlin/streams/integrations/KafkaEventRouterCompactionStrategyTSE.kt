package streams.integrations

import org.apache.kafka.common.config.TopicConfig
import org.junit.Test
import org.neo4j.internal.helpers.collection.Iterators
import streams.extensions.execute
import streams.events.*
import streams.integrations.CompactionStrategyTestCommon.assertTopicFilled
import streams.utils.JSONUtils
import streams.setConfig
import streams.start
import java.time.Duration
import java.util.*
import kotlin.test.*

class KafkaEventRouterCompactionStrategyTSE : KafkaEventRouterBaseTSE() {

    private val bootstrapServerMap = mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)

    private fun createProducerRecordKeyForDeleteStrategy(meta: Meta) = "${meta.txId + meta.txEventId}-${meta.txEventId}"

    private fun initDbWithLogStrategy(strategy: String, otherConfigs: Map<String, String>? = null, constraints: List<String>? = null) {

        db.setConfig("streams.source.schema.polling.interval", "0")
                .setConfig("kafka.streams.log.compaction.strategy", strategy)

        otherConfigs?.forEach { (k, v) -> db.setConfig(k, v) }
        db.start()
        constraints?.forEach { db.execute(it) }
    }

    private fun createManyPersons() = db.execute("UNWIND range(1, 999) AS id CREATE (:Person {name:id})")

    @Test
    fun `compact message with streams publish`() {
        val topic = UUID.randomUUID().toString()
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT)
        CompactionStrategyTestCommon.createCompactTopic(topic, bootstrapServerMap)

        KafkaEventRouterSuiteIT.registerPublishProcedure(db)
        kafkaConsumer.subscribe(listOf(topic))

        val keyRecord = "test"
        // we sent 5 messages, 3 of them with the same key, so we expect that
        // with the log compaction activated we expect to have just one message with the key equal to 'test'
        db.execute("CALL streams.publish('$topic', 'Compaction 0', {key: 'Baz'})")
        db.execute("CALL streams.publish('$topic', 'Compaction 1', {key: '$keyRecord'})")
        db.execute("CALL streams.publish('$topic', 'Compaction 2', {key: '$keyRecord'})")
        db.execute("CALL streams.publish('$topic', 'Compaction 3', {key: 'Foo'})")
        db.execute("CALL streams.publish('$topic', 'Compaction 4', {key: '$keyRecord'})")

        // to activate the log compaction process we publish dummy messages
        // and waiting for messages population in topic
        db.execute("UNWIND range(1,999) as id CALL streams.publish('$topic', id, {key: id}) RETURN null") {
            assertEquals(999, Iterators.count(it))
        }

        // check if there is only one record with key 'test' and payload 'Compaction 4'
        assertTopicFilled(kafkaConsumer, true) {
            val compactedRecord = it.filter { JSONUtils.readValue<String>(it.key()) == keyRecord }
            it.count() == 500 &&
                    compactedRecord.count() == 1 &&
                    JSONUtils.readValue<Map<*,*>>(compactedRecord.first().value())["payload"] == "Compaction 4"
        }
    }

    @Test
    fun `delete single tombstone relation with strategy compact and constraints`() {
        // we create a topic with strategy compact
        val keyRel = "KNOWS"
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}",
                "streams.source.topic.relationships.$topic" to "$keyRel{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        CompactionStrategyTestCommon.createCompactTopic(topic, bootstrapServerMap)

        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Pippo'})")
        db.execute("CREATE (:Person {name:'Pluto'})")
        db.execute("""
            |MATCH (pippo:Person {name:'Pippo'})
            |MATCH (pluto:Person {name:'Pluto'})
            |MERGE (pippo)-[:$keyRel]->(pluto)
        """.trimMargin())
        db.execute("MATCH (:Person {name:'Pippo'})-[rel:$keyRel]->(:Person {name:'Pluto'}) DELETE rel")

        // to activate the log compaction process we create dummy messages and waiting for messages population
        createManyPersons()
        assertTopicFilled(kafkaConsumer, true) {
            val nullRecords = it.filter { it.value() == null }
            val start = mapOf("ids" to mapOf("name" to "Pippo"), "labels" to listOf("Person"))
            val end = mapOf("ids" to mapOf("name" to "Pluto"), "labels" to listOf("Person"))
            it.count() == 500
                    && nullRecords.count() == 1
                    && JSONUtils.readValue<Map<*,*>>(nullRecords.first().key()) == mapOf("start" to start, "end" to end, "label" to keyRel)
        }
    }

    @Test
    fun `delete single tombstone relation with strategy compact`() {
        // we create a topic with strategy compact
        val relType = "KNOWS"
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}",
                "streams.source.topic.relationships.$topic" to "$relType{*}")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics)
        CompactionStrategyTestCommon.createCompactTopic(topic, bootstrapServerMap)

        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Pippo'})")
        db.execute("CREATE (:Person {name:'Pluto'})")
        // we create a relation, so will be created a record with payload not null
        db.execute("""
            |MATCH (pippo:Person {name:'Pippo'})
            |MATCH (pluto:Person {name:'Pluto'})
            |MERGE (pippo)-[:KNOWS]->(pluto)
        """.trimMargin())

        // we delete the relation, so will be created a tombstone record
        db.execute("MATCH (:Person {name:'Pippo'})-[rel:KNOWS]->(:Person {name:'Pluto'}) DELETE rel")
        db.execute("CREATE (:Person {name:'Paperino'})")

        createManyPersons()

        // we check that there is only one tombstone record
        assertTopicFilled(kafkaConsumer, true) {
            val nullRecords = it.filter { it.value() == null }
            it.count() == 500
                    && nullRecords.count() == 1
                    && JSONUtils.readValue<Map<*,*>>(nullRecords.first().key()) == mapOf("start" to "0", "end" to "1", "label" to relType)
        }
    }

    @Test
    fun `delete tombstone node with strategy compact`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics)
        CompactionStrategyTestCommon.createCompactTopic(topic, bootstrapServerMap)

        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Watson'})")
        db.execute("CREATE (:Person {name:'Sherlock'})")
        db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.address = '221B Baker Street'")

        // we delete a node, so will be created a tombstone record
        db.execute("MATCH (p:Person {name:'Sherlock'}) DETACH DELETE p")

        createManyPersons()
        assertTopicFilled(kafkaConsumer, true) {
            val nullRecords = it.filter { it.value() == null }
            it.count() == 500
                    && nullRecords.count() == 1
                    && JSONUtils.readValue<String>(nullRecords.first().key()) == "1"
        }
    }

    @Test
    fun `delete node tombstone with strategy compact and constraint`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        CompactionStrategyTestCommon.createCompactTopic(topic, bootstrapServerMap)

        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Watson'})")
        db.execute("CREATE (:Person {name:'Sherlock'})")
        db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.address = '221B Baker Street'")

        // we delete a node, so will be created a tombstone record
        db.execute("MATCH (p:Person {name:'Sherlock'}) DETACH DELETE p")

        createManyPersons()
        assertTopicFilled(kafkaConsumer, true) {
            val nullRecords = it.filter { it.value() == null }
            val keyRecordExpected = mapOf("ids" to mapOf("name" to "Sherlock"), "labels" to listOf("Person"))
            it.count() == 500
                    && nullRecords.count() == 1
                    && keyRecordExpected == JSONUtils.readValue<Map<*,*>>(nullRecords.first().key())
        }
    }

    @Test
    fun `test relationship with multiple constraint and strategy compact`() {
        val relType = "BUYS"
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                "streams.source.topic.relationships.${topic[1]}" to "$relType{*}",
                "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
        )
        val queries = listOf("CREATE CONSTRAINT ON (p:Product) ASSERT p.code IS UNIQUE",
                "CREATE CONSTRAINT ON (p:Other) ASSERT p.address IS UNIQUE",
                "CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE"
        )
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        kafkaConsumer.subscribe(topic)

        db.execute("CREATE (:Person:Other {name: 'Sherlock', surname: 'Holmes', address: 'Baker Street'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        var keyStart = mapOf("ids" to mapOf("address" to "Baker Street"), "labels" to listOf("Other", "Person"))
        assertEquals(keyStart, JSONUtils.readValue(records.first().key()))

        db.execute("CREATE (p:Product {code:'1367', name: 'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        val keyEnd = mapOf("ids" to mapOf("code" to "1367"), "labels" to listOf("Product"))
        assertEquals(keyEnd, JSONUtils.readValue(recordsTwo.first().key()))

        // we create a relationship with start and end node with constraint
        db.execute("MATCH (pe:Person:Other {name:'Sherlock'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        val mapRel: Map<String, Any> = JSONUtils.readValue(recordsThree.first().key())

        keyStart = mapOf("ids" to mapOf("name" to "Sherlock"), "labels" to listOf("Other", "Person"))
        assertEquals(keyStart, mapRel["start"])
        assertEquals(keyEnd, mapRel["end"])
        assertEquals(relType, mapRel["label"])

        // we update the relationship
        db.execute("MATCH (:Person:Other {name:'Sherlock'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFour.count())
        assertEquals(mapRel, JSONUtils.readValue(recordsThree.first().key()))

        // we delete the relationship
        db.execute("MATCH (:Person:Other {name:'Sherlock'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFive.count())
        assertEquals(mapRel, JSONUtils.readValue(recordsFive.first().key()))
        assertNull(recordsFive.first().value())
    }

    @Test
    fun `test label with multiple constraints and strategy compact`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person:Neo4j{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE",
                "CREATE CONSTRAINT ON (p:Neo4j) ASSERT p.surname IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person:Neo4j {name:'Sherlock', surname: 'Holmes', address: 'Baker Street'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        var keyNode = mapOf("ids" to mapOf("name" to "Sherlock"), "labels" to listOf("Person", "Neo4j"))
        assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(records.first().key()))

        db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.name='Foo'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        keyNode = mapOf("ids" to mapOf("name" to "Foo"), "labels" to listOf("Person", "Neo4j"))
        assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(recordsTwo.first().key()))

        db.execute("MATCH (p:Person {name:'Foo'}) SET p.surname='Bar'")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(recordsThree.first().key()))

        db.execute("MATCH (p:Person {name:'Foo'}) DETACH DELETE p")
        val recordsFour = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFour.count())
        assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(recordsFour.first().key()))
        assertNull(recordsFour.first().value())
    }

    @Test
    fun `node without constraint and topic compact`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics)
        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())

        var idPayload = (JSONUtils.readValue<Map<*, *>>(records.first().value())["payload"] as Map<*, *>)["id"]
        assertEquals(idPayload, JSONUtils.readValue(records.first().key()))

        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.surname='Pluto'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        idPayload = (JSONUtils.readValue<Map<*, *>>(recordsTwo.first().value())["payload"] as Map<*, *>)["id"]
        assertEquals(idPayload, JSONUtils.readValue(recordsTwo.first().key()))

        db.execute("MATCH (p:Person {name:'Pippo'}) DETACH DELETE p")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertEquals(idPayload, JSONUtils.readValue(recordsThree.first().key()))
        assertNull(recordsThree.first().value())
    }

    @Test
    fun `node with constraint and topic compact`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        kafkaConsumer.subscribe(listOf(topic))

        // we create a node with constraint and check that key is equal to constraint
        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        var keyNode = mapOf("ids" to mapOf("name" to "Pippo"), "labels" to listOf("Person"))
        assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(records.first().key()))

        // we update the node
        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.name='Pluto'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        keyNode = mapOf("ids" to mapOf("name" to "Pluto"), "labels" to listOf("Person"))
        assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(recordsTwo.first().key()))

        // we delete the node
        db.execute("MATCH (p:Person {name:'Pluto'}) DETACH DELETE p")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(recordsThree.first().key()))
        assertNull(recordsThree.first().value())
    }

    @Test
    fun `relation with nodes without constraint and topic compact`() {
        val relType = "BUYS"
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                "streams.source.topic.relationships.${topic[1]}" to "$relType{*}",
                "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
        )
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, )
        kafkaConsumer.subscribe(topic)

        // we create a node without constraint and check that key is equal to id's payload
        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        val idPayloadStart = (JSONUtils.readValue<Map<*, *>>(records.first().value())["payload"] as Map<*, *>)["id"]
        assertEquals(idPayloadStart, JSONUtils.readValue(records.first().key()))

        db.execute("CREATE (p:Product {name:'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        val idPayloadEnd = (JSONUtils.readValue<Map<*, *>>(recordsTwo.first().value())["payload"] as Map<*, *>)["id"]
        assertEquals(idPayloadEnd, JSONUtils.readValue(recordsTwo.first().key()))

        // we create a relation with start and end node without constraint
        db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        var keyRel = recordsThree.first().key()
        assertEquals(relType,  JSONUtils.readValue<Map<*,*>>(keyRel)["label"])
        assertEquals(idPayloadStart,  JSONUtils.readValue<Map<*,*>>(keyRel)["start"])
        assertEquals(idPayloadEnd,  JSONUtils.readValue<Map<*,*>>(keyRel)["end"])

        // we update the relation
        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFour.count())
        keyRel = recordsThree.first().key()
        assertEquals(relType,  JSONUtils.readValue<Map<*,*>>(keyRel)["label"])
        assertEquals(idPayloadStart,  JSONUtils.readValue<Map<*,*>>(keyRel)["start"])
        assertEquals(idPayloadEnd,  JSONUtils.readValue<Map<*,*>>(keyRel)["end"])

        // we delete the relation and check if payload is null
        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFive.count())
        keyRel = recordsThree.first().key()
        assertEquals(relType,  JSONUtils.readValue<Map<*,*>>(keyRel)["label"])
        assertEquals(idPayloadStart,  JSONUtils.readValue<Map<*,*>>(keyRel)["start"])
        assertEquals(idPayloadEnd,  JSONUtils.readValue<Map<*,*>>(keyRel)["end"])
        assertNull(recordsFive.first().value())
    }

    @Test
    fun `relation with nodes with constraint and strategy compact`() {
        val labelRel = "BUYS"
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                "streams.source.topic.relationships.${topic[1]}" to "$labelRel{*}",
                "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
        )
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE",
                "CREATE CONSTRAINT ON (p:Product) ASSERT p.code IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        kafkaConsumer.subscribe(topic)

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        val propsAfterStartNode = JSONUtils.asStreamsTransactionEvent(records.first().value()).payload.after?.properties
        val keyStartNodeActual = JSONUtils.readValue<Map<String, Any>>(records.first().key())
        val keyStartExpected = mapOf("ids" to mapOf("name" to "Pippo"), "labels" to listOf("Person"))
        assertEquals(keyStartExpected, keyStartNodeActual)
        assertEquals(mapOf("name" to "Pippo"), propsAfterStartNode)

        db.execute("CREATE (p:Product {code:'1367', name: 'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        val propsAfterEndNode =  JSONUtils.asStreamsTransactionEvent(recordsTwo.first().value()).payload.after?.properties
        val keyEndNodeActual = JSONUtils.readValue<Map<String, Any>>(recordsTwo.first().key())
        val keyEndExpected = mapOf("ids" to mapOf("code" to "1367"), "labels" to listOf("Product"))
        assertEquals(keyEndExpected, keyEndNodeActual)
        assertEquals(mapOf("code" to "1367", "name" to "Notebook"), propsAfterEndNode)

        db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:$labelRel]->(pr)")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertEquals(mapOf("start" to keyStartExpected,
                "end" to keyEndExpected,
                "label" to labelRel),
                JSONUtils.readValue(recordsThree.first().key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:$labelRel]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFour.count())
        assertEquals(mapOf("start" to keyStartExpected,
                "end" to keyEndExpected,
                "label" to labelRel),
                JSONUtils.readValue(recordsFour.first().key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:$labelRel]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFive.count())
        assertEquals(mapOf("start" to keyStartExpected,
                "end" to keyEndExpected,
                "label" to labelRel),
                JSONUtils.readValue(recordsFive.first().key()))
        assertNull(recordsFive.first().value())
    }

    // we verify that with the default strategy everything works as before
    @Test
    fun `node without constraint and strategy delete`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_DELETE, sourceTopics, )
        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        var record = records.first()
        var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.surname='Pluto'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        record = recordsTwo.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (p:Person {name:'Pippo'}) DETACH DELETE p")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        record = recordsThree.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))
        assertNotNull(record.value())
    }

    @Test
    fun `node with constraint and strategy delete`() {
        val topic = UUID.randomUUID().toString()

        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_DELETE, sourceTopics, queries)
        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        var record = records.first()
        var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.name='Pluto'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        record = records.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (p:Person {name:'Pluto'}) DETACH DELETE p")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        record = recordsThree.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))
        assertNotNull(record.value())
    }

    @Test
    fun `relation with nodes without constraint and strategy delete`() {
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
                "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
        )
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_DELETE, sourceTopics)
        kafkaConsumer.subscribe(topic)

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        var record = records.first()
        var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("CREATE (p:Product {name:'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        record = recordsTwo.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        record = recordsThree.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFour.count())
        record = recordsFour.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFive.count())
        record = recordsFive.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))
        assertNotNull(record.value())
    }

    @Test
    fun `relation with nodes with constraint and strategy delete`() {
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
                "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
        )
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE",
                "CREATE CONSTRAINT ON (p:Product) ASSERT p.code IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_DELETE, sourceTopics, queries)
        kafkaConsumer.subscribe(topic)

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        var record = records.first()
        var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("CREATE (p:Product {code:'1367', name: 'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        record = recordsTwo.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        record = recordsThree.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFour.count())
        record = recordsFour.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFive.count())
        record = recordsFive.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))
        assertNotNull(record.value())
    }

    @Test
    fun `invalid log strategy should switch to default strategy value`() {
        val topic = UUID.randomUUID().toString()
        // we create an invalid log strategy
        initDbWithLogStrategy("invalid", mapOf("streams.source.topic.nodes.$topic" to "Person{*}"))
        kafkaConsumer.subscribe(listOf(topic))

        // we verify that log strategy is default
        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        val record = records.first()
        val meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))
    }

    @Test
    fun `same nodes and entities in same partitions with strategy compact and without constraints`() {

        createTopicAndEntitiesAndAssertPartition(false,
                "0", "1", "3", "4")
    }

    @Test
    fun `same nodes and entities in same partitions with strategy compact and with constraints`() {

        val personLabel = "Person"
        val otherLabel = "Other"

        val expectedKeyPippo = mapOf("ids" to mapOf("name" to "Pippo"), "labels" to listOf(personLabel))
        val expectedKeyPluto = mapOf("ids" to mapOf("name" to "Pluto"), "labels" to listOf(personLabel, otherLabel))
        val expectedKeyFoo = mapOf("ids" to mapOf("name" to "Foo"), "labels" to listOf(personLabel))
        val expectedKeyBar = mapOf("ids" to mapOf("name" to "Bar"), "labels" to listOf(personLabel, otherLabel))

        createTopicAndEntitiesAndAssertPartition(true, expectedKeyPippo, expectedKeyPluto, expectedKeyFoo, expectedKeyBar)
    }

    private fun createTopicAndEntitiesAndAssertPartition(withConstraints: Boolean,
                                                         firstExpectedKey: Any,
                                                         secondExpectedKey: Any,
                                                         thirdExpectedKey: Any,
                                                         fourthExpectedKey: Any) {
        val relType = "KNOWS"

        // we create a topic with strategy compact
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}",
                "streams.source.topic.relationships.$topic" to "$relType{*}",
                "kafka.num.partitions" to "10" )
        val queries = if(withConstraints) listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE") else null

        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        CompactionStrategyTestCommon.createCompactTopic(topic, bootstrapServerMap, 10, false)

        kafkaConsumer.subscribe(listOf(topic))

        // we create some nodes and rels
        db.execute("CREATE (:Person {name:'Pippo', surname: 'Pippo_1'})")
        db.execute("CREATE (:Person:Other {name:'Pluto', surname: 'Pluto_1'})")
        db.execute("CREATE (:Person:Other {name:'Paperino', surname: 'Paperino_1'})")
        db.execute("CREATE (:Person {name:'Foo'})")
        db.execute("CREATE (:Person:Other {name:'Bar'})")
        db.execute("CREATE (:Person {name:'Baz'})")
        db.execute("CREATE (:Person {name:'Baa'})")
        db.execute("CREATE (:Person {name:'One'})")
        db.execute("CREATE (:Person {name:'Two'})")
        db.execute("CREATE (:Person {name:'Three'})")
        db.execute("""
                |MATCH (pippo:Person {name:'Pippo'})
                |MATCH (pluto:Person {name:'Pluto'})
                |MERGE (pippo)-[:KNOWS]->(pluto)
            """.trimMargin())
        db.execute("""
                |MATCH (foo:Person {name:'Foo'})
                |MATCH (bar:Person {name:'Bar'})
                |MERGE (foo)-[:KNOWS]->(bar)
            """.trimMargin())
        db.execute("""
                |MATCH (one:Person {name:'One'})
                |MATCH (two:Person {name:'Two'})
                |MERGE (one)-[:KNOWS]->(two)
            """.trimMargin())

        // we update 4 nodes and 2 rels
        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.surname = 'Pippo_update'")
        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.address = 'Rome'")

        db.execute("MATCH (p:Person {name:'Pluto'}) SET p.surname = 'Pluto_update'")
        db.execute("MATCH (p:Person {name:'Pluto'}) SET p.address = 'London'")

        db.execute("MATCH (p:Person {name:'Foo'}) SET p.surname = 'Foo_update'")
        db.execute("MATCH (p:Person {name:'Foo'}) SET p.address = 'Rome'")

        db.execute("MATCH (p:Person {name:'Bar'}) SET p.surname = 'Bar_update'")
        db.execute("MATCH (p:Person {name:'Bar'}) SET p.address = 'Tokyo'")

        db.execute("MATCH (:Person {name:'Foo'})-[rel:KNOWS]->(:Person {name:'Bar'}) SET rel.since = 1999")
        db.execute("MATCH (:Person {name:'Pippo'})-[rel:KNOWS]->(:Person {name:'Pluto'}) SET rel.since = 2019")

        val records = kafkaConsumer.poll(Duration.ofSeconds(30))

        assertEquals(23, records.count())

        val firstRecordNode = records.filter { JSONUtils.readValue<Any>(it.key()) == firstExpectedKey }
        val secondRecordNode = records.filter { JSONUtils.readValue<Any>(it.key()) ==  secondExpectedKey }
        val thirdRecordNode = records.filter { JSONUtils.readValue<Any>(it.key()) == thirdExpectedKey }
        val fourthRecordNode = records.filter { JSONUtils.readValue<Any>(it.key()) == fourthExpectedKey }
        val firstRecordRel = records.filter { JSONUtils.readValue<Any>(it.key()) == mapOf("start" to thirdExpectedKey, "end" to fourthExpectedKey, "label" to relType) }
        val secondRecordRel = records.filter { JSONUtils.readValue<Any>(it.key()) == mapOf("start" to firstExpectedKey, "end" to secondExpectedKey, "label" to relType) }

        assertEquals(3, firstRecordNode.count())
        assertEquals(3, secondRecordNode.count())
        assertEquals(3, thirdRecordNode.count())
        assertEquals(3, fourthRecordNode.count())
        assertEquals(2, firstRecordRel.count())
        assertEquals(2, secondRecordRel.count())

        assertEquals(1, firstRecordNode.groupBy { it.partition() }.count())
        assertEquals(1, secondRecordNode.groupBy { it.partition() }.count())
        assertEquals(1, thirdRecordNode.groupBy { it.partition() }.count())
        assertEquals(1, fourthRecordNode.groupBy { it.partition() }.count())
        assertEquals(1, firstRecordRel.groupBy { it.partition() }.count())
        assertEquals(1, secondRecordRel.groupBy { it.partition() }.count())
    }
}