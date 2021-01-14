package streams.integrations

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.junit.Test
import org.neo4j.graphdb.TransactionFailureException
import org.neo4j.internal.helpers.collection.Iterators
import streams.extensions.execute
import streams.events.*
import streams.utils.JSONUtils
import streams.setConfig
import streams.start
import java.time.Duration
import java.util.*
import kotlin.test.*

class KafkaEventRouterLogCompactionTSE : KafkaEventRouterBaseTSE() {

    private val bootstrapServerMap = mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)

    private fun createCompactTopic(topic: String) = AdminClient.create(bootstrapServerMap).use {
        val topics = listOf(compactTopic(topic))
        it.createTopics(topics).all().get()
    }

    private fun compactTopic(topic: String) =
            NewTopic(topic, 1, 1).configs(mapOf(
                    "cleanup.policy" to "compact",
                    "segment.ms" to "10",
                    "retention.ms" to "1",
                    "min.cleanable.dirty.ratio" to "0.01",
            ))

    private fun createProducerRecordKeyForDeleteStrategy(meta: Meta) = "${meta.txId + meta.txEventId}-${meta.txEventId}"

    private fun initDbWithLogStrategy(strategy: String, otherConfigs: Map<String, String>? = null, constraints: List<String>? = null) {

        db.setConfig("streams.source.schema.polling.interval", "0")
                .setConfig("kafka.streams.log.compaction.strategy", strategy)

        otherConfigs?.forEach { (k, v) -> db.setConfig(k, v) }
        db.start()
        constraints?.forEach { db.execute(it) }
    }

    private fun createManyPersons() = db.execute("UNWIND range(1, 9999) AS id CREATE (:Person {name:id})")

    @Test
    fun `compact message with streams publish`() {
        val topic = UUID.randomUUID().toString()
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT)
        createCompactTopic(topic)

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
        db.execute("UNWIND range(1,9999) as id CALL streams.publish('$topic', 'id', {key: 'id'}) RETURN null") {
            assertEquals(9999, Iterators.count(it))
        }
        // wait for messages population in topic
        Thread.sleep(15000)
        val records = kafkaConsumer.poll(Duration.ofMinutes(1))

        // check if there is only one record with key 'test' and payload 'Compaction 4'
        val compactedRecord = records.filter { JSONUtils.readValue<String>(it.key()) == keyRecord }
        assertEquals(1, compactedRecord.count())
        assertEquals("Compaction 4", JSONUtils.readValue<Map<*,*>>(compactedRecord.first().value())["payload"])
    }

    @Test
    fun `delete single tombstone relation with strategy compact and constraints`() {
        // we create a topic with strategy compact
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic}" to "Person{*}",
                "streams.source.topic.relationships.${topic}" to "KNOWS{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        createCompactTopic(topic)

        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Pippo'})")
        db.execute("CREATE (:Person {name:'Pluto'})")
        db.execute("""
            |MATCH (pippo:Person {name:'Pippo'})
            |MATCH (pluto:Person {name:'Pluto'})
            |MERGE (pippo)-[:KNOWS]->(pluto)
        """.trimMargin())
        db.execute("MATCH (:Person {name:'Pippo'})-[rel:KNOWS]->(:Person {name:'Pluto'}) DELETE rel")

        // to activate the log compaction process we create dummy messages
        createManyPersons()
        Thread.sleep(20000)

        val records = kafkaConsumer.poll(Duration.ofMinutes(1))
        val nullRecords = records.filter { it.value() == null }
        assertEquals(1, nullRecords.count())
        val mapRecord: Map<String, Any> = JSONUtils.readValue(nullRecords.first().key())
        assertEquals(mapOf("name" to "Pippo"), mapRecord["start"])
        assertEquals(mapOf("name" to "Pluto"), mapRecord["end"])
        assertEquals("0", mapRecord["id"])

    }

    @Test
    fun `delete single tombstone relation with strategy compact`() {
        // we create a topic with strategy compact
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic}" to "Person{*}",
                "streams.source.topic.relationships.${topic}" to "KNOWS{*}")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics)
        createCompactTopic(topic)

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

        // to activate the log compaction process we create dummy messages
        createManyPersons()
        Thread.sleep(20000)

        // we check that there is only one tombstone record
        val records = kafkaConsumer.poll(Duration.ofMinutes(1))
        val nullRecords = records.filter { it.value() == null }
        assertEquals(1, nullRecords.count())
        val keyRecord: Map<String, Any> = JSONUtils.readValue(nullRecords.first().key())
        assertEquals("0", keyRecord["start"])
        assertEquals("1", keyRecord["end"])
        assertEquals("0", keyRecord["id"])
    }

    @Test
    fun `delete tombstone node with strategy compact`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics)
        createCompactTopic(topic)

        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Watson'})")
        db.execute("CREATE (:Person {name:'Sherlock'})")
        db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.address = '221B Baker Street'")
        // we delete a node, so will be created a tombstone record
        db.execute("MATCH (p:Person {name:'Sherlock'}) DETACH DELETE p")

        // to activate the log compaction process we create dummy messages and we waiting for message population
        createManyPersons()
        Thread.sleep(20000)

        val records = kafkaConsumer.poll(Duration.ofMinutes(1))
        val nullRecords = records.filter { it.value() == null }
        assertEquals(1, nullRecords.count())
        val keyRecord: String = JSONUtils.readValue(nullRecords.first().key())
        assertEquals("1", keyRecord)
    }

    @Test
    fun `delete node tombstone with strategy compact and constraint`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopisc = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopisc, queries)
        createCompactTopic(topic)

        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Watson'})")
        db.execute("CREATE (:Person {name:'Sherlock'})")
        db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.address = '221B Baker Street'")

        // to activate the log compaction process we create dummy messages and we waiting for message population
        db.execute("MATCH (p:Person {name:'Sherlock'}) DETACH DELETE p")
        createManyPersons()
        Thread.sleep(20000)

        val records = kafkaConsumer.poll(Duration.ofMinutes(1))
        val nullRecords = records.filter { it.value() == null }
        assertEquals(1, nullRecords.count())
        val keyRecord: Map<String, Any> = JSONUtils.readValue(nullRecords.first().key())
        assertEquals(mapOf("name" to "Sherlock"), keyRecord)
    }

    @Test
    fun `test relationship with multiple constraint and strategy compact`() {

        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
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
        assertEquals(mapOf("address" to "Baker Street"), JSONUtils.readValue(records.first().key()))

        db.execute("CREATE (p:Product {code:'1367', name: 'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        assertEquals(mapOf("code" to "1367"), JSONUtils.readValue(recordsTwo.first().key()))

        // we create a relationship with start and end node with constraint
        db.execute("MATCH (pe:Person:Other {name:'Sherlock'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        val mapRel: Map<String, Any> = JSONUtils.readValue(recordsThree.first().key())

        assertEquals(mapOf("name" to "Sherlock"), mapRel["start"])
        assertEquals(mapOf("code" to "1367"), mapRel["end"])
        assertEquals("0", mapRel["id"])

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
        assertEquals(mapOf("name" to "Sherlock"), JSONUtils.readValue<Map<*, *>>(records.first().key()))

        db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.name='Foo'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        assertEquals(mapOf("name" to "Foo"), JSONUtils.readValue<Map<*, *>>(recordsTwo.first().key()))

        db.execute("MATCH (p:Person {name:'Foo'}) SET p.surname='Bar'")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertEquals(mapOf("name" to "Foo"), JSONUtils.readValue<Map<*, *>>(recordsThree.first().key()))

        db.execute("MATCH (p:Person {name:'Foo'}) DETACH DELETE p")
        val recordsFour = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFour.count())
        assertEquals(mapOf("name" to "Foo"), JSONUtils.readValue<Map<*, *>>(recordsFour.first().key()))
        assertNull(recordsFour.first().value())
    }

    @Test
    fun `node without constraint and topic compact`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic}" to "Person{*}")
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
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic}" to "Person{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        kafkaConsumer.subscribe(listOf(topic))

        // we create a node with constraint and check that key is equal to constraint
        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        assertEquals(mapOf("name" to "Pippo"), JSONUtils.readValue<Map<*, *>>(records.first().key()))

        // we update the node
        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.name='Pluto'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        assertEquals(mapOf("name" to "Pluto"), JSONUtils.readValue<Map<*, *>>(recordsTwo.first().key()))

        // we delete the node
        db.execute("MATCH (p:Person {name:'Pluto'}) DETACH DELETE p")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertEquals(mapOf("name" to "Pluto"), JSONUtils.readValue<Map<*, *>>(recordsThree.first().key()))
        assertNull(recordsThree.first().value())
    }

    @Test
    fun `relation with nodes without constraint and topic compact`() {
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
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
        var idPayloadRel = (JSONUtils.readValue<Map<*, *>>(recordsThree.first().value())["payload"] as Map<*, *>)["id"]
        var keyRel = recordsThree.first().key()
        assertEquals(idPayloadRel,  JSONUtils.readValue<Map<*,*>>(keyRel)["id"])
        assertEquals(idPayloadStart,  JSONUtils.readValue<Map<*,*>>(keyRel)["start"])
        assertEquals(idPayloadEnd,  JSONUtils.readValue<Map<*,*>>(keyRel)["end"])

        // we update the relation
        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFour.count())
        idPayloadRel = (JSONUtils.readValue<Map<*, *>>(recordsFour.first().value())["payload"] as Map<*, *>)["id"]
        keyRel = recordsThree.first().key()
        assertEquals(idPayloadRel,  JSONUtils.readValue<Map<*,*>>(keyRel)["id"])
        assertEquals(idPayloadStart,  JSONUtils.readValue<Map<*,*>>(keyRel)["start"])
        assertEquals(idPayloadEnd,  JSONUtils.readValue<Map<*,*>>(keyRel)["end"])

        // we delete the relation and check if payload is null
        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFive.count())
        keyRel = recordsThree.first().key()
        assertEquals(idPayloadRel,  JSONUtils.readValue<Map<*,*>>(keyRel)["id"])
        assertEquals(idPayloadStart,  JSONUtils.readValue<Map<*,*>>(keyRel)["start"])
        assertEquals(idPayloadEnd,  JSONUtils.readValue<Map<*,*>>(keyRel)["end"])
        assertNull(recordsFive.first().value())
    }

    @Test
    fun `relation with nodes with constraint and strategy compact`() {
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
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
        val keyStartNode = JSONUtils.readValue<Map<String, String>>(records.first().key())
        assertEquals(mapOf("name" to "Pippo"), keyStartNode)
        assertEquals(mapOf("name" to "Pippo"), propsAfterStartNode)

        db.execute("CREATE (p:Product {code:'1367', name: 'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        val propsAfterEndNode =  JSONUtils.asStreamsTransactionEvent(recordsTwo.first().value()).payload.after?.properties
        val keyEndNode = JSONUtils.readValue<Map<String, String>>(recordsTwo.first().key())
        assertEquals(mapOf("code" to "1367"), keyEndNode)
        assertEquals(mapOf("code" to "1367", "name" to "Notebook"), propsAfterEndNode)

        db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        var relPayload = JSONUtils.asStreamsTransactionEvent(recordsThree.first().value()).payload as RelationshipPayload
        assertEquals(mapOf("start" to relPayload.start.ids,
                "end" to relPayload.end.ids,
                "id" to relPayload.id),
                JSONUtils.readValue(recordsThree.first().key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFour.count())
        relPayload = JSONUtils.asStreamsTransactionEvent(recordsFour.first().value()).payload as RelationshipPayload
        assertEquals(mapOf("start" to relPayload.start.ids,
                "end" to relPayload.end.ids,
                "id" to relPayload.id),
                JSONUtils.readValue(recordsFour.first().key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsFive.count())
        assertEquals(mapOf("start" to relPayload.start.ids,
                "end" to relPayload.end.ids,
                "id" to relPayload.id),
                JSONUtils.readValue(recordsFive.first().key()))
        assertNull(recordsFive.first().value())
    }

    // we verify that with the default strategy everything works as before
    @Test
    fun `node without constraint and strategy delete`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic}" to "Person{*}")
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

        val sourceTopics = mapOf("streams.source.topic.nodes.${topic}" to "Person{*}")
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
        initDbWithLogStrategy(
                TopicConfig.CLEANUP_POLICY_DELETE,
                mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                        "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
                        "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
                ),
                listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE",
                        "CREATE CONSTRAINT ON (p:Product) ASSERT p.code IS UNIQUE")
        )
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
    fun `invalid log strategy`() {
        val topic = UUID.randomUUID().toString()
        val invalid = "invalid"
        initDbWithLogStrategy(
                invalid,
                mapOf("streams.source.topic.nodes.${topic}" to "Person{*}")
        )
        kafkaConsumer.subscribe(listOf(topic))

        assertFailsWith(TransactionFailureException::class, "Invalid kafka.streams.log.compaction.strategy value: $invalid") {
            db.execute("CREATE (:Person {name:'Pippo'})")
        }
    }

}