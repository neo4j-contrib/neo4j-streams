package streams.integrations

import extension.newDatabase
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.config.TopicConfig
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import org.neo4j.helpers.collection.Iterators
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.assertion.Assert
import streams.events.Meta
import streams.kafka.KafkaConfiguration
import streams.procedures.StreamsProcedures
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import streams.kafka.KafkaTestUtils.createConsumer
import streams.serialization.JSONUtils
import java.util.concurrent.TimeUnit

@Suppress("DEPRECATION")
class KafkaEventRouterLogCompactionIT: KafkaEventRouterBaseIT() {

    private fun compactTopic(topic: String, numTopics: Int, withCompact: Boolean) = run {
        val newTopic = NewTopic(topic, numTopics, 1)
        if (withCompact) {
            newTopic.configs(mapOf(
                    "cleanup.policy" to "compact",
                    "segment.ms" to "10",
                    "retention.ms" to "1",
                    "min.cleanable.dirty.ratio" to "0.01"))
        }
        newTopic
    }

    private fun createCompactTopic(topic: String, numTopics: Int = 1, withCompact: Boolean = true) {
        AdminClient.create(mapOf("bootstrap.servers" to kafka.bootstrapServers)).use {
            val topics = listOf(compactTopic(topic, numTopics, withCompact))
            it.createTopics(topics).all().get()
        }
    }

    private fun assertTopicFilled(kafkaConsumer: KafkaConsumer<String, ByteArray>,
                          assertion: (ConsumerRecords<String, ByteArray>) -> Boolean
    ) {
        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            kafkaConsumer.seekToBeginning(kafkaConsumer.assignment())
            val records = kafkaConsumer.poll(Duration.ofSeconds(5))
            assertion(records)
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    private fun createProducerRecordKeyForDeleteStrategy(meta: Meta) = "${meta.txId + meta.txEventId}-${meta.txEventId}"

    private fun createManyPersons() = db.execute("UNWIND range(1, 9999) AS id CREATE (:Person {name:id})")

    private fun initDbWithLogStrategy(strategy: String, otherConfigs: Map<String, String>? = null, constraints: List<String>? = null) {
        graphDatabaseBuilder.setConfig("streams.source.schema.polling.interval", "0")
                .setConfig("kafka.streams.log.compaction.strategy", strategy)

        otherConfigs?.forEach { (k, v) -> graphDatabaseBuilder.setConfig(k, v) }
        db.shutdown()
        db = graphDatabaseBuilder.newDatabase() as GraphDatabaseAPI
        constraints?.forEach { db.execute(it).close() }
    }

    @Test
    fun `compact message with streams publish`() {
        val topic = UUID.randomUUID().toString()
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT)
        createCompactTopic(topic)
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsProcedures::class.java, true)

        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->

            consumer.subscribe(listOf(topic))

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
            db.execute("UNWIND range(1,9999) as id CALL streams.publish('$topic', id, {key: id}) RETURN null").use {
                assertEquals(9999, Iterators.count(it))
            }

            // check if there is only one record with key 'test' and payload 'Compaction 4'
            assertTopicFilled(consumer) {
                val compactedRecord = it.filter { JSONUtils.readValue<String>(it.key()) == keyRecord }
                it.count() == 500 &&
                        compactedRecord.count() == 1 &&
                        JSONUtils.readValue<Map<*,*>>(compactedRecord.first().value())["payload"] == "Compaction 4"
            }
        }
    }

    @Test
    fun `delete single tombstone relation with strategy compact and constraints`() {
        // we create a topic with strategy compact
        val keyRel = "KNOWS"
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}",
                "streams.source.topic.relationships.$topic" to "KNOWS{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        createCompactTopic(topic)
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person {name:'Pippo'})")
            db.execute("CREATE (:Person {name:'Pluto'})")
            db.execute("""
            |MATCH (pippo:Person {name:'Pippo'})
            |MATCH (pluto:Person {name:'Pluto'})
            |MERGE (pippo)-[:$keyRel]->(pluto)
        """.trimMargin())
            db.execute("MATCH (:Person {name:'Pippo'})-[rel:$keyRel]->(:Person {name:'Pluto'}) DELETE rel")

            // to activate the log compaction process we create dummy messages
            createManyPersons()

            assertTopicFilled(consumer) {
                val nullRecords = it.filter { it.value() == null }
                val start = mapOf("ids" to mapOf("name" to "Pippo"), "labels" to listOf("Person"))
                val end = mapOf("ids" to mapOf("name" to "Pluto"), "labels" to listOf("Person"))
                it.count() == 500
                        && nullRecords.count() == 1
                        && JSONUtils.readValue<Map<*,*>>(nullRecords.first().key()) == mapOf("start" to start, "end" to end, "label" to keyRel)
            }
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
        createCompactTopic(topic)
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person {name:'Pippo'})")
            db.execute("CREATE (:Person {name:'Pluto'})")
            // we create a relation, so will be created a record with payload not null
            db.execute("""
                |MATCH (pippo:Person {name:'Pippo'})
                |MATCH (pluto:Person {name:'Pluto'})
                |MERGE (pippo)-[:$relType]->(pluto)
            """.trimMargin())

            // we delete the relation, so will be created a tombstone record
            db.execute("MATCH (:Person {name:'Pippo'})-[rel:$relType]->(:Person {name:'Pluto'}) DELETE rel")
            db.execute("CREATE (:Person {name:'Paperino'})")

            // to activate the log compaction process we create dummy messages
            createManyPersons()

            // we check that there is only one tombstone record
            assertTopicFilled(consumer) {
                val nullRecords = it.filter { it.value() == null }
                val keyStartRecord = JSONUtils.readValue<String>(it.first().key())
                val keyEndRecord = it.elementAtOrNull(1)?.let { JSONUtils.readValue<String>(it.key()) }
                it.count() == 500
                        && nullRecords.count() == 1
                        && JSONUtils.readValue<Map<*,*>>(nullRecords.first().key()) == mapOf("start" to keyStartRecord, "end" to keyEndRecord, "label" to relType)
            }
        }
    }

    @Test
    fun `delete node tombstone with strategy compact and constraint`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        createCompactTopic(topic)
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person {name:'Watson'})")
            db.execute("CREATE (:Person {name:'Sherlock'})")
            db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.address = '221B Baker Street'")

            // to activate the log compaction process we create dummy messages and we waiting for message population
            db.execute("MATCH (p:Person {name:'Sherlock'}) DETACH DELETE p")

            createManyPersons()
            assertTopicFilled(consumer) {
                val nullRecords = it.filter { it.value() == null }
                val keyRecordExpected = mapOf("ids" to mapOf("name" to "Sherlock"), "labels" to listOf("Person"))
                it.count() == 500
                        && nullRecords.count() == 1
                        && keyRecordExpected == JSONUtils.readValue<Map<*,*>>(nullRecords.first().key())
            }
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
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(topic)

            db.execute("CREATE (:Person:Other {name: 'Sherlock', surname: 'Holmes', address: 'Baker Street'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())
            var keyStart = mapOf("ids" to mapOf("address" to "Baker Street"), "labels" to listOf("Other", "Person"))
            assertEquals(keyStart, JSONUtils.readValue(records.first().key()))

            db.execute("CREATE (p:Product {code:'1367', name: 'Notebook'})")
            val recordsTwo = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsTwo.count())
            val keyEnd = mapOf("ids" to mapOf("code" to "1367"), "labels" to listOf("Product"))
            assertEquals(keyEnd, JSONUtils.readValue(recordsTwo.first().key()))

            // we create a relationship with start and end node with constraint
            db.execute("MATCH (pe:Person:Other {name:'Sherlock'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
            val recordsThree = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsThree.count())
            val mapRel: Map<String, Any> = JSONUtils.readValue(recordsThree.first().key())

            keyStart = mapOf("ids" to mapOf("name" to "Sherlock"), "labels" to listOf("Other", "Person"))
            assertEquals(keyStart, mapRel["start"])
            assertEquals(keyEnd, mapRel["end"])
            assertEquals(relType, mapRel["label"])

            // we update the relationship
            db.execute("MATCH (:Person:Other {name:'Sherlock'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
            val recordsFour = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFour.count())
            assertEquals(mapRel, JSONUtils.readValue(recordsThree.first().key()))

            // we delete the relationship
            db.execute("MATCH (:Person:Other {name:'Sherlock'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
            val recordsFive = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFive.count())
            assertEquals(mapRel, JSONUtils.readValue(recordsFive.first().key()))
            assertNull(recordsFive.first().value())
        }
    }

    @Test
    fun `test label with multiple constraints and strategy compact`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person:Neo4j{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE",
                "CREATE CONSTRAINT ON (p:Neo4j) ASSERT p.surname IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person:Neo4j {name:'Sherlock', surname: 'Holmes', address: 'Baker Street'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())
            var keyNode = mapOf("ids" to mapOf("name" to "Sherlock"), "labels" to listOf("Person", "Neo4j"))
            assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(records.first().key()))

            db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.name='Foo'")
            val recordsTwo = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsTwo.count())
            keyNode = mapOf("ids" to mapOf("name" to "Foo"), "labels" to listOf("Person", "Neo4j"))
            assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(recordsTwo.first().key()))

            db.execute("MATCH (p:Person {name:'Foo'}) SET p.surname='Bar'")
            val recordsThree = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsThree.count())
            assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(recordsThree.first().key()))

            db.execute("MATCH (p:Person {name:'Foo'}) DETACH DELETE p")
            val recordsFour = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFour.count())
            assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(recordsFour.first().key()))
            assertNull(recordsFour.first().value())
        }
    }

    @Test
    fun `node without constraint and topic compact`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic}" to "Person{*}")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics)
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person {name:'Pippo'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())

            var idPayload = (JSONUtils.readValue<Map<*, *>>(records.first().value())["payload"] as Map<*, *>)["id"]
            assertEquals(idPayload, JSONUtils.readValue(records.first().key()))

            db.execute("MATCH (p:Person {name:'Pippo'}) SET p.surname='Pluto'")
            val recordsTwo = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsTwo.count())
            idPayload = (JSONUtils.readValue<Map<*, *>>(recordsTwo.first().value())["payload"] as Map<*, *>)["id"]
            assertEquals(idPayload, JSONUtils.readValue(recordsTwo.first().key()))

            db.execute("MATCH (p:Person {name:'Pippo'}) DETACH DELETE p")
            val recordsThree = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsThree.count())
            assertEquals(idPayload, JSONUtils.readValue(recordsThree.first().key()))
            assertNull(recordsThree.first().value())
        }
    }

    @Test
    fun `node with constraint and topic compact`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic}" to "Person{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics, queries)
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(listOf(topic))

            // we create a node with constraint and check that key is equal to constraint
            db.execute("CREATE (:Person {name:'Pippo'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())
            var keyNode = mapOf("ids" to mapOf("name" to "Pippo"), "labels" to listOf("Person"))
            assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(records.first().key()))

            // we update the node
            db.execute("MATCH (p:Person {name:'Pippo'}) SET p.name='Pluto'")
            val recordsTwo = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsTwo.count())
            keyNode = mapOf("ids" to mapOf("name" to "Pluto"), "labels" to listOf("Person"))
            assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(recordsTwo.first().key()))

            // we delete the node
            db.execute("MATCH (p:Person {name:'Pluto'}) DETACH DELETE p")
            val recordsThree = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsThree.count())
            assertEquals(keyNode, JSONUtils.readValue<Map<*, *>>(recordsThree.first().key()))
            assertNull(recordsThree.first().value())
        }
    }

    @Test
    fun `relation with nodes without constraint and topic compact`() {
        val relType = "BUYS"
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                "streams.source.topic.relationships.${topic[1]}" to "$relType{*}",
                "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
        )
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_COMPACT, sourceTopics)
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(topic)

            // we create a node without constraint and check that key is equal to id's payload
            db.execute("CREATE (:Person {name:'Pippo'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())
            val idPayloadStart = (JSONUtils.readValue<Map<*, *>>(records.first().value())["payload"] as Map<*, *>)["id"]
            assertEquals(idPayloadStart, JSONUtils.readValue(records.first().key()))

            db.execute("CREATE (p:Product {name:'Notebook'})")
            val recordsTwo = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsTwo.count())
            val idPayloadEnd = (JSONUtils.readValue<Map<*, *>>(recordsTwo.first().value())["payload"] as Map<*, *>)["id"]
            assertEquals(idPayloadEnd, JSONUtils.readValue(recordsTwo.first().key()))

            // we create a relation with start and end node without constraint
            db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
            val recordsThree = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsThree.count())
            var keyRel = recordsThree.first().key()
            assertEquals(relType,  JSONUtils.readValue<Map<*,*>>(keyRel)["label"])
            assertEquals(idPayloadStart,  JSONUtils.readValue<Map<*,*>>(keyRel)["start"])
            assertEquals(idPayloadEnd,  JSONUtils.readValue<Map<*,*>>(keyRel)["end"])

            // we update the relation
            db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
            val recordsFour = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFour.count())
            keyRel = recordsThree.first().key()
            assertEquals(relType,  JSONUtils.readValue<Map<*,*>>(keyRel)["label"])
            assertEquals(idPayloadStart,  JSONUtils.readValue<Map<*,*>>(keyRel)["start"])
            assertEquals(idPayloadEnd,  JSONUtils.readValue<Map<*,*>>(keyRel)["end"])

            // we delete the relation and check if payload is null
            db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
            val recordsFive = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFive.count())
            keyRel = recordsThree.first().key()
            assertEquals(relType,  JSONUtils.readValue<Map<*,*>>(keyRel)["label"])
            assertEquals(idPayloadStart,  JSONUtils.readValue<Map<*,*>>(keyRel)["start"])
            assertEquals(idPayloadEnd,  JSONUtils.readValue<Map<*,*>>(keyRel)["end"])
            assertNull(recordsFive.first().value())
        }
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
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(topic)

            db.execute("CREATE (:Person {name:'Pippo'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())
            val propsAfterStartNode = JSONUtils.asStreamsTransactionEvent(records.first().value()).payload.after?.properties
            val keyStartNodeActual = JSONUtils.readValue<Map<String, Any>>(records.first().key())
            val keyStartExpected = mapOf("ids" to mapOf("name" to "Pippo"), "labels" to listOf("Person"))
            assertEquals(keyStartExpected, keyStartNodeActual)
            assertEquals(mapOf("name" to "Pippo"), propsAfterStartNode)

            db.execute("CREATE (p:Product {code:'1367', name: 'Notebook'})")
            val recordsTwo = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsTwo.count())
            val propsAfterEndNode =  JSONUtils.asStreamsTransactionEvent(recordsTwo.first().value()).payload.after?.properties
            val keyEndNodeActual = JSONUtils.readValue<Map<String, Any>>(recordsTwo.first().key())
            val keyEndExpected = mapOf("ids" to mapOf("code" to "1367"), "labels" to listOf("Product"))
            assertEquals(keyEndExpected, keyEndNodeActual)
            assertEquals(mapOf("code" to "1367", "name" to "Notebook"), propsAfterEndNode)

            db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:$labelRel]->(pr)")
            val recordsThree = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsThree.count())
            assertEquals(mapOf("start" to keyStartExpected,
                    "end" to keyEndExpected,
                    "label" to labelRel),
                    JSONUtils.readValue(recordsThree.first().key()))

            db.execute("MATCH (:Person {name:'Pippo'})-[rel:$labelRel]->(:Product {name:'Notebook'}) SET rel.price = '100'")
            val recordsFour = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFour.count())
            assertEquals(mapOf("start" to keyStartExpected,
                    "end" to keyEndExpected,
                    "label" to labelRel),
                    JSONUtils.readValue(recordsFour.first().key()))

            db.execute("MATCH (:Person {name:'Pippo'})-[rel:$labelRel]->(:Product {name:'Notebook'}) DELETE rel")
            val recordsFive = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFive.count())
            assertEquals(mapOf("start" to keyStartExpected,
                    "end" to keyEndExpected,
                    "label" to labelRel),
                    JSONUtils.readValue(recordsFive.first().key()))
            assertNull(recordsFive.first().value())
        }
    }

    // we verify that with the default strategy everything works as before
    @Test
    fun `node without constraint and strategy delete`() {
        val topic = UUID.randomUUID().toString()
        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_DELETE, sourceTopics)
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person {name:'Pippo'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())
            var record = records.first()
            var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("MATCH (p:Person {name:'Pippo'}) SET p.surname='Pluto'")
            val recordsTwo = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsTwo.count())
            record = recordsTwo.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("MATCH (p:Person {name:'Pippo'}) DETACH DELETE p")
            val recordsThree = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsThree.count())
            record = recordsThree.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))
            assertNotNull(record.value())
        }
    }

    @Test
    fun `node with constraint and strategy delete`() {
        val topic = UUID.randomUUID().toString()

        val sourceTopics = mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        val queries = listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_DELETE, sourceTopics, queries)
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person {name:'Pippo'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())
            var record = records.first()
            var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("MATCH (p:Person {name:'Pippo'}) SET p.name='Pluto'")
            val recordsTwo = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsTwo.count())
            record = records.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("MATCH (p:Person {name:'Pluto'}) DETACH DELETE p")
            val recordsThree = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsThree.count())
            record = recordsThree.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))
            assertNotNull(record.value())
        }
    }

    @Test
    fun `relation with nodes without constraint and strategy delete`() {
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val sourceTopics = mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
                "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
        )
        initDbWithLogStrategy(TopicConfig.CLEANUP_POLICY_DELETE, sourceTopics)
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(topic)

            db.execute("CREATE (:Person {name:'Pippo'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())
            var record = records.first()
            var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("CREATE (p:Product {name:'Notebook'})")
            val recordsTwo = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsTwo.count())
            record = recordsTwo.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
            val recordsThree = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsThree.count())
            record = recordsThree.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
            val recordsFour = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFour.count())
            record = recordsFour.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
            val recordsFive = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFive.count())
            record = recordsFive.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))
            assertNotNull(record.value())
        }
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
        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(topic)

            db.execute("CREATE (:Person {name:'Pippo'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())
            var record = records.first()
            var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("CREATE (p:Product {code:'1367', name: 'Notebook'})")
            val recordsTwo = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsTwo.count())
            record = recordsTwo.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
            val recordsThree = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsThree.count())
            record = recordsThree.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
            val recordsFour = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFour.count())
            record = recordsFour.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))

            db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
            val recordsFive = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, recordsFive.count())
            record = recordsFive.first()
            meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))
            assertNotNull(record.value())
        }
    }

    @Test
    fun `invalid log strategy should switch to default strategy value`() {
        val topic = UUID.randomUUID().toString()
        // we create an invalid log strategy
        initDbWithLogStrategy("invalid", mapOf("streams.source.topic.nodes.$topic" to "Person{*}"))

        createConsumer(KafkaConfiguration(bootstrapServers = kafka.bootstrapServers)).use { consumer ->
            consumer.subscribe(listOf(topic))

            // we verify that log strategy is default
            db.execute("CREATE (:Person {name:'Pippo'})")
            val records = consumer.poll(Duration.ofSeconds(10))
            assertEquals(1, records.count())
            val record = records.first()
            val meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
            assertEquals(createProducerRecordKeyForDeleteStrategy(meta), JSONUtils.readValue(record.key()))
        }
    }
}