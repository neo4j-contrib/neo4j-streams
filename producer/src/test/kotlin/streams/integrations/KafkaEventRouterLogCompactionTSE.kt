package streams.integrations

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.junit.*
import streams.extensions.execute
import streams.events.*
import streams.utils.JSONUtils
import streams.setConfig
import streams.start
import java.time.Duration
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KafkaEventRouterLogCompactionTSE : KafkaEventRouterBaseTSE() {

    private val bootstrapServerMap = mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)

    private fun compactTopic(topic: String) =
            NewTopic(topic, 60, 1).configs(mapOf(
                    "cleanup.policy" to "compact",
                    "delete.retention.ms" to "0",
                    "segment.ms" to "10",
                    "retention.ms" to "1",
                    "min.cleanable.dirty.ratio" to "0.01",
            ))

    private fun stringStrategyDelete(meta: Meta) = "${meta.txId + meta.txEventId}-${meta.txEventId}"

    private fun initDbWithPolicy(policy: String, otherConfigs: Map<String, String>? = null, constraints: List<String>? = null) {

        db.setConfig("streams.source.schema.polling.interval", "0")
                .setConfig("kafka.log.cleanup.policy", policy)

        otherConfigs?.forEach { (k, v) -> db.setConfig(k, v) }
        db.start()
        constraints?.forEach { db.execute(it) }
    }

    private fun createManyPersons() = (1..9999).forEach {
        db.execute("CREATE (:Person {name:'$it'})")
    }

    @Test
    fun `compact message with streams publish`() {
        val topic = UUID.randomUUID().toString()
        initDbWithPolicy(TopicConfig.CLEANUP_POLICY_COMPACT)

        AdminClient.create(bootstrapServerMap).use {
            it.createTopics(listOf(compactTopic(topic))).all().get()

            KafkaEventRouterSuiteIT.registerPublishProcedure(db)
            kafkaConsumer.subscribe(listOf(topic))

            val keyRecord = "test"
            db.execute("CALL streams.publish('$topic', 'Compaction 0', {key: 'Baz'})")
            db.execute("CALL streams.publish('$topic', 'Compaction 1', {key: '$keyRecord'})")
            db.execute("CALL streams.publish('$topic', 'Compaction 2', {key: '$keyRecord'})")
            db.execute("CALL streams.publish('$topic', 'Compaction 3', {key: 'Foo'})")
            db.execute("CALL streams.publish('$topic', 'Compaction 4', {key: '$keyRecord'})")
            (1..9999).forEach {
                db.execute("CALL streams.publish('$topic', '$it', {key: '$it'})")
            }

            val records = kafkaConsumer.poll(Duration.ofMinutes(1))
            assertEquals(1, records.filter{ JSONUtils.readValue<String>(it.key()) == keyRecord }.count())
        }
    }

    @Test
    fun `delete single tombstone relation with compaction and constraints`() {
        val topic = UUID.randomUUID().toString()
        initDbWithPolicy(
                TopicConfig.CLEANUP_POLICY_COMPACT,
                mapOf("streams.source.topic.nodes.${topic}" to "Person{*}",
                        "streams.source.topic.relationships.${topic}" to "KNOWS{*}"),
                listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        )
        AdminClient.create(bootstrapServerMap).use {

            it.createTopics(listOf(compactTopic(topic))).all().get()
            kafkaConsumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person {name:'Pippo'})")
            db.execute("CREATE (:Person {name:'Pluto'})")
            db.execute("""
                |MATCH (pippo:Person {name:'Pippo'})
                |MATCH (pluto:Person {name:'Pluto'})
                |MERGE (pippo)-[:KNOWS]->(pluto)
            """.trimMargin())
            db.execute("MATCH (:Person {name:'Pippo'})-[rel:KNOWS]->(:Person {name:'Pluto'}) DELETE rel")
            createManyPersons()

            val records = kafkaConsumer.poll(Duration.ofSeconds(10))
            assertTrue { records.all { JSONUtils.asStreamsTransactionEvent(it.value()).payload is NodePayload} }
        }
    }

    @Test
    fun `delete single tombstone relation with compaction`() {
        val topic = UUID.randomUUID().toString()

        initDbWithPolicy(
                TopicConfig.CLEANUP_POLICY_COMPACT,
                mapOf("streams.source.topic.nodes.${topic}" to "Person{*}",
                        "streams.source.topic.relationships.${topic}" to "KNOWS{*}")
        )
        AdminClient.create(bootstrapServerMap).use {

            it.createTopics(listOf(compactTopic(topic))).all().get()
            kafkaConsumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person {name:'Pippo'})")
            db.execute("CREATE (:Person {name:'Pluto'})")
            db.execute("""
                |MATCH (pippo:Person {name:'Pippo'})
                |MATCH (pluto:Person {name:'Pluto'})
                |MERGE (pippo)-[:KNOWS]->(pluto)
            """.trimMargin())
            db.execute("MATCH (:Person {name:'Pippo'})-[rel:KNOWS]->(:Person {name:'Pluto'}) DELETE rel")
            createManyPersons()

            val records = kafkaConsumer.poll(Duration.ofMinutes(1))
            assertTrue { records.all { JSONUtils.asStreamsTransactionEvent(it.value()).payload is NodePayload} }
        }

    }

    @Test
    fun testDeleteNodeTombstoneCompact() {
        val topic = UUID.randomUUID().toString()
        initDbWithPolicy(TopicConfig.CLEANUP_POLICY_COMPACT,
                mapOf("streams.source.topic.nodes.$topic" to "Person{*}")
        )
        AdminClient.create(
                mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)).use {

            it.createTopics(listOf(compactTopic(topic))).all().get()
            kafkaConsumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person {name:'Watson'})")
            db.execute("CREATE (:Person {name:'Sherlock'})")
            db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.address = '221B Baker Street'")
            db.execute("MATCH (p:Person {name:'Sherlock'}) DETACH DELETE p")
            createManyPersons()

            val records = kafkaConsumer.poll(Duration.ofSeconds(10))
            assertTrue { records.none {
                JSONUtils.asStreamsTransactionEvent(it.value()).payload.after?.properties?.get("name").toString() == "Sherlock"
            }}
            assertTrue { records.any {
                JSONUtils.asStreamsTransactionEvent(it.value()).payload.after?.properties?.get("name").toString() == "Watson"
            }}
        }
    }

    @Test
    fun testDeleteNodeTombstoneCompactAndConstraint() {
        val topic = UUID.randomUUID().toString()
        initDbWithPolicy(TopicConfig.CLEANUP_POLICY_COMPACT,
                mapOf("streams.source.topic.nodes.$topic" to "Person{*}"),
                listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        )
        AdminClient.create(
                mapOf("bootstrap.servers" to KafkaEventRouterSuiteIT.kafka.bootstrapServers)).use {

            it.createTopics(listOf(compactTopic(topic))).all().get()
            kafkaConsumer.subscribe(listOf(topic))

            db.execute("CREATE (:Person {name:'Watson'})")
            db.execute("CREATE (:Person {name:'Sherlock'})")
            db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.address = '221B Baker Street'")
            db.execute("MATCH (p:Person {name:'Sherlock'}) DETACH DELETE p")
            createManyPersons()

            val records = kafkaConsumer.poll(Duration.ofSeconds(10))
            assertTrue { records.none {
                JSONUtils.asStreamsTransactionEvent(it.value()).payload.after?.properties?.get("name").toString() == "Sherlock"
            }}
            assertTrue { records.any {
                JSONUtils.asStreamsTransactionEvent(it.value()).payload.after?.properties?.get("name").toString() == "Watson"
            }}
        }
    }

    // TODO - CHECK IDS RESULT
    @Ignore
    @Test
    fun testRelationshipWithMultipleConstraintInNodes() {
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        initDbWithPolicy(
                TopicConfig.CLEANUP_POLICY_COMPACT,
                mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                        "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
                        "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
                ),
                listOf("CREATE CONSTRAINT ON (p:Product) ASSERT p.code IS UNIQUE",
                        "CREATE CONSTRAINT ON (p:Other) ASSERT p.a IS UNIQUE",
                        "CREATE CONSTRAINT ON (p:Person) ASSERT p.name2 IS UNIQUE"
                        )
        )
        kafkaConsumer.subscribe(topic)

        db.execute("CREATE (:Person:Other {name2: 'Sherlock', surname: 'Holmes', address: 'Baker Street'})")
        val records = kafkaConsumer.poll(10000)
        assertEquals(1, records.count())
//        assertEquals(mapOf("name" to "Sherlock", "surname" to "Holmes"), JSONUtils.readValue(records.first().key()))

        db.execute("CREATE (p:Product:Other {code:'1367', name: 'Notebook', surname: 'Bar'})")
        val recordsTwo = kafkaConsumer.poll(10000)
        assertEquals(1, recordsTwo.count())
//        assertEquals(mapOf("code" to "1367", "surname" to "Bar"), JSONUtils.readValue(recordsTwo.first().key()))

        db.execute("MATCH (pe:Person:Other {name2:'Sherlock'}), (pr:Product:Other {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(10000)
        assertEquals(1, recordsThree.count())
        assertEquals(mapOf("start" to mapOf("surname" to "Holmes"),
                "end" to mapOf("surname" to "Bar"),
                "id" to "0"),
                JSONUtils.readValue(recordsThree.first().key()))

        db.execute("MATCH (:Person:Other {name:'Sherlock'})-[rel:BUYS]->(:Product:Other {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(10000)
        assertEquals(1, recordsFour.count())
        assertEquals(mapOf("start" to mapOf("surname" to "Holmes"),
                "end" to mapOf("surname" to "Bar"),
                "id" to "0"),
                JSONUtils.readValue(recordsThree.first().key()))

        db.execute("MATCH (:Person:Other {name:'Pippo'})-[rel:BUYS]->(:Product:Other {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(10000)
        assertEquals(1, recordsFive.count())
        assertEquals(mapOf("start" to mapOf("surname" to "Holmes"),
                "end" to mapOf("surname" to "Bar"),
                "id" to "0"),
                JSONUtils.readValue(recordsFive.first().key()))
        assertNull(recordsFive.first().value())
    }

    @Test
    fun testLabelWithMultipleConstraint() {
        val topic = UUID.randomUUID().toString()
        initDbWithPolicy(TopicConfig.CLEANUP_POLICY_COMPACT,
                mapOf("streams.source.topic.nodes.$topic" to "Person:Neo4j{*}"),
                listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE",
                        "CREATE CONSTRAINT ON (p:Neo4j) ASSERT p.surname IS UNIQUE")
        )
        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person:Neo4j {name:'Sherlock', surname: 'Holmes', address: 'Baker Street'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        assertEquals(mapOf("name" to "Sherlock", "surname" to "Holmes"), JSONUtils.readValue<Map<*, *>>(records.first().key()))

        db.execute("MATCH (p:Person {name:'Sherlock'}) SET p.name='Foo'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        assertEquals(mapOf("name" to "Foo", "surname" to "Holmes"), JSONUtils.readValue<Map<*, *>>(recordsTwo.first().key()))

        db.execute("MATCH (p:Person {name:'Foo'}) DETACH DELETE p")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertEquals(mapOf("name" to "Foo", "surname" to "Holmes"), JSONUtils.readValue<Map<*, *>>(recordsThree.first().key()))
        assertNull(recordsThree.first().value())
    }

    @Test
    fun nodeWithNoConstraintAndTopicCompact() {
        val topic = UUID.randomUUID().toString()

        initDbWithPolicy(
                TopicConfig.CLEANUP_POLICY_COMPACT,
                mapOf("streams.source.topic.nodes.${topic}" to "Person{*}"),
        )
        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        assertEquals("0", JSONUtils.readValue(records.first().key()))

        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.surname='Pluto'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        assertEquals("0", JSONUtils.readValue(recordsTwo.first().key()))


        db.execute("MATCH (p:Person {name:'Pippo'}) DETACH DELETE p")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertNull(recordsThree.first().value())
    }

    @Test
    fun nodeWithConstraintAndTopicCompact() {
        val topic = UUID.randomUUID().toString()

        initDbWithPolicy(
                TopicConfig.CLEANUP_POLICY_COMPACT,
                mapOf("streams.source.topic.nodes.${topic}" to "Person{*}"),
                listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        )
        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        assertEquals(mapOf("name" to "Pippo"), JSONUtils.readValue<Map<*, *>>(records.first().key()))

        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.name='Pluto'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        assertEquals(mapOf("name" to "Pluto"), JSONUtils.readValue<Map<*, *>>(recordsTwo.first().key()))

        db.execute("MATCH (p:Person {name:'Pluto'}) DETACH DELETE p")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertNull(recordsThree.first().value())
    }

    @Test
    fun relationWithNodesWithNoConstraintAndTopicCompact() {
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())

        initDbWithPolicy(
                TopicConfig.CLEANUP_POLICY_COMPACT,
                mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                        "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
                        "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
                ),
        )
        kafkaConsumer.subscribe(topic)

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(10000)
        assertEquals(1, records.count())
        assertEquals("0", JSONUtils.readValue(records.first().key()))

        db.execute("CREATE (p:Product {name:'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(10000)
        assertEquals(1, recordsTwo.count())
        assertEquals("1", JSONUtils.readValue(recordsTwo.first().key()))

        db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(10000)
        assertEquals(1, recordsThree.count())
        assertEquals(mapOf("start" to "0", "end" to "1", "id" to "0"),
                JSONUtils.readValue(recordsThree.first().key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(10000)
        assertEquals(1, recordsFour.count())
        assertEquals(mapOf("start" to "0", "end" to "1", "id" to "0"),
                JSONUtils.readValue(recordsFour.first().key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(10000)
        assertEquals(1, recordsFive.count())
        assertNull(recordsFive.first().value())
    }

    @Test
    fun relationWithNodesWithConstraintAndTopicCompact() {
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        initDbWithPolicy(
                TopicConfig.CLEANUP_POLICY_COMPACT,
                mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                        "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
                        "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
                ),
                listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE",
                        "CREATE CONSTRAINT ON (p:Product) ASSERT p.code IS UNIQUE")
        )
        kafkaConsumer.subscribe(topic)

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(10000)
        assertEquals(1, records.count())
        assertEquals(mapOf("name" to "Pippo"), JSONUtils.readValue(records.first().key()))

        db.execute("CREATE (p:Product {code:'1367', name: 'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(10000)
        assertEquals(1, recordsTwo.count())
        assertEquals(mapOf("code" to "1367"), JSONUtils.readValue(recordsTwo.first().key()))

        db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(10000)
        assertEquals(1, recordsThree.count())
        assertEquals(mapOf("start" to mapOf("name" to "Pippo"),
                "end" to mapOf("code" to "1367"),
                "id" to "0"),
                JSONUtils.readValue(recordsThree.first().key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(10000)
        assertEquals(1, recordsFour.count())
        assertEquals(mapOf("start" to mapOf("name" to "Pippo"),
                "end" to mapOf("code" to "1367"),
                "id" to "0"),
                JSONUtils.readValue(recordsThree.first().key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(10000)
        assertEquals(1, recordsFive.count())
        assertNull(recordsFive.first().value())
    }

    @Test
    fun nodeWithNoConstraintAndTopicDelete() {
        val topic = UUID.randomUUID().toString()
        initDbWithPolicy(
                TopicConfig.CLEANUP_POLICY_DELETE,
                mapOf("streams.source.topic.nodes.${topic}" to "Person{*}"),
        )
        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(10000)
        assertEquals(1, records.count())
        var record = records.first()
        var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.surname='Pluto'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        record = recordsTwo.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta), JSONUtils.readValue(recordsTwo.first().key()))

        db.execute("MATCH (p:Person {name:'Pippo'}) DETACH DELETE p")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertNotNull(recordsThree.first().value())
    }

    @Test
    fun nodeWithConstraintAndTopicDelete() {
        val topic = UUID.randomUUID().toString()

        initDbWithPolicy(
                TopicConfig.CLEANUP_POLICY_DELETE,
                mapOf("streams.source.topic.nodes.${topic}" to "Person{*}"),
                listOf("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
        )
        kafkaConsumer.subscribe(listOf(topic))

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, records.count())
        var record = records.first()
        var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (p:Person {name:'Pippo'}) SET p.name='Pluto'")
        val recordsTwo = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsTwo.count())
        record = records.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (p:Person {name:'Pluto'}) DETACH DELETE p")
        val recordsThree = kafkaConsumer.poll(Duration.ofSeconds(10))
        assertEquals(1, recordsThree.count())
        assertNotNull(recordsThree.first().value())
    }

    @Test
    fun relationWithNodesWithNoConstraintAndTopicDelete() {
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        initDbWithPolicy(
                TopicConfig.CLEANUP_POLICY_DELETE,
                mapOf("streams.source.topic.nodes.${topic[0]}" to "Person{*}",
                        "streams.source.topic.relationships.${topic[1]}" to "BUYS{*}",
                        "streams.source.topic.nodes.${topic[2]}" to "Product{*}"
                ),
        )
        kafkaConsumer.subscribe(topic)

        db.execute("CREATE (:Person {name:'Pippo'})")
        val records = kafkaConsumer.poll(10000)
        assertEquals(1, records.count())
        var record = records.first()
        var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta), JSONUtils.readValue(record.key()))

        db.execute("CREATE (p:Product {name:'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(10000)
        assertEquals(1, recordsTwo.count())
        record = recordsTwo.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(10000)
        assertEquals(1, recordsThree.count())
        record = recordsTwo.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta),
                JSONUtils.readValue(record.key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(10000)
        assertEquals(1, recordsFour.count())
        record = recordsFour.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta),
                JSONUtils.readValue(record.key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(10000)
        assertEquals(1, recordsFive.count())
        assertNotNull(recordsFive.first().value())
    }

    @Test
    fun relationWithNodesWithConstraintAndTopicDelete() {
        val topic = listOf(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
        initDbWithPolicy(
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
        val records = kafkaConsumer.poll(10000)
        assertEquals(1, records.count())
        var record = records.first()
        var meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta), JSONUtils.readValue(record.key()))

        db.execute("CREATE (p:Product {code:'1367', name: 'Notebook'})")
        val recordsTwo = kafkaConsumer.poll(10000)
        assertEquals(1, recordsTwo.count())
        record = recordsTwo.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (pe:Person {name:'Pippo'}), (pr:Product {name:'Notebook'}) MERGE (pe)-[:BUYS]->(pr)")
        val recordsThree = kafkaConsumer.poll(10000)
        assertEquals(1, recordsThree.count())
        record = recordsThree.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) SET rel.price = '100'")
        val recordsFour = kafkaConsumer.poll(10000)
        assertEquals(1, recordsFour.count())
        record = recordsFour.first()
        meta = JSONUtils.asStreamsTransactionEvent(record.value()).meta
        assertEquals(stringStrategyDelete(meta), JSONUtils.readValue(record.key()))

        db.execute("MATCH (:Person {name:'Pippo'})-[rel:BUYS]->(:Product {name:'Notebook'}) DELETE rel")
        val recordsFive = kafkaConsumer.poll(10000)
        assertEquals(1, recordsFive.count())
        assertNotNull(recordsFive.first().value())
    }
}