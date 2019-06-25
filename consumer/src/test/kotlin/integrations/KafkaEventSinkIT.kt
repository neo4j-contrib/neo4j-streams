package integrations

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.hamcrest.Matchers.equalTo
import org.junit.*
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.factory.GraphDatabaseBuilder
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.test.assertion.Assert.assertEventually
import org.testcontainers.containers.KafkaContainer
import streams.events.*
import streams.serialization.JSONUtils
import streams.utils.StreamsUtils
import java.util.*
import java.util.concurrent.TimeUnit

@Suppress("UNCHECKED_CAST", "DEPRECATION")
class KafkaEventSinkIT {
    companion object {
        /**
         * Kafka TestContainers uses Confluent OSS images.
         * We need to keep in mind which is the right Confluent Platform version for the Kafka version this project uses
         *
         * Confluent Platform | Apache Kafka
         *                    |
         * 4.0.x	          | 1.0.x
         * 4.1.x	          | 1.1.x
         * 5.0.x	          | 2.0.x
         *
         * Please see also https://docs.confluent.io/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility
         */
        private const val confluentPlatformVersion = "4.0.2"
        @JvmStatic
        lateinit var kafka: KafkaContainer

        @BeforeClass @JvmStatic
        fun setUpContainer() {
            var exists = false
            StreamsUtils.ignoreExceptions({
                kafka = KafkaContainer(confluentPlatformVersion)
                kafka.start()
                exists = true
            }, IllegalStateException::class.java)
            Assume.assumeTrue("Kafka container has to exist", exists)
            Assume.assumeTrue("Kafka must be running", kafka.isRunning)
        }

        @AfterClass @JvmStatic
        fun tearDownContainer() {
            StreamsUtils.ignoreExceptions({
                kafka.stop()
            }, UninitializedPropertyAccessException::class.java)
        }
    }

    private lateinit var graphDatabaseBuilder: GraphDatabaseBuilder
    private lateinit var db: GraphDatabaseAPI

    private val cypherQueryTemplate = "MERGE (n:Label {id: event.id}) ON CREATE SET n += event.properties"

    private val topics = listOf("shouldWriteCypherQuery")

    private val kafkaProperties = Properties()

    private lateinit var kafkaProducer: KafkaProducer<String, ByteArray>

    // Test data
    private val dataProperties = mapOf("prop1" to "foo", "bar" to 1)
    private val data = mapOf("id" to 1, "properties" to dataProperties)

    @Before
    fun setUp() {
        graphDatabaseBuilder = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", kafka.bootstrapServers)
                .setConfig("streams.sink.enabled", "true")

        kafkaProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        kafkaProperties["zookeeper.connect"] = kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"]
        kafkaProperties["group.id"] = "neo4j"
        kafkaProperties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        kafkaProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java

        kafkaProducer = KafkaProducer(kafkaProperties)
    }

    @After
    fun tearDown() {
        db.shutdown()
        kafkaProducer.close()
    }

    @Test
    fun shouldWriteDataFromSink() = runBlocking {
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI

        val producerRecord = ProducerRecord(topics[0], UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val props = data
                .flatMap {
                    if (it.key == "properties") {
                        val map = it.value as Map<String, Any>
                        map.entries.map { it.key to it.value }
                    } else {
                        listOf(it.key to it.value)
                    }
                }
                .toMap()

        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH (n:Label) WHERE properties(n) = {props}
                |RETURN count(*) AS count""".trimMargin()
            val result = db.execute(query, mapOf("props" to props)).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, equalTo(true), 30, TimeUnit.SECONDS)

    }

    @Test
    fun shouldNotWriteDataFromSinkWithNoTopicLoaded() = runBlocking {
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI

        val producerRecord = ProducerRecord(topics[0], UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        delay(5000)

        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH (n:Label)
                |RETURN n""".trimMargin()
            val result = db.execute(query).columnAs<Node>("n")
            result.hasNext()
        }, equalTo(false), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldWriteDataFromSinkWithCDCTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.sourceId", topic)
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.sourceId.idName", "customIdN@me")
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.sourceId.labelName", "CustomLabelN@me")
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI

        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                    username = "user",
                    txId = 1,
                    txEventId = 0,
                    txEventsCount = 3,
                    operation = OperationType.created
                ),
                payload = NodePayload(id = "0",
                    before = null,
                    after = NodeChange(properties = mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = Schema()
        )
        val cdcDataEnd = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                    username = "user",
                    txId = 1,
                    txEventId = 1,
                    txEventsCount = 3,
                    operation = OperationType.created
                ),
                payload = NodePayload(id = "1",
                    before = null,
                    after = NodeChange(properties = mapOf("name" to "Michael", "comp@ny" to "Neo4j"), labels = listOf("User Ext"))
                ),
                schema = Schema()
        )
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                    username = "user",
                    txId = 1,
                    txEventId = 2,
                    txEventsCount = 3,
                    operation = OperationType.created
                ),
                payload = RelationshipPayload(
                    id = "3",
                    start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                    end = RelationshipNodeChange(id = "1", labels = listOf("User Ext"), ids = emptyMap()),
                    after = RelationshipChange(properties = mapOf("since" to 2014)),
                    before = null,
                    label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataEnd))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataRelationship))
        kafkaProducer.send(producerRecord).get()

        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User:`CustomLabelN@me`{name:'Andrea', `comp@ny`:'LARUS-BA', `customIdN@me`: '0'})-[r:`KNOWS WHO`{since:2014, `customIdN@me`: '3'}]->(e:`User Ext`:`CustomLabelN@me`{name:'Michael', `comp@ny`:'Neo4j', `customIdN@me`: '1'})
                |RETURN count(p) AS count""".trimMargin()
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, equalTo(true), 30, TimeUnit.SECONDS)

    }

    @Test
    fun shouldWriteDataFromSinkWithCDCSchemaTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.schema", topic)
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI

        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "comp@ny" to "String"),
                constraints = listOf(Constraint(label = "User", type =  StreamsConstraintType.UNIQUE, properties = setOf("name", "surname"))))
        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 1,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )
        val cdcDataRelationship = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 2,
                txEventsCount = 3,
                operation = OperationType.created
        ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = mapOf("name" to "Andrea", "surname" to "Santurbano")),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = mapOf("name" to "Michael", "surname" to "Hunger")),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataEnd))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataRelationship))
        kafkaProducer.send(producerRecord).get()

        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})
                |RETURN count(p) AS count
                |""".trimMargin()
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldDeleteDataFromSinkWithCDCSchemaTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cdc.schema", topic)
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI

        db.execute("CREATE (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})").close()
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "comp@ny" to "String"),
                constraints = listOf(Constraint(label = "User", type =  StreamsConstraintType.UNIQUE, properties = setOf("name", "surname"))))
        val cdcDataStart = StreamsTransactionEvent(meta = Meta(timestamp = System.currentTimeMillis(),
                username = "user",
                txId = 1,
                txEventId = 0,
                txEventsCount = 3,
                operation = OperationType.deleted
        ),
                payload = NodePayload(id = "0",
                        after = null,
                        before = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )

        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()

        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})
                |RETURN count(p) AS count
                |""".trimMargin()
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 0L && !result.hasNext()
        }, equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldWriteLastOffsetWithNoAutoCommit() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
        graphDatabaseBuilder.setConfig("kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}", "false")
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        val partition = 0
        var producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val newData = data.toMutableMap()
        newData["id"] = 2
        producerRecord = ProducerRecord(topic, partition, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(newData))
        val resp = kafkaProducer.send(producerRecord).get()

        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = "MATCH (n:Label) RETURN count(*) AS count"
            val result = db.execute(query).columnAs<Long>("count")

            val kafkaConsumer = createConsumer<String, ByteArray>()
            val offsetAndMetadata = kafkaConsumer.committed(TopicPartition(topic, partition))
            kafkaConsumer.close()

            result.hasNext() && result.next() == 2L && !result.hasNext() && resp.offset() + 1 == offsetAndMetadata.offset()
        }, equalTo(true), 30, TimeUnit.SECONDS)

    }

    @Test
    fun shouldWorkWithNodePatternTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.pattern.node.$topic",
                "(:User{!userId,name,surname,address.city})")
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI


        val data = mapOf("userId" to 1, "name" to "Andrea", "surname" to "Santurbano",
                "address" to mapOf("city" to "Venice", "CAP" to "30100"))

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = "MATCH (n:User{name: 'Andrea', surname: 'Santurbano', userId: 1, `address.city`: 'Venice'}) RETURN count(n) AS count"
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldWorkWithRelPatternTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.pattern.relationship.$topic",
                "(:User{!sourceId,sourceName,sourceSurname})-[:KNOWS]->(:User{!targetId,targetName,targetSurname})")
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        val data = mapOf("sourceId" to 1, "sourceName" to "Andrea", "sourceSurname" to "Santurbano",
                "targetId" to 1, "targetName" to "Michael", "targetSurname" to "Hunger", "since" to 2014)

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH p = (s:User{sourceName: 'Andrea', sourceSurname: 'Santurbano', sourceId: 1})-[:KNOWS{since: 2014}]->(e:User{targetName: 'Michael', targetSurname: 'Hunger', targetId: 1})
                RETURN count(p) AS count
            """.trimIndent()
            val result = db.execute(query).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should fix issue 186 with auto commit false`() = runBlocking {
        val product = "product" to "MERGE (p:Product {id: event.id}) ON CREATE SET p.name = event.name"
        val customer = "customer" to "MERGE (c:Customer {id: event.id}) ON CREATE SET c.name = event.name"
        val bought = "bought" to """
            MERGE (c:Customer {id: event.id})
            MERGE (p:Product {id: event.id})
            MERGE (c)-[:BOUGHT]->(p)
        """.trimIndent()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.${product.first}", product.second)
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.${customer.first}", customer.second)
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.${bought.first}", bought.second)
        graphDatabaseBuilder.setConfig("kafka.${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG}", "false")
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI

        val props = mapOf("id" to 1, "name" to "My Awesome Product")
        var producerRecord = ProducerRecord(product.first, UUID.randomUUID().toString(),
                JSONUtils.writeValueAsBytes(props))
        kafkaProducer.send(producerRecord).get()
        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH (p:Product)
                WHERE properties(p) = {props}
                RETURN count(p) AS count
            """.trimIndent()
            val result = db.execute(query, mapOf("props" to props)).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should fix issue 186 with auto commit true`() {
        val product = "product" to "MERGE (p:Product {id: event.id}) ON CREATE SET p.name = event.name"
        val customer = "customer" to "MERGE (c:Customer {id: event.id}) ON CREATE SET c.name = event.name"
        val bought = "bought" to """
            MERGE (c:Customer {id: event.id})
            MERGE (p:Product {id: event.id})
            MERGE (c)-[:BOUGHT]->(p)
        """.trimIndent()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.${product.first}", product.second)
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.${customer.first}", customer.second)
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.${bought.first}", bought.second)
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI

        val props = mapOf("id" to 1, "name" to "My Awesome Product")
        var producerRecord = ProducerRecord(product.first, UUID.randomUUID().toString(),
                JSONUtils.writeValueAsBytes(props))
        kafkaProducer.send(producerRecord).get()
        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH (p:Product)
                WHERE properties(p) = {props}
                RETURN count(p) AS count
            """.trimIndent()
            val result = db.execute(query, mapOf("props" to props)).columnAs<Long>("count")
            result.hasNext() && result.next() == 1L && !result.hasNext()
        }, equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should send data to the DLQ because of QueryExecutionException`() {
        val topic = UUID.randomUUID().toString()
        val dlqTopic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.$topic","MERGE (c:Customer {id: event.id})")
        graphDatabaseBuilder.setConfig("streams.sink.dlq", dlqTopic)
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        val data = mapOf("id" to null, "name" to "Andrea", "surname" to "Santurbano")

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH (c:Customer)
                RETURN count(c) AS count
            """.trimIndent()
            val result = db.execute(query).columnAs<Long>("count")

            val dlqConsumer = createConsumer<ByteArray, ByteArray>(ByteArrayDeserializer::class.java.name,
                    ByteArrayDeserializer::class.java.name,
                    dlqTopic)
            val records = dlqConsumer.poll(5000)
            val record = if (records.isEmpty) null else records.records(dlqTopic).iterator().next()
            val headers = record?.headers()?.map { it.key() to String(it.value()) }?.toMap().orEmpty()
            val value = if (record != null) JSONUtils.readValue<Any>(record.value()!!) else emptyMap<String, Any>()
            dlqConsumer.close()
            !records.isEmpty && headers.size == 7 && value == data && result.hasNext() && result.next() == 0L && !result.hasNext()
                    && headers["__streams.errorsexception.class.name"] == "org.neo4j.graphdb.QueryExecutionException"
        }, equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun `should send data to the DLQ because of JsonParseException`() {
        val topic = UUID.randomUUID().toString()
        val dlqTopic = UUID.randomUUID().toString()
        graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.$topic","MERGE (c:Customer {id: event.id})")
        graphDatabaseBuilder.setConfig("streams.sink.dlq", dlqTopic)
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI

        val data = """{id: 1, "name": "Andrea", "surname": "Santurbano"}"""

        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(),
                data.toByteArray())
        kafkaProducer.send(producerRecord).get()
        assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                MATCH (c:Customer)
                RETURN count(c) AS count
            """.trimIndent()
            val result = db.execute(query).columnAs<Long>("count")

            val dlqConsumer = createConsumer<ByteArray, ByteArray>(ByteArrayDeserializer::class.java.name,
                    ByteArrayDeserializer::class.java.name,
                    dlqTopic)
            val records = dlqConsumer.poll(5000)
            val record = if (records.isEmpty) null else records.records(dlqTopic).iterator().next()
            val headers = record?.headers()?.map { it.key() to String(it.value()) }?.toMap().orEmpty()
            val value = if (record != null) String(record.value()) else emptyMap<String, Any>()
            dlqConsumer.close()
            !records.isEmpty && headers.size == 7 && data == value && result.hasNext() && result.next() == 0L && !result.hasNext()
                    && headers["__streams.errorsexception.class.name"] == "com.fasterxml.jackson.core.JsonParseException"
        }, equalTo(true), 30, TimeUnit.SECONDS)
    }

    private fun <K, V> createConsumer(keyDeserializer: String = StringDeserializer::class.java.name,
                               valueDeserializer: String = ByteArrayDeserializer::class.java.name,
                               vararg topics: String): KafkaConsumer<K, V> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        props["zookeeper.connect"] = kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"]
        props["group.id"] = "neo4j"
        props["enable.auto.commit"] = "true"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer
        props["auto.offset.reset"] = "earliest"
        val consumer = KafkaConsumer<K, V>(props)
        if (!topics.isNullOrEmpty()) {
            consumer.subscribe(topics.toList())
        }
        return consumer
    }

}