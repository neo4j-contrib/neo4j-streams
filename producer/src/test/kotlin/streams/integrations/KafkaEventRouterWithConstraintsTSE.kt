package streams.integrations

import org.junit.Before
import org.junit.Test
import streams.events.Constraint
import streams.events.EntityType
import streams.events.NodePayload
import streams.events.OperationType
import streams.events.RelationshipPayload
import streams.events.StreamsConstraintType
import streams.extensions.execute
import streams.utils.JSONUtils
import streams.setConfig
import streams.start
import kotlin.test.assertEquals

class KafkaEventRouterWithConstraintsTSE: KafkaEventRouterBaseTSE() {

    @Before
    fun setUpInner() {
        db.setConfig("streams.source.topic.nodes.personConstraints", "PersonConstr{*}")
                .setConfig("streams.source.topic.nodes.productConstraints", "ProductConstr{*}")
                .setConfig("streams.source.topic.relationships.boughtConstraints", "BOUGHT{*}")
                .setConfig("streams.source.schema.polling.interval", "0")
                .start()
        db.execute("CREATE CONSTRAINT ON (p:PersonConstr) ASSERT p.name IS UNIQUE")
        db.execute("CREATE CONSTRAINT ON (p:ProductConstr) ASSERT p.name IS UNIQUE")
    }


    @Test
    fun testCreateNodeWithConstraints() {
        kafkaConsumer.subscribe(listOf("personConstraints"))
        db.execute("CREATE (:PersonConstr {name:'Andrea'})")
        val records = kafkaConsumer.poll(5000)
        assertEquals(1, records.count())
        assertEquals(true, records.all {
            JSONUtils.asStreamsTransactionEvent(it.value()).let {
                val payload = it.payload as NodePayload
                val labels = payload.after!!.labels!!
                val properties = payload.after!!.properties
                labels == listOf("PersonConstr") && properties == mapOf("name" to "Andrea")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String")
                        && it.schema.constraints == listOf(Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE))
            }
        })
    }

    @Test
    fun testCreateRelationshipWithConstraints() {
        db.execute("CREATE (:PersonConstr {name:'Andrea'})")
        db.execute("CREATE (:ProductConstr {name:'My Awesome Product', price: '100€'})")
        db.execute("""
            |MATCH (p:PersonConstr {name:'Andrea'})
            |MATCH (pp:ProductConstr {name:'My Awesome Product'})
            |MERGE (p)-[:BOUGHT]->(pp)
        """.trimMargin())
        kafkaConsumer.subscribe(listOf("personConstraints", "productConstraints", "boughtConstraints"))
        val records = kafkaConsumer.poll(10000)
        assertEquals(3, records.count())

        val map = records
                .map {
                    val evt = JSONUtils.asStreamsTransactionEvent(it.value())
                    evt.payload.type to evt
                }
                .groupBy({ it.first }, { it.second })
        assertEquals(true, map[EntityType.node].orEmpty().isNotEmpty() && map[EntityType.node].orEmpty().all {
            val payload = it.payload as NodePayload
            val (labels, properties) = payload.after!!.labels!! to payload.after!!.properties!!
            when (labels) {
                listOf("ProductConstr") -> properties == mapOf("name" to "My Awesome Product", "price" to "100€")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String", "price" to "String")
                        && it.schema.constraints == listOf(Constraint("ProductConstr", setOf("name"), StreamsConstraintType.UNIQUE))
                listOf("PersonConstr") -> properties == mapOf("name" to "Andrea")
                        && it.meta.operation == OperationType.created
                        && it.schema.properties == mapOf("name" to "String")
                        && it.schema.constraints == listOf(Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE))
                else -> false
            }
        })
        assertEquals(true, map[EntityType.relationship].orEmpty().isNotEmpty() && map[EntityType.relationship].orEmpty().all {
            val payload = it.payload as RelationshipPayload
            val (start, end, properties) = Triple(payload.start, payload.end, payload.after!!.properties!!)
            properties.isNullOrEmpty()
                    && start.ids == mapOf("name" to "Andrea")
                    && end.ids == mapOf("name" to "My Awesome Product")
                    && it.meta.operation == OperationType.created
                    && it.schema.properties == emptyMap<String, String>()
                    && it.schema.constraints.toSet() == setOf(
                    Constraint("PersonConstr", setOf("name"), StreamsConstraintType.UNIQUE),
                    Constraint("ProductConstr", setOf("name"), StreamsConstraintType.UNIQUE))
        })
    }
}