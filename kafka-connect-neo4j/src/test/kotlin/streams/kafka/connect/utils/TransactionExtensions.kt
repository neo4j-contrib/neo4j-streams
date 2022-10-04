package streams.kafka.connect.utils

import org.neo4j.driver.Result
import org.neo4j.driver.Transaction
import org.neo4j.driver.exceptions.NoSuchRecordException
import org.neo4j.driver.types.Node
import org.neo4j.graphdb.Label

fun Transaction.findNodes(label: Label): Result = this.run("""
        MATCH (n:`${label.name()}`)
        RETURN n
    """.trimIndent())

fun Transaction.findNode(label: Label, key: String, value: Any): Node? = try {
    this.run("""
        MATCH (n:`${label.name()}`{`$key`: ${'$'}value})
        RETURN n
    """.trimIndent(), mapOf("value" to value))
        .single()[0]
        .asNode()
} catch (e: NoSuchRecordException) {
    null
}

fun Transaction.allRelationships(): Result = this.run("""
        MATCH ()-[r]->()
        RETURN r
    """.trimIndent())

fun Transaction.allNodes(): Result = this.run("""
        MATCH (n)
        RETURN n
    """.trimIndent())

fun Transaction.allLabels(): List<Label> = this.run("""
        CALL db.labels() YIELD label
        RETURN label
    """.trimIndent())
    .list()
    .map { Label.label(it["label"].asString()) }