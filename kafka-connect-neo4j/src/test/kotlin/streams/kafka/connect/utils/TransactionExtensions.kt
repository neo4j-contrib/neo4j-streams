package streams.kafka.connect.utils

import org.neo4j.driver.Result
import org.neo4j.driver.Transaction
import org.neo4j.driver.exceptions.NoSuchRecordException
import org.neo4j.driver.types.Node

fun Transaction.findNodes(label: String): Result = this.run("""
        MATCH (n:`${label}`)
        RETURN n
    """.trimIndent())

fun Transaction.findNode(label: String, key: String, value: Any): Node? = try {
    this.run("""
        MATCH (n:`${label}`{`$key`: ${'$'}value})
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

fun Transaction.allLabels(): List<String> = this.run("""
        CALL db.labels() YIELD label
        RETURN label
    """.trimIndent())
    .list()
    .map { it["label"].asString() }