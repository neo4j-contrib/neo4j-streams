package streams.extensions

import org.neo4j.configuration.Config
import org.neo4j.graphdb.GraphDatabaseService

fun GraphDatabaseService.execute(cypher: String) = this.execute(cypher, emptyMap())

fun GraphDatabaseService.execute(cypher: String, params: Map<String, Any>) = this.beginTx().use {
    val resp = it.execute(cypher, params)
    it.commit()
    resp
}

fun Config.raw() = this.values.mapNotNull {
    val value = it.value?.toString()
    if (value == null) {
        null
    } else {
        val key = it.key.name()
        key to value
    }
}.toMap()