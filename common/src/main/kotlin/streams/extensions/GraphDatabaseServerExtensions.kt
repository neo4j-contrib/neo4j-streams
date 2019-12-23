package streams.extensions

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result

fun GraphDatabaseService.execute(cypher: String) = this.execute(cypher, emptyMap())
fun GraphDatabaseService.execute(cypher: String, params: Map<String, Any>) = this.executeTransactionally(cypher, params)

fun <T> GraphDatabaseService.execute(cypher: String, lambda: ((Result) -> T)) = this.execute(cypher, emptyMap(), lambda)
fun <T> GraphDatabaseService.execute(cypher: String, params: Map<String, Any>, lambda: ((Result) -> T)) = this.beginTx().use {
    val resp = it.execute(cypher, params)
    it.commit()
    lambda.let { it(resp) }
}