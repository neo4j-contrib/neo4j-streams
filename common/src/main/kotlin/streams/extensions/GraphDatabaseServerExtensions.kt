package streams.extensions

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result
import streams.utils.Neo4jUtils

fun GraphDatabaseService.execute(cypher: String) = this.execute(cypher, emptyMap())
fun GraphDatabaseService.execute(cypher: String, params: Map<String, Any>) = this.executeTransactionally(cypher, params)

fun <T> GraphDatabaseService.execute(cypher: String, lambda: ((Result) -> T)) = this.execute(cypher, emptyMap(), lambda)
fun <T> GraphDatabaseService.execute(cypher: String, params: Map<String, Any>, lambda: ((Result) -> T)) = this.beginTx().use {
    val result = it.execute(cypher, params)
    val ret = lambda(result)
    it.commit()
    ret
}

fun GraphDatabaseService.isSystemDb() = this.databaseName() == Neo4jUtils.SYSTEM_DATABASE_NAME