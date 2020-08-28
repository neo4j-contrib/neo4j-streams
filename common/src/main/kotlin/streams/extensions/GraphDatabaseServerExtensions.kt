package streams.extensions

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result
import streams.utils.StreamsUtils

fun GraphDatabaseService.execute(cypher: String) = this.execute(cypher, emptyMap())
fun GraphDatabaseService.execute(cypher: String, params: Map<String, Any>) = this.executeTransactionally(cypher, params)

fun <T> GraphDatabaseService.execute(cypher: String, lambda: ((Result) -> T)) = this.execute(cypher, emptyMap(), lambda)
fun <T> GraphDatabaseService.execute(cypher: String,
                                     params: Map<String, Any>,
                                     lambda: ((Result) -> T)) = this.executeTransactionally(cypher, params, lambda)

fun GraphDatabaseService.isSystemDb() = this.databaseName() == StreamsUtils.SYSTEM_DATABASE_NAME