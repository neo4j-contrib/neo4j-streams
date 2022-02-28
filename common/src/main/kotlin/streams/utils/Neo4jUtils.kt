package streams.utils

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.extensions.execute
import kotlin.streams.toList

object Neo4jUtils {
    fun isWriteableInstance(db: GraphDatabaseAPI, availableAction: () -> Boolean = { true }): Boolean {
        try {
            return availableAction() && ProcedureUtils
                    .clusterMemberRole(db)
                    .equals(StreamsUtils.LEADER, ignoreCase = true)
        } catch (e: QueryExecutionException) {
            if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
                return availableAction()
            }
            throw e
        }
    }

    fun hasApoc(db: GraphDatabaseAPI): Boolean = try {
         db.execute("RETURN apoc.version() AS version") {
            it.columnAs<String>("version").next()
            true
        }
    } catch (e: QueryExecutionException) {
        false
    }

    private fun clusterHasLeader(db: GraphDatabaseAPI): Boolean = try {
        db.execute("""
                |CALL dbms.cluster.overview() YIELD databases
                |RETURN databases[${'$'}database] AS role
            """.trimMargin(), mapOf("database" to db.databaseName())) {
            it.columnAs<String>("role")
                    .stream()
                    .toList()
                    .contains(StreamsUtils.LEADER)
        }
    } catch (e: Exception) {
        false
    }

    fun <T> executeInWriteableInstance(db: GraphDatabaseAPI,
                                       availableAction: () -> Boolean,
                                       action: () -> T?): T? = if (isWriteableInstance(db, availableAction)) {
        action()
    } else {
        null
    }

    fun waitForTheLeaders(db: GraphDatabaseAPI, log: Log, timeout: Long = 240000, onFailure: () -> Unit = {}, action: () -> Unit) {
        GlobalScope.launch(Dispatchers.IO) {
            val start = System.currentTimeMillis()
            val delay: Long = 2000
            var isClusterCorrectlyFormed: Boolean
            do {
                isClusterCorrectlyFormed = clusterHasLeader(db)
                if (!isClusterCorrectlyFormed) {
                    log.info("${StreamsUtils.LEADER} not found, new check comes in $delay milliseconds...")
                    delay(delay)
                }
            } while (!isClusterCorrectlyFormed && System.currentTimeMillis() - start < timeout)
            if (isClusterCorrectlyFormed) {
                log.debug("${StreamsUtils.LEADER} has been found")
                action()
            } else {
                log.warn("$timeout ms have passed and the ${StreamsUtils.LEADER} has not been elected, the Streams plugin will not started")
                onFailure()
            }
        }
    }

    fun waitForAvailable(db: GraphDatabaseAPI, log: Log, timeout: Long = 240000, onFailure: () -> Unit = {}, action: () -> Unit) {
        GlobalScope.launch(Dispatchers.IO) {
            val start = System.currentTimeMillis()
            val delay: Long = 2000
            var isAvailable: Boolean
            do {
                isAvailable = db.isAvailable(delay)
                if (!isAvailable) {
                    log.debug("Waiting for Neo4j to be ready...")
                }
            } while (!isAvailable && System.currentTimeMillis() - start < timeout)
            if (isAvailable) {
                log.debug("Neo4j is ready")
                action()
            } else {
                log.warn("$timeout ms have passed and Neo4j is not online, the Streams plugin will not started")
                onFailure()
            }
        }
    }

    fun isReadReplica(db: GraphDatabaseAPI): Boolean = db.dependencyResolver
        .resolveDependency(Config::class.java)
        .let { it.get(GraphDatabaseSettings.mode).name == "READ_REPLICA" }
}