package streams.utils

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.extensions.execute
import java.lang.reflect.InvocationTargetException
import kotlin.streams.toList

object Neo4jUtils {
    fun isWriteableInstance(db: GraphDatabaseAPI, availableAction: () -> Boolean = { true }): Boolean {
        try {
            val isSlave = StreamsUtils.ignoreExceptions(
                {
                    val hadb = Class.forName("org.neo4j.kernel.ha.HighlyAvailableGraphDatabase")
                    hadb.isInstance(db) && !(hadb.getMethod("isMaster").invoke(db) as Boolean)
                }, ClassNotFoundException::class.java, IllegalAccessException::class.java,
                InvocationTargetException::class.java, NoSuchMethodException::class.java)
            if (isSlave != null && isSlave) {
                return false
            }

            return availableAction() && ProcedureUtils.clusterMemberRole(db).equals(StreamsUtils.LEADER, ignoreCase = true)
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
    } catch (e: QueryExecutionException) {
        if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
            false
        }
        throw e
    }

    fun <T> executeInWriteableInstance(db: GraphDatabaseAPI,
                                       availableAction: () -> Boolean,
                                       action: () -> T?): T? = if (isWriteableInstance(db, availableAction)) {
        action()
    } else {
        null
    }

    fun isClusterCorrectlyFormed(dbms: DatabaseManagementService) = dbms.listDatabases()
        .filterNot { it == StreamsUtils.SYSTEM_DATABASE_NAME }
        .map { dbms.database(it) as GraphDatabaseAPI }
        .all { clusterHasLeader(it) }

    fun waitForTheLeaders(dbms: DatabaseManagementService, log: Log, timeout: Long = 120000, action: () -> Unit) {
        GlobalScope.launch(Dispatchers.IO) {
            val start = System.currentTimeMillis()
            val delay: Long = 2000
            while (!isClusterCorrectlyFormed(dbms) && System.currentTimeMillis() - start < timeout) {
                log.info("${StreamsUtils.LEADER} not found, new check comes in $delay milliseconds...")
                delay(delay)
            }
            action()
        }
    }

    fun isReadReplica(db: GraphDatabaseAPI): Boolean = db.dependencyResolver
            .resolveDependency(Config::class.java)
            .let { it.get(GraphDatabaseSettings.mode).name == "READ_REPLICA" }
}