package streams.utils

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.internal.LogService
import org.neo4j.logging.Log
import streams.StreamsEventSinkAvailabilityListener
import java.lang.reflect.InvocationTargetException
import kotlin.streams.toList

object Neo4jUtils {
    @JvmStatic val LEADER = "LEADER"
    fun isWriteableInstance(db: GraphDatabaseAPI, isCluster: Boolean = false): Boolean {
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

            val available = StreamsEventSinkAvailabilityListener.isAvailable(db)
            return if (isCluster && available) {
                val role = db.execute("CALL dbms.cluster.role() YIELD role RETURN role").use {
                    it.columnAs<String>("role").next()
                }
                role.equals(LEADER, ignoreCase = true)
            } else {
                available
            }
        } catch (e: QueryExecutionException) {
            if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
                return true
            }
            throw e
        }
    }

    fun getLogService(db: GraphDatabaseAPI): LogService {
        return db.dependencyResolver
                .resolveDependency(LogService::class.java)
    }

    fun isCluster(db: GraphDatabaseAPI, log: Log? = null): Boolean {
        try {
            return db.execute("CALL dbms.cluster.overview()").use {
                if (it.hasNext()) {
                    if (log?.isDebugEnabled == true) {
                        log?.debug(it.resultAsString())
                    }
                }
                true
            }
        } catch (e: QueryExecutionException) {
            if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
                return false
            }
            throw e
        }
    }

    fun hasApoc(db: GraphDatabaseAPI): Boolean {
        return try {
            db.execute("RETURN apoc.version() AS version").use {
                it.columnAs<String>("version").next()
            }
            true
        } catch (e: QueryExecutionException) {
            false
        }
    }

    fun clusterHasLeader(db: GraphDatabaseAPI): Boolean {
        try {
            return db.execute("""
                CALL dbms.cluster.overview() YIELD role
                RETURN role
            """.trimIndent()).use {
                it.columnAs<String>("role")
                    .stream()
                    .toList()
                    .contains(LEADER)
            }
        } catch (e: QueryExecutionException) {
            if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
                return false
            }
            throw e
        }
    }

    fun <T> executeInWriteableInstance(db: GraphDatabaseAPI, action: () -> T?): T? {
        return if (isWriteableInstance(db)) {
            action()
        } else {
            null
        }
    }

    fun executeInLeader(db: GraphDatabaseAPI, log: Log, timeout: Long = 120000, action: () -> Unit) {
        GlobalScope.launch(Dispatchers.IO) {
            val start = System.currentTimeMillis()
            val delay: Long = 2000
            while (!clusterHasLeader(db) && System.currentTimeMillis() - start < timeout) {
                log.info("$LEADER not found, new check comes in $delay milliseconds...")
                delay(delay)
            }
            executeInWriteableInstance(db, action)
        }
    }

    fun waitForTheLeader(db: GraphDatabaseAPI, log: Log, timeout: Long = 120000, action: () -> Unit) {
        GlobalScope.launch(Dispatchers.IO) {
            val start = System.currentTimeMillis()
            val delay: Long = 2000
            while (!clusterHasLeader(db) && System.currentTimeMillis() - start < timeout) {
                log.info("$LEADER not found, new check comes in $delay milliseconds...")
                delay(delay)
            }
            action()
        }
    }

}