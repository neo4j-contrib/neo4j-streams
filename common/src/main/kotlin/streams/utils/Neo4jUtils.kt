package streams.utils

import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.internal.LogService
import java.lang.reflect.InvocationTargetException

object Neo4jUtils {
    fun isWriteableInstance(db: GraphDatabaseAPI): Boolean {
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

            val role = db.execute("CALL dbms.cluster.role()").columnAs<String>("role").next()
            return role.equals("LEADER", ignoreCase = true)
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
}