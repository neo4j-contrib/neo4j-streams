package streams.utils

import org.neo4j.collection.RawIterator
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.exceptions.UnsatisfiedDependencyException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.logging.internal.LogService
import streams.extensions.execute
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType


object ProcedureUtils {
    @JvmStatic
    private val raftMachineClass: Class<*>? = try {
        Class.forName("com.neo4j.causalclustering.core.consensus.RaftMachine")
    } catch (e: ClassNotFoundException) {
        null
    }

    @JvmStatic
    private val isLeaderMethodHandle = raftMachineClass?.let {
        val lookup = MethodHandles.lookup()
        lookup.findVirtual(it, "isLeader", MethodType.methodType(Boolean::class.java))
            .asType(MethodType.methodType(Boolean::class.java, Any::class.java))
    }

    fun clusterMemberRole(db: GraphDatabaseAPI): String {
        val fallback: (Exception?) -> String = { e: Exception? ->
            val userLog = db.dependencyResolver
                .resolveDependency(LogService::class.java)
                .getUserLog(ProcedureUtils::class.java)
            e?.let { userLog.warn("Cannot call the APIs, trying with the cypher query", e) }
                ?: userLog.warn("Cannot call the APIs, trying with the cypher query")
            db.execute("CALL dbms.cluster.role(\$database)",
                mapOf("database" to db.databaseName())
            ) { it.columnAs<String>("role").next() }
        }
        val execute = {
            raftMachineClass?.let {
                try {
                    val raftMachine: Any = db.dependencyResolver.resolveDependency(raftMachineClass)
                    val isLeader = isLeaderMethodHandle!!.invokeExact(raftMachine) as Boolean
                    if (isLeader) "LEADER" else "FOLLOWER"
                } catch (e: UnsatisfiedDependencyException) {
                    "LEADER"
                }
            } ?: "LEADER"
        }
        return executeOrFallback(execute, fallback)
    }

    fun isCluster(db: GraphDatabaseAPI): Boolean = try {
        db.dependencyResolver.resolveDependency(raftMachineClass)
        true
    } catch (e: UnsatisfiedDependencyException) {
        false
    }

    fun isCluster(dbms: DatabaseManagementService): Boolean = dbms.listDatabases()
        .firstOrNull { it != StreamsUtils.SYSTEM_DATABASE_NAME }
        ?.let { dbms.database(it) as GraphDatabaseAPI }
        ?.let { isCluster(it) } ?: false

    private fun <T> executeOrFallback(execute: () -> T, fallback: (Exception?) -> T): T = try {
        execute()
    } catch (e: Exception) {
        fallback(e)
    }
}