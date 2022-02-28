package streams.extensions

import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.utils.StreamsUtils

fun DatabaseManagementService.getSystemDb() = this.database(StreamsUtils.SYSTEM_DATABASE_NAME) as GraphDatabaseAPI

fun DatabaseManagementService.getDefaultDbName() = getSystemDb().let {
    try {
        it.beginTx().use {
            val col = it.execute("SHOW DEFAULT DATABASE").columnAs<String>("name")
            if (col.hasNext()) {
                col.next()
            } else {
                null
            }
        }
    } catch (e: Exception) {
        null
    }
}

fun DatabaseManagementService.getDefaultDb() = getDefaultDbName()?.let { this.database(it) as GraphDatabaseAPI }

fun DatabaseManagementService.isAvailable(timeout: Long) = this.listDatabases()
    .all { this.database(it).isAvailable(timeout) }

