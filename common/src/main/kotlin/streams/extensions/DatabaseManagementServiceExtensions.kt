package streams.extensions

import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.utils.Neo4jUtils

fun DatabaseManagementService.getSystemDb() = this.database(Neo4jUtils.SYSTEM_DATABASE_NAME) as GraphDatabaseAPI