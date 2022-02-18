package streams.utils

import org.junit.Test
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Neo4jUtilsTest {

    companion object {
        @JvmField
        val db = TestDatabaseManagementServiceBuilder()
                .impermanent()
                .build()
                .database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME) as GraphDatabaseAPI
    }

    @Test
    fun shouldCheckIfIsWriteableInstance() {
        val isWriteableInstance = Neo4jUtils.isWriteableInstance(db)
        assertTrue { isWriteableInstance }
    }

    @Test
    fun `should not have APOC`() {
        assertFalse { Neo4jUtils.hasApoc(db) }
    }

}