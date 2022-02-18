package streams.utils

import org.junit.Test
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import kotlin.test.assertFalse


class ProcedureUtilsTest {

    companion object {
        @JvmField
        val db = TestDatabaseManagementServiceBuilder()
                .impermanent()
                .build()
                .database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME) as GraphDatabaseAPI
    }

    @Test
    fun shouldCheckIfIsACluster() {
        val isCluster = ProcedureUtils.isCluster(db)
        assertFalse { isCluster }
    }

}