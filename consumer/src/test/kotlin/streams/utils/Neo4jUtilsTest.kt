package streams.utils

import org.junit.*
import org.neo4j.kernel.impl.logging.LogService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import kotlin.test.assertTrue

class Neo4jUtilsTest {

    companion object {
        private lateinit var db: GraphDatabaseAPI
        @BeforeClass
        @JvmStatic
        fun setUp() {
            db = TestGraphDatabaseFactory()
                    .newImpermanentDatabaseBuilder()
                    .newGraphDatabase() as GraphDatabaseAPI
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            db.shutdown()
        }
    }

    @Test
    fun shouldCheckIfIsWriteableInstance() {
        val isWriteableIntance = Neo4jUtils.isWriteableInstance(db)
        assertTrue { isWriteableIntance != null && isWriteableIntance!! }
    }

    @Test
    fun shouldGetLogService() {
        val logService = Neo4jUtils.getLogService(db)
        assertTrue { logService is LogService }
    }

}