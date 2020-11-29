package streams.utils

import extension.newDatabase
import org.junit.*
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import streams.StreamsEventSinkAvailabilityListener
import streams.events.StreamsPluginStatus
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Neo4jUtilsTest {

    companion object {
        private lateinit var db: GraphDatabaseAPI
        @BeforeClass
        @JvmStatic
        fun setUp() {
            db = TestGraphDatabaseFactory()
                    .newImpermanentDatabaseBuilder()
                    .newDatabase(StreamsPluginStatus.STOPPED) as GraphDatabaseAPI
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            db.shutdown()
        }
    }

    @Test
    fun shouldCheckIfIsWriteableInstance() {
        StreamsEventSinkAvailabilityListener.setAvailable(db, true)
        val isWriteableInstance = Neo4jUtils.isWriteableInstance(db)
        assertTrue { isWriteableInstance }
    }

    @Test
    fun shouldCheckIfIsACluster() {
        val isEnterprise = Neo4jUtils.isCluster(db)
        assertFalse { isEnterprise }
    }

    @Test
    fun `should not have APOC`() {
        assertFalse { Neo4jUtils.hasApoc(db) }
    }

}