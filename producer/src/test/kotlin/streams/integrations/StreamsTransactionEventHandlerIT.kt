package streams.integrations

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestGraphDatabaseFactory
import streams.StreamsTransactionEventHandler
import streams.mocks.MockStreamsEventRouter
import kotlin.test.assertEquals


class StreamsTransactionEventHandlerIT {

    var db: GraphDatabaseService? = null
    val router : MockStreamsEventRouter = MockStreamsEventRouter()

    @Before
    fun setUp() {
        router.reset()
        db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase()
        db!!.registerTransactionEventHandler(StreamsTransactionEventHandler(router))
    }

    @After
    fun tearDown() {
        db?.shutdown()
    }

    @Test fun testBeforeCommit(){
        db!!.execute("CREATE (:Person {name:'Omar', age: 30}), (:Person {name:'Andrea', age: 31})")
        router.reset()

        db!!.execute("MATCH (o:Person {name:'Omar'}), (a:Person {name:'Andrea'}) " +
                "SET o:Test " +
                "REMOVE o:Person " +
                "SET o.age = 31 " +
                "SET o.surname = 'unknown' " +
                "REMOVE o.name " +
                "SET a:Marked ")

        assertEquals(2,router.events.size)
    }

}
