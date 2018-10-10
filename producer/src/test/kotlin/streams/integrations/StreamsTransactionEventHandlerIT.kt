package streams.integrations

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestGraphDatabaseFactory
import streams.StreamsTransactionEventHandler
import streams.events.NodeChange
import streams.events.OperationType
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

    @Test fun testSequence(){
        db!!.execute("CREATE (:Person {name:'Omar', age: 30}), (:Person {name:'Andrea', age: 31})")

        assertEquals(2,router.events.size)
        assertEquals(OperationType.created,router.events[0].meta.operation)
        assertEquals(OperationType.created,router.events[1].meta.operation)
        assertEquals(2,router.events[0].meta.txEventsCount)
        assertEquals(2,router.events[1].meta.txEventsCount)
        assertEquals(0,router.events[0].meta.txEventId)
        assertEquals(1,router.events[1].meta.txEventId)

        router.reset()

        db!!.execute("MATCH (o:Person {name:'Omar'}), (a:Person {name:'Andrea'}) " +
                "SET o:Test " +
                "REMOVE o:Person " +
                "SET o.age = 31 " +
                "SET o.surname = 'unknown' " +
                "REMOVE o.name " +
                "SET a:Marked ")

        assertEquals(2,router.events.size)
        assertEquals(OperationType.updated,router.events[0].meta.operation)
        assertEquals(OperationType.updated,router.events[1].meta.operation)
        assertEquals(2,router.events[0].meta.txEventsCount)
        assertEquals(2,router.events[1].meta.txEventsCount)
        assertEquals(0,router.events[0].meta.txEventId)
        assertEquals(1,router.events[1].meta.txEventId)


        router.reset()

        db!!.execute("MATCH (o:Marked) DELETE o ")

        assertEquals(1,router.events.size)
        assertEquals(OperationType.deleted,router.events[0].meta.operation)
        val before : NodeChange = router.events[0].payload.before as NodeChange
        assertEquals(listOf("Person","Marked") , before.labels)
        assertEquals(mapOf("name" to "Andrea", "age" to 31L) , before.properties)

        assertEquals(1,router.events[0].meta.txEventsCount)
        assertEquals(0,router.events[0].meta.txEventId)
    }

}
