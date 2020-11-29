package streams.configuration

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import kotlin.streams.toList
import kotlin.test.assertEquals

@Suppress("DEPRECATION")
class StreamsConfigProceduresIT {

    private lateinit var db: GraphDatabaseAPI

    @Before
    fun setUp() {
        db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase() as GraphDatabaseAPI
    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    @Test
    fun `should set properties`() {
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsConfigProcedures::class.java, true)
        val props = mapOf("streams.procedures.enabled" to "true")
        val actual = db.execute("CALL streams.configuration.set(\$props)", mapOf("props" to props))
                .stream()
                .toList()
        assertEquals(1, actual.size)
        val expected = mapOf("name" to "streams.procedures.enabled", "value" to "true")
        assertEquals(expected, actual[0])
    }
}
