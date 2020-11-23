package streams.kafka.connect.sink

import org.junit.*
import org.neo4j.driver.*
import org.neo4j.driver.exceptions.ClientException
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Neo4j4ForAuraTest {

    companion object {
        private val user: String? = System.getenv("AURA_USER")
        private val password: String? = System.getenv("AURA_PASSWORD")
        private val uri: String? = System.getenv("AURA_URI")
        private var driver: Driver? = null

        private const val SHOW_CURRENT_USER = "SHOW CURRENT USER"
        private const val DBMS_LIST_CONFIG = "CALL dbms.listConfig"
        private const val NEO4J = "neo4j"
        private const val SYSTEM = "system"
        private const val ERROR_ADMIN_COMMAND = "Executing admin procedure is not allowed for user '$NEO4J' with roles [PUBLIC] overridden by READ restricted to ACCESS."

        @BeforeClass
        @JvmStatic
        fun setUp() {
            assertTrue { user != null }
            assertTrue { password != null }
            driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            driver?.close()
        }
    }

    @Test
    fun `neo4j user should not have the admin role`() {

        driver?.session(SessionConfig.forDatabase(SYSTEM)).use { session ->
            session?.run(SHOW_CURRENT_USER).let {
                assertTrue { it!!.hasNext() }
                val roles = it!!.next().get("roles").asList()
                assertFalse { roles.contains("admin") }
                assertTrue { roles.contains("PUBLIC") }
                assertFalse { it.hasNext() }
            }
        }
    }

    @Test
    fun `should fail if I try to run SHOW CURRENT USER commands on neo4j database`() {

        assertFailsWith(ClientException::class,
                "This is an administration command and it should be executed against the system database: $SHOW_CURRENT_USER")
        {
            driver?.session(SessionConfig.forDatabase(NEO4J)).use {
                it?.run(SHOW_CURRENT_USER)
            }
        }
    }

    @Test
    fun `should fail if I try to run admin commands with neo4j user `() {

        assertFailsWith(ClientException::class, ERROR_ADMIN_COMMAND)
        {
            driver?.session(SessionConfig.forDatabase(SYSTEM)).use {
                it?.run(DBMS_LIST_CONFIG)
            }
        }

        assertFailsWith(ClientException::class, ERROR_ADMIN_COMMAND)
        {
            driver?.session(SessionConfig.forDatabase(NEO4J)).use {
                it?.run(DBMS_LIST_CONFIG)
            }
        }
    }
}
