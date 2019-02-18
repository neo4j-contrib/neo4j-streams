package streams.kafka.connect.sink

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkConnector
import org.junit.Test
import org.neo4j.driver.internal.async.pool.PoolSettings
import org.neo4j.driver.v1.Config
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

class Neo4jSinkConnectorConfigTest {

   @Test(expected = ConfigException::class)
   fun `should throw a ConfigException because of mismatch`() {
       try {
           val originals = mapOf(SinkConnector.TOPICS_CONFIG to "foo, bar",
                   "${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}foo" to "CREATE (p:Person{name: event.firsName})")
           Neo4jSinkConnectorConfig(originals)
       } catch (e: ConfigException) {
           assertEquals("There is a mismatch between provided Cypher queries ([foo]) and configured topics ([foo, bar])", e.message)
           throw e
       }
   }

    @Test(expected = ConfigException::class)
    fun `should throw a ConfigException because of cross defined topics`() {
        try {
            val originals = mapOf(SinkConnector.TOPICS_CONFIG to "foo, bar",
                    "${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}foo" to "CREATE (p:Person{name: event.firsName})",
                    "${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}bar" to "CREATE (p:Person{name: event.firsName})",
                    "${Neo4jSinkConnectorConfig.TOPIC_CDC_MERGE}" to "foo")
            Neo4jSinkConnectorConfig(originals)
        } catch (e: ConfigException) {
            assertEquals("The following topics are cross defined between Cypher template configuration and CDC configuration: [foo]", e.message)
            throw e
        }
    }

    @Test
    fun `should return the configuration`() {
        val originals = mapOf(SinkConnector.TOPICS_CONFIG to "foo",
                "${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}foo" to "CREATE (p:Person{name: event.firsName})",
                Neo4jSinkConnectorConfig.SERVER_URI to "bolt://neo4j:7687",
                Neo4jSinkConnectorConfig.BATCH_SIZE to 10,
                Neo4jSinkConnectorConfig.AUTHENTICATION_BASIC_USERNAME to "FOO",
                Neo4jSinkConnectorConfig.AUTHENTICATION_BASIC_PASSWORD to "BAR")
        val config = Neo4jSinkConnectorConfig(originals)

        assertEquals(originals["${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}foo"], config.cypherTopics["foo"])
        assertFalse { config.encryptionEnabled }
        assertEquals(originals[Neo4jSinkConnectorConfig.SERVER_URI], config.serverUri.toString())
        assertEquals(originals[Neo4jSinkConnectorConfig.BATCH_SIZE], config.batchSize)
        assertEquals(Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES, config.encryptionTrustStrategy)
        assertEquals(AuthenticationType.BASIC, config.authenticationType)
        assertEquals(originals[Neo4jSinkConnectorConfig.AUTHENTICATION_BASIC_USERNAME], config.authenticationUsername)
        assertEquals(originals[Neo4jSinkConnectorConfig.AUTHENTICATION_BASIC_PASSWORD], config.authenticationPassword)
        assertEquals(originals[Neo4jSinkConnectorConfig.AUTHENTICATION_BASIC_PASSWORD], config.authenticationPassword)
        assertEquals("", config.authenticationKerberosTicket)
        assertNull(config.encryptionCACertificateFile, "encryptionCACertificateFile should be null")

        assertEquals(PoolSettings.DEFAULT_MAX_CONNECTION_LIFETIME, config.connectionMaxConnectionLifetime)
        assertEquals(PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT, config.connectionLifenessCheckTimeout)
        assertEquals(Neo4jSinkConnectorConfig.CONNECTION_POOL_MAX_SIZE_DEFAULT, config.connectionPoolMaxSize)
        assertEquals(PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT, config.connectionAcquisitionTimeout)
        assertEquals(Config.LoadBalancingStrategy.LEAST_CONNECTED, config.loadBalancingStrategy)
        assertEquals(Neo4jSinkConnectorConfig.BATCH_TIMEOUT_DEFAULT, config.batchTimeout)
    }

}