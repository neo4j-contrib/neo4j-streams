package streams.kafka.connect.sink

import kotlinx.coroutines.async
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.whileSelect
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.exceptions.ClientException
import org.neo4j.driver.v1.exceptions.TransientException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.utils.StreamsUtils
import streams.utils.retryForException
import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.driver.v1.exceptions.Neo4jException
import java.util.concurrent.TimeUnit


class Neo4jService(private val config: Neo4jSinkConnectorConfig) {

    private val converter = ValueConverter()

    private val log: Logger = LoggerFactory.getLogger(Neo4jService::class.java)

    private val driver: Driver

    private val skipNeo4jErrors = listOf("Neo.ClientError.Statement.PropertyNotFound",
            "Neo.ClientError.Statement.SemanticError",
            "Neo.ClientError.Statement.ParameterMissing")

    init {
        val configBuilder = Config.build()
        if (this.config.encryptionEnabled) {
            configBuilder.withEncryption()
            val trustStrategy: Config.TrustStrategy = when (this.config.encryptionTrustStrategy) {
                Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES -> Config.TrustStrategy.trustAllCertificates()
                Config.TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES -> Config.TrustStrategy.trustSystemCertificates()
                Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES -> Config.TrustStrategy.trustCustomCertificateSignedBy(this.config.encryptionCACertificateFile)
                else -> {
                    throw ConfigException(Neo4jSinkConnectorConfig.ENCRYPTION_TRUST_STRATEGY, this.config.encryptionTrustStrategy.toString(), "Encryption Trust Strategy is not supported.")
                }
            }
            configBuilder.withTrustStrategy(trustStrategy)
        } else {
            configBuilder.withoutEncryption()
        }
        val authToken = when (this.config.authenticationType) {
            AuthenticationType.NONE -> AuthTokens.none()
            AuthenticationType.BASIC -> {
                if (this.config.authenticationRealm != "") {
                    AuthTokens.basic(this.config.authenticationUsername, this.config.authenticationPassword, this.config.authenticationRealm)
                } else {
                    AuthTokens.basic(this.config.authenticationUsername, this.config.authenticationPassword)
                }
            }
            AuthenticationType.KERBEROS -> AuthTokens.kerberos(this.config.authenticationKerberosTicket)
        }
        configBuilder.withMaxConnectionPoolSize(this.config.connectionPoolMaxSize)
        configBuilder.withMaxConnectionLifetime(this.config.connectionMaxConnectionLifetime, TimeUnit.MILLISECONDS)
        configBuilder.withConnectionAcquisitionTimeout(this.config.connectionAcquisitionTimeout, TimeUnit.MILLISECONDS)
        configBuilder.withLoadBalancingStrategy(this.config.loadBalancingStrategy)
        configBuilder.withMaxTransactionRetryTime(config.retryBackoff, TimeUnit.MILLISECONDS)
        val neo4jConfig = configBuilder.toConfig()
        this.driver = GraphDatabase.driver(this.config.serverUri, authToken, neo4jConfig)
    }

    fun close() {
        driver.close()
    }

    private fun write(topic: String, records: List<SinkRecord>) {
        val query = "${StreamsUtils.UNWIND} ${config.topicMap[topic]}"
        val data = mapOf<String, Any>("events" to records.map { converter.convert(it.value()) })
        driver.session().use { session ->
            try {
                runBlocking {
                    retryForException<Unit>(exceptions = arrayOf(ClientException::class.java, TransientException::class.java),
                            retries = config.retryMaxAttempts, delayTime = 0) { // we use the delayTime = 0, because we delegate the retryBackoff to the Neo4j Java Driver
                        try {
                            session.writeTransaction {
                                val result = it.run(query, data)
                                if (log.isDebugEnabled) {
                                    log.debug("Successfully executed query, summary: ${result.summary()}")
                                }
                            }
                        } catch (neoException: Neo4jException) {
                            if (skipNeo4jErrors.contains(neoException.code())) {
                                log.info("Skip query: `$query`. Error message `${neoException.message}`. Error code: `${neoException.code()}`")
                            } else {
                                throw neoException
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                if (log.isDebugEnabled) {
                    if (e is Neo4jException) {
                        log.debug("Exception `${e.message}` with code `${e.code()}` while executing query `$query`, with data `$data`")
                    } else {
                        log.debug("Exception `${e.message}` while executing query `$query`, with data `$data`")
                    }
                }
                throw e
            }
        }
    }

    suspend fun writeData(data: Map<String, List<List<SinkRecord>>>) = coroutineScope {
        val timeout = config.batchTimeout
        val ticker = ticker(timeout)
        val deferredList = data
                .flatMap { (topic, records) ->
                    records.map { async { write(topic, it) } }
                }
        whileSelect {
            ticker.onReceive {
                if (log.isDebugEnabled) {
                    log.debug("Timeout $timeout occurred while executing queries")
                }
                deferredList.forEach { deferred -> deferred.cancel() }
                false // Stops the whileSelect
            }
            val isAllCompleted = deferredList.all { it.isCompleted } // when all are completed
            deferredList.forEach {
                it.onAwait { !isAllCompleted } // Stops the whileSelect
            }
        }
        val exceptionMessages = deferredList
                .mapNotNull { it.getCompletionExceptionOrNull() }
                .map { it.message }
                .joinToString("\n")
        if (exceptionMessages.isNotBlank()) {
            throw ConnectException(exceptionMessages)
        }
    }
}