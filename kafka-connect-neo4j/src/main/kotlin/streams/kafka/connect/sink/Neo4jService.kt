package streams.kafka.connect.sink

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.exceptions.TransientException
import org.neo4j.driver.net.ServerAddress
import org.neo4j.kernel.api.exceptions.Status
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.extensions.awaitAll
import streams.extensions.errors
import streams.kafka.connect.utils.PropertiesUtil
import streams.service.StreamsSinkEntity
import streams.service.StreamsSinkService
import streams.utils.retryForException
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.streams.toList


class Neo4jService(private val config: Neo4jSinkConnectorConfig):
        StreamsSinkService(Neo4jStrategyStorage(config)) {

    private val log: Logger = LoggerFactory.getLogger(Neo4jService::class.java)

    private val driver: Driver
    private val sessionConfig: SessionConfig
    private val transactionConfig: TransactionConfig

    init {
        val configBuilder = Config.builder()
        configBuilder.withUserAgent("neo4j-kafka-connect-sink/${PropertiesUtil.getVersion()}")

        if (!this.config.hasSecuredURI()) {
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
        configBuilder.withMaxTransactionRetryTime(config.retryBackoff, TimeUnit.MILLISECONDS)
        configBuilder.withResolver { address -> this.config.serverUri.map { ServerAddress.of(it.host, it.port) }.toSet() }
        val neo4jConfig = configBuilder.build()

        this.driver = GraphDatabase.driver(this.config.serverUri.firstOrNull(), authToken, neo4jConfig)
        val sessionConfigBuilder = SessionConfig.builder()
        if (config.database.isNotBlank()) {
            sessionConfigBuilder.withDatabase(config.database)
        }
        this.sessionConfig = sessionConfigBuilder.build()
        
        val batchTimeout = this.config.batchTimeout
        this.transactionConfig = if (batchTimeout > 0) {
            TransactionConfig.builder()
                    .withTimeout(Duration.ofMillis(batchTimeout))
                    .build()
        } else {
            TransactionConfig.empty()
        }
    }

    fun close() {
        driver.close()
    }

    override fun write(query: String, events: Collection<Any>) {
        println("events ${events.size}")
        val data = mapOf<String, Any>("events" to events)
        driver.session(sessionConfig).use { session ->
            try {
                runBlocking {
                    retryForException(exceptions = arrayOf(ClientException::class.java, TransientException::class.java),
                            retries = config.retryMaxAttempts, delayTime = 0, // we use the delayTime = 0, because we delegate the retryBackoff to the Neo4j Java Driver
                            listOf(Status.Transaction.TransactionTimedOut.code().description())) { 

                        session.writeTransaction({
                            val summary = it.run(query, data).consume()
                            if (log.isDebugEnabled) {
                                println("Successfully executed query: `$query`. Summary: $summary")
                            }
                        }, transactionConfig)
                    }
                }
            } catch (e: Exception) {
                if (log.isDebugEnabled) {
                    val subList = events.stream()
                            .limit(5.coerceAtMost(events.size).toLong())
                            .toList()
                    log.debug("Exception `${e.message}` while executing query: `$query`, with data: `$subList` total-records ${events.size}")
                }
                throw e
            }
        }
//        runBlocking { delay(10000) }

//        driver.session(sessionConfig).use { session ->
//            try {
//                session.writeTransaction({
//                    kotlin.io.println("mo sto qua2 ${this@Neo4jService.transactionConfig.timeout().toMillis()}")
//                    val summary = it.run(query, data).consume()
////                            println("run2 $result")
////                            val summary = result!!.consume()
//                    if (log.isDebugEnabled) {
//                        kotlin.io.println("Successfully executed query: `$query`. Summary: $summary")
//                    }
//                    kotlin.io.println("e invece mo sto qua2")
//                }, transactionConfig)
//            } catch (e: Exception) {
//                kotlin.io.println("eccetto 2 $e")
//            }
//        }
    }

    fun writeData(data: Map<String, List<List<StreamsSinkEntity>>>) {
        val errors = if (config.parallelBatches) writeDataAsync(data) else writeDataSync(data);
        if (errors.isNotEmpty()) {
            throw ConnectException(errors.map { it.message }.toSet()
                    .joinToString("\n", "Errors executing ${data.values.map { it.size }.sum()} jobs:\n"))
        }
    }

    @ExperimentalCoroutinesApi
    @ObsoleteCoroutinesApi
    private fun writeDataAsync(data: Map<String, List<List<StreamsSinkEntity>>>) = runBlocking {
        val jobs = data
                .flatMap { (topic, records) ->
                    records.map { async(Dispatchers.IO) { writeForTopic(topic, it) } }
                }

        // timeout starts with writeTransaction()
        jobs.awaitAll()
        jobs.mapNotNull { it.errors() }
    }
    
    private fun writeDataSync(data: Map<String, List<List<StreamsSinkEntity>>>) =
            data.flatMap { (topic, records) ->
                records.mapNotNull {
                    try {
                        writeForTopic(topic, it)
                        null
                    } catch (e: Exception) {
                        e
                    }
                }
            }
}