package streams.kafka.connect.sink

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.whileSelect
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.exceptions.TransientException
import org.neo4j.driver.net.ServerAddress
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import streams.kafka.connect.utils.PropertiesUtil
import streams.service.StreamsSinkEntity
import streams.service.StreamsSinkService
import streams.service.TopicType
import streams.utils.StreamsUtils
import streams.utils.retryForException
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException


class Neo4jService(private val config: Neo4jSinkConnectorConfig):
        StreamsSinkService(Neo4jStrategyStorage(config)) {

    private val converter = Neo4jValueConverter()
    private val log: Logger = LoggerFactory.getLogger(Neo4jService::class.java)

    private val driver: Driver

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
    }

    fun close() {
        driver.close()
    }


//    override fun getCypherTemplate(topic: String): String? = "${StreamsUtils.UNWIND} ${config.topics.cypherTopics[topic]}"

    override fun write(query: String, events: Collection<Any>) {
        val sessionConfigBuilder = SessionConfig.builder()
        if (config.database.isNotBlank()) {
            sessionConfigBuilder.withDatabase(config.database)
        }
        val session = driver.session(sessionConfigBuilder.build())
        val records = events.map { converter.convert(it) }
        val data = mapOf<String, Any>("events" to records)
        try {
            runBlocking {
                retryForException<Unit>(exceptions = arrayOf(ClientException::class.java, TransientException::class.java),
                        retries = config.retryMaxAttempts, delayTime = 0) { // we use the delayTime = 0, because we delegate the retryBackoff to the Neo4j Java Driver
                    session.writeTransaction {
                        val summary = it.run(query, data).consume()
                        if (log.isDebugEnabled) {
                            log.debug("Successfully executed query: `$query`. Summary: $summary")
                        }
                    }
                }
            }
        } catch (e: Exception) {
            if (log.isDebugEnabled) {
                log.debug("Exception `${e.message}` while executing query: `$query`, with data: `${records.subList(0,Math.min(5,records.size))}` total-records ${records.size}")
            }
            throw e
        } finally {
            session.close()
        }
    }

    // taken from https://stackoverflow.com/questions/52192752/kotlin-how-to-run-n-coroutines-and-wait-for-first-m-results-or-timeout
    @ObsoleteCoroutinesApi
    @ExperimentalCoroutinesApi
    suspend fun <T> List<Deferred<T>>.awaitAll(timeoutMs: Long): List<T> {
        val jobs = CopyOnWriteArraySet<Deferred<T>>(this)
        val result = ArrayList<T>(size)
        val timeout = ticker(timeoutMs)

        whileSelect {
            jobs.forEach { deferred ->
                deferred.onAwait {
                    jobs.remove(deferred)
                    result.add(it)
                    result.size != size
                }
            }

            timeout.onReceive {
                jobs.forEach { it.cancel() }
                throw TimeoutException("Tasks $size cancelled after timeout of $timeoutMs ms.")
            }
        }

        return result
    }

    @ExperimentalCoroutinesApi
    fun <T> Deferred<T>.errors() = when {
        isCompleted -> getCompletionExceptionOrNull()
        isCancelled -> getCompletionExceptionOrNull() // was getCancellationException()
        isActive -> RuntimeException("Job $this still active")
        else -> null }

    suspend fun writeData(data: Map<String, List<List<StreamsSinkEntity>>>) {
        val errors = if (config.parallelBatches) writeDataAsync(data) else writeDataSync(data);

        if (errors.isNotEmpty()) {
            throw ConnectException(errors.map { it.message }.distinct().joinToString("\n", "Errors executing ${data.values.map { it.size }.sum()} jobs:\n"))
        }
    }

    @ExperimentalCoroutinesApi
    @ObsoleteCoroutinesApi
    suspend fun writeDataAsync(data: Map<String, List<List<StreamsSinkEntity>>>) = coroutineScope {
        val jobs = data
                .flatMap { (topic, records) ->
                    records.map { async(Dispatchers.IO) { writeForTopic(topic, it) } }
                }

        jobs.awaitAll(config.batchTimeout)
        jobs.mapNotNull { it.errors() }
    }
    
    fun writeDataSync(data: Map<String, List<List<StreamsSinkEntity>>>) =
            data.flatMap { (topic, records) ->
                records.map {
                    try {
                        writeForTopic(topic, it)
                    } catch (e: Exception) {
                        e
                    }
                }.filterIsInstance<Throwable>()
            }
}