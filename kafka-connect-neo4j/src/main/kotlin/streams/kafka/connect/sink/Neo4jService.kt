package streams.kafka.connect.sink

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.whileSelect
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.exceptions.ClientException
import org.neo4j.driver.v1.exceptions.TransientException
import org.neo4j.driver.v1.net.ServerAddress
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import streams.service.StreamsSinkEntity
import streams.service.StreamsSinkService
import streams.service.TopicType
import streams.service.TopicTypeGroup
import streams.utils.StreamsUtils
import streams.utils.retryForException
import java.lang.RuntimeException
import java.util.concurrent.CompletionException
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException


class Neo4jService(private val config: Neo4jSinkConnectorConfig):
        StreamsSinkService(config.strategyMap) {

    private val converter = Neo4jValueConverter()
    private val log: Logger = LoggerFactory.getLogger(Neo4jService::class.java)

    private val driver: Driver

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
        configBuilder.withResolver { address -> this.config.serverUri.map { ServerAddress.of(it.host, it.port) }.toSet() }
        val neo4jConfig = configBuilder.toConfig()

        this.driver = GraphDatabase.driver(this.config.serverUri.firstOrNull(), authToken, neo4jConfig)
    }

    fun close() {
        driver.close()
    }

    override fun getTopicType(topic: String): TopicType? = TopicType.values()
            .filter { topicType ->
                when (topicType.group) {
                    TopicTypeGroup.CDC -> (config.topics.cdcSourceIdTopics.contains(topic))
                            || (config.topics.cdcSchemaTopics.contains(topic))
                    TopicTypeGroup.CYPHER -> config.topics.cypherTopics.containsKey(topic)
                    TopicTypeGroup.PATTERN -> (topicType == TopicType.PATTERN_NODE && config.topics.nodePatternTopics.containsKey(topic))
                            || (topicType == TopicType.PATTERN_RELATIONSHIP && config.topics.relPatternTopics.containsKey(topic))
                }
            }
            .firstOrNull()

    override fun getCypherTemplate(topic: String): String? = "${StreamsUtils.UNWIND} ${config.topics.cypherTopics[topic]}"

    override fun write(query: String, events: Collection<Any>) {
        val session = driver.session()
        val records = events.map { converter.convert(it) }
        val data = mapOf<String, Any>("events" to records)
        try {
            runBlocking {
                retryForException<Unit>(exceptions = arrayOf(ClientException::class.java, TransientException::class.java),
                        retries = config.retryMaxAttempts, delayTime = 0) { // we use the delayTime = 0, because we delegate the retryBackoff to the Neo4j Java Driver
                    session.writeTransaction {
                        val summary = it.run(query, data).consume()
                        if (log.isDebugEnabled) {
                            log.debug("Successfully executed query: `$query`. Summary: ${summary}")
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
                throw TimeoutException("Tasks ${size} cancelled after timeout of $timeoutMs ms.")
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

        @ExperimentalCoroutinesApi
        @ObsoleteCoroutinesApi
        suspend fun writeData(data: Map<String, List<List<StreamsSinkEntity>>>) = coroutineScope {

            val errors = if (config.parallelBatches) {
                val jobs = data
                        .flatMap { (topic, records) ->
                            records.map { async(Dispatchers.IO) { writeForTopic(topic, it) } }
                        }

                jobs.awaitAll(config.batchTimeout)
                jobs.mapNotNull { it.errors() }
            } else {
                writeDataSync(data)
            }
            if (errors.isNotEmpty()) {
                throw ConnectException(errors.map{ it.message }.distinct().joinToString("\n","Errors executing ${data.values.map { it.size }.sum()} jobs:\n"))
            }
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