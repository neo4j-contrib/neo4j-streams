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
import streams.extensions.awaitAll
import streams.extensions.errors
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import streams.service.StreamsSinkEntity
import streams.service.StreamsSinkService
import streams.service.TopicType
import streams.service.TopicTypeGroup
import streams.utils.StreamsUtils
import streams.utils.retryForException
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

    override fun getTopicType(topic: String): TopicType? {
        val topicConfigMap = config.topics.asMap()
        return TopicType.values()
                .filter { topicType ->
                    val topicConfig = topicConfigMap.getOrDefault(topicType, emptyList<Any>())
                    when (topicConfig) {
                        is Collection<*> -> topicConfig.contains(topic)
                        is Map<*, *> -> topicConfig.containsKey(topic)
                        else -> false
                    }
                }
                .firstOrNull()
    }

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