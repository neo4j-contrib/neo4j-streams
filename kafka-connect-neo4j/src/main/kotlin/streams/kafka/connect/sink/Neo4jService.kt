package streams.kafka.connect.sink

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.whileSelect
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.exceptions.ClientException
import org.neo4j.driver.v1.exceptions.TransientException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.kafka.connect.sink.converters.Neo4jValueConverter
import streams.service.StreamsSinkEntity
import streams.service.StreamsSinkService
import streams.service.TopicType
import streams.service.TopicTypeGroup
import streams.utils.StreamsUtils
import streams.utils.retryForException
import java.util.concurrent.TimeUnit


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
        val neo4jConfig = configBuilder.toConfig()

        if (this.config.serverUri.size > 1) {
            this.driver = GraphDatabase.routingDriver(this.config.serverUri, authToken, neo4jConfig)
        } else {
            this.driver = GraphDatabase.driver(this.config.serverUri.get(0), authToken, neo4jConfig)
        }
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
        val records = events as List<Any>
        val data = mapOf<String, Any>("events" to records.map { converter.convert(it) })
        try {
            runBlocking {
                retryForException<Unit>(exceptions = arrayOf(ClientException::class.java, TransientException::class.java),
                        retries = config.retryMaxAttempts, delayTime = 0) { // we use the delayTime = 0, because we delegate the retryBackoff to the Neo4j Java Driver
                    session.writeTransaction {
                        val result = it.run(query, data)
                        if (log.isDebugEnabled) {
                            log.debug("Successfully executed query: `$query`. Summary: ${result.summary()}")
                        }
                    }
                }
            }
        } catch (e: Exception) {
            if (log.isDebugEnabled) {
                log.debug("Exception `${e.message}` while executing query: `$query`, with data: `$data`")
            }
            throw e
        }
    }


    suspend fun writeData(data: Map<String, List<List<StreamsSinkEntity>>>) = coroutineScope {
        val timeout = config.batchTimeout
        val ticker = ticker(timeout)
        val deferredList = data
                .flatMap { (topic, records) ->
                    records.map { async(Dispatchers.IO) { writeForTopic(topic, it) } }
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