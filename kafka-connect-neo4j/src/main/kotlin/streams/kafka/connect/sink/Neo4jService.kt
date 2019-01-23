package streams.kafka.connect.sink

import kotlinx.coroutines.async
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.selects.whileSelect
import org.apache.kafka.common.config.ConfigException
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit


class Neo4jService(private val config: Neo4jSinkConnectorConfig) {

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
        val neo4jConfig = configBuilder.toConfig()
        this.driver = GraphDatabase.driver(this.config.serverUri, authToken, neo4jConfig)
    }

    fun close() {
        driver.close()
    }

    private fun write(query: String, data: Map<String, Any>) {
        val session = driver.session()
        session.writeTransaction {
            try {
                it.run(query, data)
                it.success()
                if (log.isDebugEnabled) {
                    log.debug("Successfully executed query: `$query`, with data: `$data`")
                }
            } catch (e: Exception) {
                if (log.isDebugEnabled) {
                    log.debug("Exception `${e.message}` while executing query: `$query`, with data: `$data`")
                }
                it.failure()
            }
            it.close()
        }
        session.close()
    }

    suspend fun writeData(data: Map<String, List<List<Map<String, Any>>>>) = coroutineScope {
        val timeout = config.batchTimeout
        val ticker = ticker(timeout)
        val deferredList = data.flatMap { (query, events) ->
            events.flatMap { it.map { async { write(query, it) } } }
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
    }



}