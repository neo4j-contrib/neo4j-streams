package streams.kafka.connect.sink

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators
import com.github.jcustenborder.kafka.connect.utils.config.validators.filesystem.ValidFile
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkTask
import org.neo4j.driver.internal.async.pool.PoolSettings
import org.neo4j.driver.v1.Config
import streams.kafka.connect.utils.PropertiesUtil
import java.io.File
import java.net.URI
import java.util.concurrent.TimeUnit

enum class AuthenticationType {
    NONE, BASIC, KERBEROS
}

object ConfigGroup {
    const val ENCRYPTION = "Encryption"
    const val CONNECTION = "Connection"
    const val AUTHENTICATION = "Authentication"
    const val TOPIC_CYPHER_MAPPING = "Topic Cypher Mapping"
    const val BATCH = "Batch Management"
    const val RETRY = "Retry Strategy"
    const val DEPRECATED = "Deprecated Properties (please check the documentation)"
}

class Neo4jSinkConnectorConfig(originals: Map<*, *>): AbstractConfig(config(), originals) {
    val encryptionEnabled: Boolean
    val encryptionTrustStrategy: Config.TrustStrategy.Strategy
    var encryptionCACertificateFile: File? = null

    val authenticationType: AuthenticationType
    val authenticationUsername: String
    val authenticationPassword: String
    val authenticationRealm: String
    val authenticationKerberosTicket: String

    val serverUri: URI
    val connectionMaxConnectionLifetime: Long
    val connectionLifenessCheckTimeout: Long
    val connectionPoolMaxSize: Int
    val connectionAcquisitionTimeout: Long
    val loadBalancingStrategy: Config.LoadBalancingStrategy

    val retryBackoff: Long
    val retryMaxAttempts: Int

    val batchTimeout: Long
    val batchSize: Int

    val cdcMergeTopics: Set<String>

    val cypherTopics: Map<String, String>


    init {
        encryptionEnabled = getBoolean(ENCRYPTION_ENABLED)
        encryptionTrustStrategy = ConfigUtils
                .getEnum(Config.TrustStrategy.Strategy::class.java, this, ENCRYPTION_TRUST_STRATEGY)
        val encryptionCACertificatePATH = getString(ENCRYPTION_CA_CERTIFICATE_PATH) ?: ""
        if (encryptionCACertificatePATH != "") {
            encryptionCACertificateFile = File(encryptionCACertificatePATH)
        }

        authenticationType = ConfigUtils
                .getEnum(AuthenticationType::class.java, this, AUTHENTICATION_TYPE)
        authenticationRealm = getString(AUTHENTICATION_BASIC_REALM)
        authenticationUsername = getString(AUTHENTICATION_BASIC_USERNAME)
        authenticationPassword = getPassword(AUTHENTICATION_BASIC_PASSWORD).value()
        authenticationKerberosTicket = getPassword(AUTHENTICATION_KERBEROS_TICKET).value()

        serverUri = ConfigUtils.uri(this, SERVER_URI)
        connectionLifenessCheckTimeout = getLong(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS)
        connectionMaxConnectionLifetime = getLong(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS)
        connectionPoolMaxSize = getInt(CONNECTION_POOL_MAX_SIZE)
        connectionAcquisitionTimeout = getLong(CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS)
        loadBalancingStrategy = ConfigUtils
                .getEnum(Config.LoadBalancingStrategy::class.java, this, CONNECTION_LOAD_BALANCE_STRATEGY)

        retryBackoff = getLong(RETRY_BACKOFF_MSECS)
        retryMaxAttempts = getInt(RETRY_MAX_ATTEMPTS)

        batchTimeout = getLong(BATCH_TIMEOUT_MSECS)
        batchSize = getInt(BATCH_SIZE)

        cypherTopics = getCypherTopics(originals)
        cdcMergeTopics = getCDCTopics()

        validateAllTopics(originals)
    }

    private fun validateAllTopics(originals: Map<*, *>) {
        val crossDefinedTopics = cdcMergeTopics.intersect(cypherTopics.keys)
        if (crossDefinedTopics.isNotEmpty()) {
            throw ConfigException("The following topics are cross defined between Cypher template configuration and CDC configuration: $crossDefinedTopics")
        }
        val topics = if (originals.containsKey(SinkTask.TOPICS_CONFIG)) {
            originals["topics"].toString().split(",").map { it.trim() }.toSet()
        } else { // TODO manage regexp
            emptySet()
        }
        if (topics != getAllTopics()) {
            throw ConfigException("There is a mismatch between provided Cypher queries (${cypherTopics.keys}) and configured topics ($topics)")
        }
    }

    fun getAllTopics() = cypherTopics.keys.toSet() + cdcMergeTopics

    private fun getCDCTopics(): Set<String> {
        val cdcTopicsString = getString(TOPIC_CDC)
        return if (cdcTopicsString == "") {
            emptySet()
        } else {
            cdcTopicsString.split(";").toSet()
        }
    }

    private fun getCypherTopics(originals: Map<*, *>): Map<String, String> {
        return originals
                .filterKeys { it.toString().startsWith(TOPIC_CYPHER_PREFIX) }
                .mapKeys { it.key.toString().replace(TOPIC_CYPHER_PREFIX, "") }
                .mapValues { it.value.toString() }
    }

    companion object {
        const val SERVER_URI = "neo4j.server.uri"

        const val AUTHENTICATION_TYPE = "neo4j.authentication.type"
        const val AUTHENTICATION_BASIC_USERNAME = "neo4j.authentication.basic.username"
        const val AUTHENTICATION_BASIC_PASSWORD = "neo4j.authentication.basic.password"
        const val AUTHENTICATION_BASIC_REALM = "neo4j.authentication.basic.realm"
        const val AUTHENTICATION_KERBEROS_TICKET = "neo4j.authentication.kerberos.ticket"

        const val ENCRYPTION_ENABLED = "neo4j.encryption.enabled"
        const val ENCRYPTION_TRUST_STRATEGY = "neo4j.encryption.trust.strategy"
        const val ENCRYPTION_CA_CERTIFICATE_PATH = "neo4j.encryption.ca.certificate.path"

        const val CONNECTION_MAX_CONNECTION_LIFETIME_MSECS = "neo4j.connection.max.lifetime.msecs"
        const val CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS = "neo4j.connection.acquisition.timeout.msecs"
        const val CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS = "neo4j.connection.liveness.check.timeout.msecs"
        const val CONNECTION_POOL_MAX_SIZE = "neo4j.connection.max.pool.size"
        const val CONNECTION_LOAD_BALANCE_STRATEGY = "neo4j.load.balance.strategy"

        const val BATCH_SIZE = "neo4j.batch.size"
        const val BATCH_TIMEOUT_MSECS = "neo4j.batch.timeout.msecs"

        const val RETRY_BACKOFF_MSECS = "neo4j.retry.backoff.msecs"
        const val RETRY_MAX_ATTEMPTS = "neo4j.retry.max.attemps"

        const val TOPIC_CYPHER_PREFIX = "neo4j.topic.cypher."
        const val TOPIC_CDC = "neo4j.topic.cdc.merge"

        const val CONNECTION_POOL_MAX_SIZE_DEFAULT = 100
        val BATCH_TIMEOUT_DEFAULT = TimeUnit.SECONDS.toMillis(30L)
        const val BATCH_SIZE_DEFAULT = 1000
        val RETRY_BACKOFF_DEFAULT = TimeUnit.SECONDS.toMillis(30L)
        const val RETRY_MAX_ATTEMPTS_DEFAULT = 5

        fun config(): ConfigDef {
            return ConfigDef()
                    .define(ConfigKeyBuilder
                            .of(AUTHENTICATION_TYPE, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(AUTHENTICATION_TYPE))
                            .importance(ConfigDef.Importance.HIGH)
                            .defaultValue(AuthenticationType.BASIC.toString())
                            .group(ConfigGroup.AUTHENTICATION)
                            .validator(ValidEnum.of(AuthenticationType::class.java))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(AUTHENTICATION_BASIC_USERNAME, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(AUTHENTICATION_BASIC_USERNAME))
                            .importance(ConfigDef.Importance.HIGH)
                            .defaultValue("")
                            .group(ConfigGroup.AUTHENTICATION)
                            .recommender(Recommenders.visibleIf(AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString()))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(AUTHENTICATION_BASIC_PASSWORD, ConfigDef.Type.PASSWORD)
                            .documentation(PropertiesUtil.getProperty(AUTHENTICATION_BASIC_PASSWORD))
                            .importance(ConfigDef.Importance.HIGH)
                            .defaultValue("")
                            .group(ConfigGroup.AUTHENTICATION)
                            .recommender(Recommenders.visibleIf(AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString()))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(AUTHENTICATION_BASIC_REALM, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(AUTHENTICATION_BASIC_REALM))
                            .importance(ConfigDef.Importance.HIGH)
                            .defaultValue("")
                            .group(ConfigGroup.AUTHENTICATION)
                            .recommender(Recommenders.visibleIf(AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString()))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(AUTHENTICATION_KERBEROS_TICKET, ConfigDef.Type.PASSWORD)
                            .documentation(PropertiesUtil.getProperty(AUTHENTICATION_KERBEROS_TICKET))
                            .importance(ConfigDef.Importance.HIGH)
                            .defaultValue("")
                            .group(ConfigGroup.AUTHENTICATION)
                            .recommender(Recommenders.visibleIf(AUTHENTICATION_TYPE, AuthenticationType.KERBEROS.toString()))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(SERVER_URI, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(SERVER_URI))
                            .importance(ConfigDef.Importance.HIGH)
                            .defaultValue("bolt://localhost:7687")
                            .group(ConfigGroup.CONNECTION)
                            .validator(Validators.validURI("bolt", "bolt+routing"))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(CONNECTION_POOL_MAX_SIZE, ConfigDef.Type.INT)
                            .documentation(PropertiesUtil.getProperty(CONNECTION_POOL_MAX_SIZE))
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(CONNECTION_POOL_MAX_SIZE_DEFAULT)
                            .group(ConfigGroup.CONNECTION)
                            .validator(ConfigDef.Range.atLeast(1))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS, ConfigDef.Type.LONG)
                            .documentation(PropertiesUtil.getProperty(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS))
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(PoolSettings.DEFAULT_MAX_CONNECTION_LIFETIME)
                            .group(ConfigGroup.CONNECTION)
                            .validator(ConfigDef.Range.atLeast(1))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS, ConfigDef.Type.LONG)
                            .documentation(PropertiesUtil.getProperty(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS))
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT)
                            .group(ConfigGroup.CONNECTION)
                            .validator(ConfigDef.Range.atLeast(1))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS, ConfigDef.Type.LONG)
                            .documentation(PropertiesUtil.getProperty(CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS))
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT)
                            .group(ConfigGroup.CONNECTION)
                            .validator(ConfigDef.Range.atLeast(1))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(CONNECTION_LOAD_BALANCE_STRATEGY, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(CONNECTION_LOAD_BALANCE_STRATEGY))
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(Config.LoadBalancingStrategy.LEAST_CONNECTED.toString())
                            .group(ConfigGroup.CONNECTION)
                            .validator(ValidEnum.of(Config.LoadBalancingStrategy::class.java))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(ENCRYPTION_ENABLED, ConfigDef.Type.BOOLEAN)
                            .documentation(PropertiesUtil.getProperty(ENCRYPTION_ENABLED))
                            .importance(ConfigDef.Importance.HIGH)
                            .defaultValue(false)
                            .group(ConfigGroup.ENCRYPTION).build())
                    .define(ConfigKeyBuilder
                            .of(ENCRYPTION_TRUST_STRATEGY, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(ENCRYPTION_TRUST_STRATEGY))
                            .importance(ConfigDef.Importance.MEDIUM)
                            .defaultValue(Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES.toString())
                            .group(ConfigGroup.ENCRYPTION)
                            .validator(ValidEnum.of(Config.TrustStrategy.Strategy::class.java))
                            .recommender(Recommenders.visibleIf(ENCRYPTION_ENABLED, true))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(ENCRYPTION_CA_CERTIFICATE_PATH, ConfigDef.Type.STRING)
                            .documentation(PropertiesUtil.getProperty(ENCRYPTION_CA_CERTIFICATE_PATH))
                            .importance(ConfigDef.Importance.MEDIUM)
                            .defaultValue("")
                            .group(ConfigGroup.ENCRYPTION)
                            .validator(Validators.blankOr(ValidFile.of())) // TODO check
                            .recommender(Recommenders.visibleIf(
                                    ENCRYPTION_TRUST_STRATEGY,
                                    Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES.toString()))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(BATCH_SIZE, ConfigDef.Type.INT)
                            .documentation(PropertiesUtil.getProperty(BATCH_SIZE))
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(BATCH_SIZE_DEFAULT)
                            .group(ConfigGroup.BATCH)
                            .validator(ConfigDef.Range.atLeast(1))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(BATCH_TIMEOUT_MSECS, ConfigDef.Type.LONG)
                            .documentation(PropertiesUtil.getProperty(BATCH_TIMEOUT_MSECS))
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(BATCH_TIMEOUT_DEFAULT)
                            .group(ConfigGroup.BATCH)
                            .validator(ConfigDef.Range.atLeast(1)).build())
                    .define(ConfigKeyBuilder
                            .of(RETRY_BACKOFF_MSECS, ConfigDef.Type.LONG)
                            .documentation(PropertiesUtil.getProperty(RETRY_BACKOFF_MSECS))
                            .importance(ConfigDef.Importance.MEDIUM)
                            .defaultValue(RETRY_BACKOFF_DEFAULT)
                            .group(ConfigGroup.RETRY)
                            .validator(ConfigDef.Range.atLeast(1))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(RETRY_MAX_ATTEMPTS, ConfigDef.Type.INT)
                            .documentation(PropertiesUtil.getProperty(RETRY_MAX_ATTEMPTS))
                            .importance(ConfigDef.Importance.MEDIUM)
                            .defaultValue(RETRY_MAX_ATTEMPTS_DEFAULT)
                            .group(ConfigGroup.RETRY)
                            .validator(ConfigDef.Range.atLeast(1)).build())
                    .define(ConfigKeyBuilder.of(TOPIC_CDC, ConfigDef.Type.STRING)
                            .documentation(TOPIC_CDC).importance(ConfigDef.Importance.HIGH)
                            .defaultValue("").group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                            .build())
        }
    }
}