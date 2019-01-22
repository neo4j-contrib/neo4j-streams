package streams.kafka.connect.sink

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators
import com.github.jcustenborder.kafka.connect.utils.config.validators.filesystem.ValidFile
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.sink.SinkTask
import org.neo4j.driver.internal.async.pool.PoolSettings
import org.neo4j.driver.v1.Config
import java.io.File
import java.net.URI
import java.util.concurrent.TimeUnit

enum class AuthenticationType {
    NONE, BASIC, KERBEROS
}

class Neo4jSinkConnectorConfig(originals: Map<*, *>) : AbstractConfig(config(), originals) {
    val encryptionEnabled: Boolean
    val encryptionTrustStrategy: Config.TrustStrategy.Strategy
    val authenticationType: AuthenticationType
    val authenticationUsername: String
    val authenticationPassword: String
    val authenticationRealm: String
    val authenticationKerberosTicket: String
    val serverUri: URI
    var encryptionCACertificateFile: File? = null
    val connectionMaxConnectionLifetime: Long
    val connectionLifenessCheckTimeout: Long
    val connectionPoolMaxSize: Int
    val connectionAcquisitionTimeout: Long
    val loadBalancingStrategy: Config.LoadBalancingStrategy
    val batchTimeout: Long
    val batchSize: Int
    val queryTimeout: Long

    val topicMap: Map<String, String>

    init {
        encryptionEnabled = getBoolean(ENCRYPTION_ENABLED)
        encryptionTrustStrategy = ConfigUtils
                .getEnum(Config.TrustStrategy.Strategy::class.java, this, ENCRYPTION_TRUST_STRATEGY) as Config.TrustStrategy.Strategy
        val encryptionCACertificatePATH = getString(ENCRYPTION_CA_CERTIFICATE_PATH) ?: ""

        if (encryptionCACertificatePATH != "") {
            encryptionCACertificateFile = File(encryptionCACertificatePATH)
        }

        authenticationType = ConfigUtils
                .getEnum(AuthenticationType::class.java, this, AUTHENTICATION_TYPE)
        serverUri = ConfigUtils.uri(this, SERVER_URI)
        authenticationRealm = getString(AUTHENTICATION_BASIC_REALM)
        authenticationUsername = getString(AUTHENTICATION_BASIC_USERNAME)
        authenticationPassword = getPassword(AUTHENTICATION_BASIC_PASSWORD).value()
        authenticationKerberosTicket = getPassword(AUTHENTICATION_KERBEROS_TICKET).value()
        connectionLifenessCheckTimeout = getLong(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS)
        connectionMaxConnectionLifetime = getLong(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS)
        connectionPoolMaxSize = getInt(CONNECTION_POOL_MAX_SIZE)
        connectionAcquisitionTimeout = getLong(CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS)

        loadBalancingStrategy = ConfigUtils
                .getEnum(Config.LoadBalancingStrategy::class.java, this, CONNECTION_LOAD_BALANCE_STRATEGY) as Config.LoadBalancingStrategy
        batchTimeout = getLong(BATCH_TIMEOUT_MSEC)
        queryTimeout = getLong(QUERY_TIMEOUT_MSEC)
        batchSize = getInt(BATCH_SIZE)

        topicMap = originals
                .filterKeys { it.toString().startsWith(TOPIC_CYPHER_PREFIX) }
                .mapKeys { it.key.toString().replace(TOPIC_CYPHER_PREFIX, "") }
                .mapValues { it.value.toString() }

        val topics = if (originals.containsKey(SinkTask.TOPICS_CONFIG)) {
            originals["topics"].toString().split(",").map { it.trim() }.toSet()
        } else { // TODO manage regexp
            emptySet()
        }
        if (topics != topicMap.keys.toSet()) {
            throw RuntimeException("There is a mismatch between provided Cypher queries (${topicMap.keys}) and configured topics ($topics)")
        }
    }

    companion object {
        const val SERVER_URI = "neo4j.server.uri"
        const val AUTHENTICATION_TYPE = "neo4j.authentication.type"
        const val BATCH_SIZE = "neo4j.batch.size"
        const val BATCH_TIMEOUT_MSEC = "neo4j.batch.timeout.msec"
        const val QUERY_TIMEOUT_MSEC = "neo4j.query.timeout.msec"
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
        const val TOPIC_CYPHER_PREFIX = "neo4j.topic.cypher."
        const val GROUP_ENCRYPTION = "Encryption"
        const val GROUP_CONNECTION = "Connection"
        const val GROUP_DATA_MANAGEMENT = "Data Management"

        const val CONNECTION_POOL_MAX_SIZE_DEFAULT = 100
        val BATCH_TIMEOUT_MSEC_DEFAULT = TimeUnit.SECONDS.toMillis(30L)
        val QUERY_TIMEOUT_MSEC_DEFAULT = TimeUnit.SECONDS.toMillis(50L)
        const val BATCH_SIZE_DEFAULT = 1000

        fun config(): ConfigDef {
            return ConfigDef()
                    .define(ConfigKeyBuilder.of(SERVER_URI, ConfigDef.Type.STRING)
                            .documentation(SERVER_URI).importance(ConfigDef.Importance.HIGH)
                            .defaultValue("bolt://localhost:7687").group(GROUP_CONNECTION)
                            .validator(Validators.validURI("bolt", "bolt+routing"))
                            .build())
                    .define(ConfigKeyBuilder.of(AUTHENTICATION_TYPE, ConfigDef.Type.STRING)
                            .documentation(AUTHENTICATION_TYPE)
                            .importance(ConfigDef.Importance.HIGH)
                            .defaultValue(AuthenticationType.BASIC.toString()).group(GROUP_CONNECTION)
                            .validator(ValidEnum.of(AuthenticationType::class.java))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(AUTHENTICATION_BASIC_USERNAME, ConfigDef.Type.STRING)
                            .documentation(AUTHENTICATION_BASIC_USERNAME)
                            .importance(ConfigDef.Importance.HIGH).defaultValue("").group(GROUP_CONNECTION)
                            .recommender(Recommenders
                                    .visibleIf(AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString()))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(AUTHENTICATION_BASIC_PASSWORD, ConfigDef.Type.PASSWORD)
                            .documentation(AUTHENTICATION_BASIC_PASSWORD)
                            .importance(ConfigDef.Importance.HIGH).defaultValue("").group(GROUP_CONNECTION)
                            .recommender(Recommenders
                                    .visibleIf(AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString()))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(AUTHENTICATION_BASIC_REALM, ConfigDef.Type.STRING)
                            .documentation(AUTHENTICATION_BASIC_REALM)
                            .importance(ConfigDef.Importance.HIGH).defaultValue("").group(GROUP_CONNECTION)
                            .recommender(Recommenders
                                    .visibleIf(AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString()))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(AUTHENTICATION_KERBEROS_TICKET, ConfigDef.Type.PASSWORD)
                            .documentation(AUTHENTICATION_KERBEROS_TICKET)
                            .importance(ConfigDef.Importance.HIGH).defaultValue("").group(GROUP_CONNECTION)
                            .recommender(Recommenders
                                    .visibleIf(AUTHENTICATION_TYPE, AuthenticationType.KERBEROS.toString()))
                            .build())
                    .define(ConfigKeyBuilder.of(CONNECTION_POOL_MAX_SIZE, ConfigDef.Type.INT)
                            .documentation(CONNECTION_POOL_MAX_SIZE)
                            .importance(ConfigDef.Importance.LOW).defaultValue(CONNECTION_POOL_MAX_SIZE_DEFAULT)
                            .group(GROUP_CONNECTION).validator(ConfigDef.Range.atLeast(1))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS, ConfigDef.Type.LONG)
                            .documentation(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS)
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(PoolSettings.DEFAULT_MAX_CONNECTION_LIFETIME)
                            .group(GROUP_CONNECTION).validator(ConfigDef.Range.atLeast(1))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS, ConfigDef.Type.LONG)
                            .documentation(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS)
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT)
                            .group(GROUP_CONNECTION).validator(ConfigDef.Range.atLeast(1))
                            .build())
                    .define(ConfigKeyBuilder
                            .of(CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS, ConfigDef.Type.LONG)
                            .documentation(CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS)
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT)
                            .group(GROUP_CONNECTION).validator(ConfigDef.Range.atLeast(1))
                            .build())
                    .define(ConfigKeyBuilder.of(CONNECTION_LOAD_BALANCE_STRATEGY, ConfigDef.Type.STRING)
                            .documentation(CONNECTION_LOAD_BALANCE_STRATEGY)
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(Config.LoadBalancingStrategy.LEAST_CONNECTED.toString())
                            .group(GROUP_CONNECTION)
                            .validator(ValidEnum.of(Config.LoadBalancingStrategy::class.java))
                            .build())
                    .define(ConfigKeyBuilder.of(ENCRYPTION_ENABLED, ConfigDef.Type.BOOLEAN)
                            .documentation(ENCRYPTION_ENABLED)
                            .importance(ConfigDef.Importance.HIGH).defaultValue(false)
                            .group(GROUP_ENCRYPTION).build())
                    .define(
                            ConfigKeyBuilder.of(ENCRYPTION_TRUST_STRATEGY, ConfigDef.Type.STRING)
                                    .documentation(ENCRYPTION_TRUST_STRATEGY)
                                    .importance(ConfigDef.Importance.MEDIUM)
                                    .defaultValue(Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES.toString())
                                    .group(GROUP_ENCRYPTION)
                                    .validator(ValidEnum.of(Config.TrustStrategy.Strategy::class.java))
                                    .recommender(Recommenders.visibleIf(ENCRYPTION_ENABLED, true))
                                    .build())
                    .define(ConfigKeyBuilder
                            .of(ENCRYPTION_CA_CERTIFICATE_PATH, ConfigDef.Type.STRING)
                            .documentation(ENCRYPTION_CA_CERTIFICATE_PATH)
                            .importance(ConfigDef.Importance.MEDIUM).defaultValue("").group(GROUP_ENCRYPTION)
                            .validator(Validators.blankOr(ValidFile.of())) // TODO check
                            .recommender(Recommenders.visibleIf(
                                    ENCRYPTION_TRUST_STRATEGY,
                                    Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES.toString()))
                            .build())
                    .define(ConfigKeyBuilder.of(BATCH_SIZE, ConfigDef.Type.INT)
                            .documentation(BATCH_SIZE)
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(BATCH_SIZE_DEFAULT).group(GROUP_DATA_MANAGEMENT)
                            .build())
                    .define(ConfigKeyBuilder.of(BATCH_TIMEOUT_MSEC, ConfigDef.Type.LONG)
                            .documentation(BATCH_TIMEOUT_MSEC)
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(BATCH_TIMEOUT_MSEC_DEFAULT).group(GROUP_DATA_MANAGEMENT)
                            .validator(ConfigDef.Range.atLeast(1)).build())
                    .define(ConfigKeyBuilder.of(QUERY_TIMEOUT_MSEC, ConfigDef.Type.LONG)
                            .documentation(QUERY_TIMEOUT_MSEC)
                            .importance(ConfigDef.Importance.LOW)
                            .defaultValue(QUERY_TIMEOUT_MSEC_DEFAULT).group(GROUP_DATA_MANAGEMENT)
                            .validator(ConfigDef.Range.atLeast(1)).build())
        }
    }
}