package streams.kafka.connect.common

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.AUTHENTICATION_BASIC_PASSWORD
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.AUTHENTICATION_BASIC_REALM
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.AUTHENTICATION_BASIC_USERNAME
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.AUTHENTICATION_KERBEROS_TICKET
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.AUTHENTICATION_TYPE
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.BATCH_SIZE
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.BATCH_TIMEOUT_MSECS
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.CONNECTION_MAX_CONNECTION_LIFETIME_MSECS
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.CONNECTION_POOL_MAX_SIZE
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.DATABASE
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.ENCRYPTION_CA_CERTIFICATE_PATH
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.ENCRYPTION_ENABLED
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.ENCRYPTION_TRUST_STRATEGY
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.RETRY_BACKOFF_MSECS
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.RETRY_MAX_ATTEMPTS
import streams.kafka.connect.common.Neo4jConnectorConfig.Companion.SERVER_URI
import streams.kafka.connect.sink.Neo4jSinkConnectorConfig
import streams.kafka.connect.sink.Neo4jSinkConnectorConfig.Companion.BATCH_PARALLELIZE
import streams.kafka.connect.sink.Neo4jSinkConnectorConfig.Companion.TOPIC_CDC_SCHEMA
import streams.kafka.connect.sink.Neo4jSinkConnectorConfig.Companion.TOPIC_CDC_SOURCE_ID
import streams.kafka.connect.sink.Neo4jSinkConnectorConfig.Companion.TOPIC_CDC_SOURCE_ID_ID_NAME
import streams.kafka.connect.sink.Neo4jSinkConnectorConfig.Companion.TOPIC_CDC_SOURCE_ID_LABEL_NAME
import streams.kafka.connect.sink.Neo4jSinkConnectorConfig.Companion.TOPIC_CUD
import streams.kafka.connect.sink.Neo4jSinkConnectorConfig.Companion.TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED
import streams.kafka.connect.sink.Neo4jSinkConnectorConfig.Companion.TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED
import streams.kafka.connect.source.Neo4jSourceConnectorConfig.Companion.ENFORCE_SCHEMA
import streams.kafka.connect.source.Neo4jSourceConnectorConfig.Companion.SOURCE_TYPE
import streams.kafka.connect.source.Neo4jSourceConnectorConfig.Companion.SOURCE_TYPE_QUERY
import streams.kafka.connect.source.Neo4jSourceConnectorConfig.Companion.STREAMING_FROM
import streams.kafka.connect.source.Neo4jSourceConnectorConfig.Companion.STREAMING_POLL_INTERVAL
import streams.kafka.connect.source.Neo4jSourceConnectorConfig.Companion.STREAMING_PROPERTY
import streams.kafka.connect.source.Neo4jSourceConnectorConfig.Companion.TOPIC

/**
 * Configuration migrator
 *
 * @property settings
 * @constructor Create empty Configuration migrator
 */
class ConfigurationMigrator(private val settings: Map<String, String>) {

    private val log: Logger = LoggerFactory.getLogger(ConfigurationMigrator::class.java)

    /**
     * Property converter
     *
     * @property updatedConfigKey - v5.1 configuration key
     * @property migrationHandler - Value migration handler
     */
    data class PropertyConverter(val updatedConfigKey: String, val migrationHandler: () -> String)

    private val propertyConverterMap: Map<String, PropertyConverter> = mutableMapOf(
        // Common
        DATABASE to PropertyConverter("neo4j.database") { settings[DATABASE] as String },
        SERVER_URI to PropertyConverter("neo4j.uri") { settings[SERVER_URI] as String },
        AUTHENTICATION_TYPE to PropertyConverter("neo4j.authentication.type") { settings[AUTHENTICATION_TYPE] as String },
        AUTHENTICATION_BASIC_USERNAME to PropertyConverter("neo4j.authentication.basic.username") {settings[AUTHENTICATION_BASIC_USERNAME] as String},
        AUTHENTICATION_BASIC_PASSWORD to PropertyConverter("neo4j.authentication.basic.password") {settings[AUTHENTICATION_BASIC_PASSWORD] as String},
        AUTHENTICATION_BASIC_REALM to PropertyConverter("neo4j.authentication.basic.realm") {settings[AUTHENTICATION_BASIC_REALM] as String},
        AUTHENTICATION_KERBEROS_TICKET to PropertyConverter("neo4j.authentication.kerberos.ticket") {settings[AUTHENTICATION_KERBEROS_TICKET] as String},
        BATCH_SIZE to PropertyConverter("neo4j.batch-size") {settings[BATCH_SIZE] as String},
        ENCRYPTION_ENABLED to PropertyConverter("neo4j.security.encrypted") {settings[ENCRYPTION_ENABLED] as String},
        ENCRYPTION_TRUST_STRATEGY to PropertyConverter("neo4j.security.trust-strategy") {settings[ENCRYPTION_TRUST_STRATEGY] as String},
        ENCRYPTION_CA_CERTIFICATE_PATH to PropertyConverter("") {settings[ENCRYPTION_CA_CERTIFICATE_PATH] as String},
        CONNECTION_MAX_CONNECTION_LIFETIME_MSECS to PropertyConverter("neo4j.connection-timeout") { convertMsecs(settings[CONNECTION_MAX_CONNECTION_LIFETIME_MSECS] as String) },
        CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS to PropertyConverter("neo4j.pool.connection-acquisition-timeout") { convertMsecs(settings[CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS] as String) },
        CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS to PropertyConverter("neo4j.pool.idle-time-before-connection-test") { convertMsecs(settings[CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS] as String) },
        CONNECTION_POOL_MAX_SIZE to PropertyConverter("neo4j.pool.max-connection-pool-size") {settings[CONNECTION_POOL_MAX_SIZE] as String},
        RETRY_BACKOFF_MSECS to PropertyConverter("neo4j.max-retry-time") { convertMsecs(settings[RETRY_BACKOFF_MSECS] as String) },
        RETRY_MAX_ATTEMPTS to PropertyConverter("neo4j.max-retry-attempts") {settings[RETRY_MAX_ATTEMPTS] as String},
        // Sink
        TOPIC_CDC_SOURCE_ID to PropertyConverter("neo4j.cdc.source-id.topics") {settings[TOPIC_CDC_SOURCE_ID] as String},
        TOPIC_CDC_SOURCE_ID_LABEL_NAME to PropertyConverter("neo4j.cdc.source-id.label-name") {settings[TOPIC_CDC_SOURCE_ID_LABEL_NAME] as String},
        TOPIC_CDC_SOURCE_ID_ID_NAME to PropertyConverter("neo4j.cdc.source-id.property-name") {settings[TOPIC_CDC_SOURCE_ID_ID_NAME] as String},
        TOPIC_CDC_SCHEMA to PropertyConverter("neo4j.cdc.schema.topics") {settings[TOPIC_CDC_SCHEMA] as String},
        BATCH_PARALLELIZE to PropertyConverter("") {settings[BATCH_PARALLELIZE] as String},
        TOPIC_CUD to PropertyConverter("neo4j.cud.topics") {settings[TOPIC_CUD] as String},
        TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED to PropertyConverter("neo4j.pattern.node.merge-properties") {settings[TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED] as String},
        TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED to PropertyConverter("neo4j.pattern.relationship.merge-properties") {settings[TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED] as String},
        // Source
        BATCH_TIMEOUT_MSECS to PropertyConverter("neo4j.batch-timeout") { convertMsecs(settings[BATCH_TIMEOUT_MSECS] as String) },
        TOPIC to PropertyConverter("neo4j.query.topic") {settings[TOPIC] as String},
        STREAMING_FROM to PropertyConverter("") {settings[STREAMING_FROM] as String},
        SOURCE_TYPE to PropertyConverter("") {settings[SOURCE_TYPE] as String},
        SOURCE_TYPE_QUERY to PropertyConverter("neo4j.query") {settings[SOURCE_TYPE_QUERY] as String},
        STREAMING_PROPERTY to PropertyConverter("neo4j.query.streaming-property") {settings[STREAMING_PROPERTY] as String},
        STREAMING_POLL_INTERVAL to PropertyConverter("neo4j.query.poll-interval") { convertMsecs(settings[STREAMING_POLL_INTERVAL] as String) },
        ENFORCE_SCHEMA to PropertyConverter("") {settings[ENFORCE_SCHEMA] as String}
    )

    // Configuration properties that have user-defined keys
    private val prefixConverterMap: Map<String, String> = mutableMapOf(
        Neo4jSinkConnectorConfig.TOPIC_PATTERN_NODE_PREFIX to "neo4j.pattern.node.topic.",
        Neo4jSinkConnectorConfig.TOPIC_PATTERN_RELATIONSHIP_PREFIX to "neo4j.pattern.relationship.topic.",
        Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX to "neo4j.cypher.topic."
    )

    /**
     * Migrate configuration keys from existing format to v5.1 connector format.
     * Configuration properties containing msecs units are converted to new format.
     *
     * @return updated configuration key-value pairs
     */
    fun migrate(): Map<String, Any> {
        val updatedConfig: MutableMap<String, String> = mutableMapOf()

        settings.forEach { (originalKey, value) ->
            val propConverter = propertyConverterMap[originalKey]
            if (propConverter != null) {
                val newKey = propConverter.updatedConfigKey
                updatedConfig[newKey] = propConverter.migrationHandler()
                log.debug("Migrating configuration {} to {}", originalKey, newKey)
            } else {
                // prefix match?
                val prefixMatch = prefixConverterMap.keys.find { k -> originalKey.startsWith(k) }
                prefixMatch?.let { prefix ->
                    val replacement = prefixConverterMap[prefixMatch]
                    replacement?.let { repl ->
                        val newKey = originalKey.replace(prefix, repl)
                        updatedConfig[newKey] = value
                        log.debug("Migrating configuration prefix key {} to {}", originalKey, newKey)
                    }
                }
            }
        }

        outputToLog(updatedConfig)

        return updatedConfig
    }

    private fun outputToLog(config: Map<String, Any>) {
        val mapper = ObjectMapper()
        val json = mapper.writeValueAsString(config)
        log.info("Migrated configuration to v5.1 connector format: {}", json)
    }

    companion object {
        private fun convertMsecs(msecs: String): String {
            return "${msecs}ms"
        }
    }

}

