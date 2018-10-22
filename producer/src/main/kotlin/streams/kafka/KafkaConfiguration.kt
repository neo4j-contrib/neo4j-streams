package streams.kafka

import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.LongSerializer
import streams.NodeRoutingConfiguration
import streams.RelationshipRoutingConfiguration
import streams.RoutingConfigurationFactory
import streams.events.EntityType
import streams.serialization.JacksonUtil
import java.util.*

fun Map<String,String>.getInt(name:String, defaultValue: Int) = this.get(name)?.toInt() ?: defaultValue

private fun <T> filterMap(config: Map<String, String>, routingPrefix: String, clazz: Class<T>): List<T> {
    return config
            .filterKeys { it.startsWith(routingPrefix) }
            .flatMap { RoutingConfigurationFactory.getRoutingConfiguration(it.key.replace(routingPrefix, StringUtils.EMPTY) , it.value, EntityType.node) as List<T> }
}

private object KafkaRoutingConfigurationConstants {
    const val NODE_ROUTING_KEY_PREFIX: String = "kafka.routing.nodes."
    const val REL_ROUTING_KEY_PREFIX: String = "kafka.routing.relationships."
}

data class KafkaConfiguration(val zookeeperConnect: String = "localhost:2181",
                              val bootstrapServers: String = "localhost:9092",
                              val acks: String = "1",
                              val numPartitions: Int = 1,
                              val retries: Int = 2,
                              val batchSize: Int = 16384,
                              val bufferMemory: Int = 33554432,
                              val reindexBatchSize: Int = 1000,
                              val sessionTimeoutMs: Int = 15 * 1000,
                              val connectionTimeoutMs: Int = 10 * 1000,
                              val replication: Int = 1,
                              val transactionalId: String = StringUtils.EMPTY,
                              val lingerMs: Int = 1,
                              val nodeRouting : List<NodeRoutingConfiguration> = listOf(NodeRoutingConfiguration()),
                              val relRouting : List<RelationshipRoutingConfiguration> = listOf(RelationshipRoutingConfiguration())){
    companion object {
        fun from(config: Map<String, String>) : KafkaConfiguration {
            val nodeRouting = filterMap(config = config, routingPrefix = KafkaRoutingConfigurationConstants.NODE_ROUTING_KEY_PREFIX,
                    clazz = NodeRoutingConfiguration::class.java)

            val relRouting = filterMap(config = config, routingPrefix = KafkaRoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX,
                    clazz = RelationshipRoutingConfiguration::class.java)

            val default = KafkaConfiguration()
            return default.copy(zookeeperConnect = config.getOrDefault("zookeeper.connect",default.zookeeperConnect),
                    bootstrapServers = config.getOrDefault("bootstrap.servers", default.bootstrapServers),
                    acks = config.getOrDefault("acks", default.acks),
                    numPartitions = config.getInt("num.partitions", default.numPartitions),
                    retries = config.getInt("retries", default.retries),
                    batchSize = config.getInt("batch.size", default.batchSize),
                    bufferMemory = config.getInt("buffer.memory", default.bufferMemory),
                    reindexBatchSize = config.getInt("reindex.batch.size", default.reindexBatchSize),
                    sessionTimeoutMs = config.getInt("session.timeout.ms", default.sessionTimeoutMs),
                    connectionTimeoutMs = config.getInt("connection.timeout.ms", default.connectionTimeoutMs),
                    replication = config.getInt("replication", default.replication),
                    transactionalId = config.getOrDefault("transactional.id", default.transactionalId),
                    lingerMs = config.getInt("linger.ms", default.lingerMs),
                    nodeRouting = if (nodeRouting.isEmpty()) default.nodeRouting else nodeRouting,
                    relRouting = if (relRouting.isEmpty()) default.relRouting else relRouting
            )
        }
    }

    fun asProperties(): Properties {
        val props = Properties()
        val map = JacksonUtil.getMapper().convertValue(this, Map::class.java)
                .mapKeys { it.key.toString().split("(?<=[a-z])(?=[A-Z])".toRegex()).joinToString(separator = ".").toLowerCase() }
                .filter {
                    if (it.key == "transactional.id") {
                        it.value != StringUtils.EMPTY
                    } else {
                        it.key != "node.routing" && it.key != "rel.routing"
                    }
                }
        props.putAll(map)
        props.putAll(addSerializers())
        return props
    }

    private fun addSerializers() : Properties {
        val props = Properties()
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =  LongSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        return props
    }

}