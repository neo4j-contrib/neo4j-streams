package streams

import kafka.getInt
import org.apache.commons.lang3.StringUtils
import streams.events.EntityType



private fun <T> filterMap(config: Map<String, String>, routingPrefix: String, clazz: Class<T>): List<T> {
    return config.entries
            .filter { it.key.startsWith(routingPrefix) }
            .map { it.key.replace(routingPrefix, StringUtils.EMPTY) to it.value }
            .flatMap { RoutingConfigurationFactory.getRoutingConfiguration(it.first, it.second, EntityType.node) as List<T> }
}

data class StreamsConfiguration(val zookeeperHosts: String = "localhost:2181",
                                val kafkaHosts: String = "localhost:9092",
                                val acks: String = "1",
                                val partitionSize: Int = 1,
                                val retries: Int = 2,
                                val kafkaBatchSize: Int = 16384,
                                val kafkaBufferSize: Int = 33554432,
                                val reindexBatchSize: Int = 1000,
                                val sessionTimeoutMs: Int = 15 * 1000,
                                val connectTimeoutMs: Int = 10 * 1000,
                                val replication: Int = 1,
                                val nodeRouting : List<NodeRoutingConfiguration> = listOf(NodeRoutingConfiguration()),
                                val relRouting : List<RelationshipRoutingConfiguration> = listOf(RelationshipRoutingConfiguration())){
    companion object {
        fun from(config: Map<String,String>) : StreamsConfiguration {
            val nodeRouting = filterMap(config = config, routingPrefix = RoutingConfigurationConstants.NODE_ROUTING_KEY_PREFIX,
                    clazz = NodeRoutingConfiguration::class.java)

            val relRouting = filterMap(config = config, routingPrefix = RoutingConfigurationConstants.REL_ROUTING_KEY_PREFIX,
                    clazz = RelationshipRoutingConfiguration::class.java)

            val default = StreamsConfiguration()
            return default.copy(zookeeperHosts = config.getOrDefault("zookeeper.connect",default.zookeeperHosts),
                    kafkaHosts = config.getOrDefault("bootstrap.servers", default.kafkaHosts),
                    acks = config.getOrDefault("acks", default.acks),
                    partitionSize = config.getInt("num.partitions", default.partitionSize),
                    retries = config.getInt("retries", default.retries),
                    kafkaBatchSize = config.getInt("batch.size", default.kafkaBatchSize),
                    kafkaBufferSize = config.getInt("buffer.memory", default.kafkaBufferSize),
                    reindexBatchSize = config.getInt("reindex.batch.size", default.reindexBatchSize),
                    sessionTimeoutMs = config.getInt("session.timeout.ms", default.sessionTimeoutMs),
                    connectTimeoutMs = config.getInt("connection.timeout.ms", default.connectTimeoutMs),
                    replication = config.getInt("replication", default.replication),
                    nodeRouting = if (nodeRouting.isEmpty()) default.nodeRouting else nodeRouting,
                    relRouting = if (relRouting.isEmpty()) default.relRouting else relRouting
            )
        }
    }

}