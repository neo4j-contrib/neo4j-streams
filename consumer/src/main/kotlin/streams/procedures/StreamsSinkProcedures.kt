package streams.procedures

import org.neo4j.logging.Log
import org.neo4j.procedure.*
import streams.StreamsEventConsumer
import streams.StreamsEventConsumerFactory
import streams.StreamsEventSinkConfigMapper
import streams.StreamsSinkConfiguration
import java.util.stream.Stream

class StreamResult(@JvmField val event: Map<String, *>)

class StreamsSinkProcedures {

    @JvmField @Context
    var log: Log? = null

    @Procedure(mode = Mode.SCHEMA, name = "streams.consume")
    @Description("streams.consume(topic, {timeout: <long value>, from: <string>, groupId: <string>, commit: <boolean>, partitions:[{partition: <number>, offset: <number>}]}) " +
            "YIELD event - Allows to consume custom topics")
    fun consume(@Name("topic") topic: String?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamResult> {
        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            return Stream.empty()
        }

        val properties = config?.mapValues { it.value.toString() } ?: emptyMap()
        val configuration = StreamsSinkProcedures.streamsEventSinkConfigMapper.convert(config = properties)

        val data = readData(topic, config ?: emptyMap(), configuration)

        return data.map { StreamResult(mapOf("data" to it)) }.stream()
    }

    private fun readData(topic: String, procedureConfig: Map<String, Any>, consumerConfig: Map<String, String>): List<Any> {
        val cfg = procedureConfig.mapValues { if (it.key != "partitions") it.value else mapOf(topic to it.value) }
        val timeout = cfg.getOrDefault("timeout", 1000).toString().toLong()
        val consumer = createConsumer(consumerConfig, topic)
        consumer.start()
        val data = mutableListOf<Any>()
        try {
            val start = System.currentTimeMillis()
            while ((System.currentTimeMillis() - start) < timeout) {
                consumer.read(cfg) { _, topicData ->
                    data.addAll(topicData.mapNotNull { it.value })
                }
            }
        } catch (e: Exception) {
            if (log?.isDebugEnabled!!) {
                log?.error("Error while consuming data", e)
            }
        }
        if (log?.isDebugEnabled!!) {
            log?.debug("Data retrieved from topic $topic after $timeout milliseconds: $data")
        }
        consumer.stop()
        return data
    }

    private fun createConsumer(consumerConfig: Map<String, String>, topic: String): StreamsEventConsumer {
        return streamsEventConsumerFactory
                .createStreamsEventConsumer(consumerConfig, log!!)
                .withTopics(setOf(topic))
    }

    private fun checkEnabled() {
        if (!StreamsSinkProcedures.streamsSinkConfiguration.proceduresEnabled) {
            throw RuntimeException("In order to use the procedure you must set streams.procedures.enabled=true")
        }
    }


    companion object {
        private lateinit var streamsSinkConfiguration: StreamsSinkConfiguration
        private lateinit var streamsEventConsumerFactory: StreamsEventConsumerFactory
        private lateinit var streamsEventSinkConfigMapper: StreamsEventSinkConfigMapper

        fun registerStreamsEventSinkConfigMapper(streamsEventSinkConfigMapper: StreamsEventSinkConfigMapper) {
            this.streamsEventSinkConfigMapper = streamsEventSinkConfigMapper
        }

        fun registerStreamsSinkConfiguration(streamsSinkConfiguration: StreamsSinkConfiguration) {
            this.streamsSinkConfiguration = streamsSinkConfiguration
        }

        fun registerStreamsEventConsumerFactory(streamsEventConsumerFactory: StreamsEventConsumerFactory) {
            this.streamsEventConsumerFactory = streamsEventConsumerFactory
        }
    }
}