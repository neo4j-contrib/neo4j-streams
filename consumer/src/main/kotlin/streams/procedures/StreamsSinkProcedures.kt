package streams.procedures

import org.neo4j.logging.Log
import org.neo4j.procedure.*
import streams.StreamsEventConsumerFactory
import streams.StreamsEventSinkConfigMapper
import streams.StreamsSinkConfiguration
import java.util.stream.Stream

class StreamResult(@JvmField val event: Map<String, *>)

class StreamsSinkProcedures {

    @JvmField @Context
    var log: Log? = null

    @Procedure(mode = Mode.SCHEMA, name = "streams.consume")
    @Description("streams.consume(topic, {timeout: <long value>, from: <string>}) YIELD event - Allows to consume custom topics")
    fun consume(@Name("topic") topic: String?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamResult> {
        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            return Stream.empty()
        }

        val properties = config?.mapValues { it.value.toString() } ?: emptyMap()
        val configuration = StreamsSinkProcedures.streamsEventSinkConfigMapper.convert(config = properties)

        val consumer = StreamsSinkProcedures
                .streamsEventConsumerFactory
                .createStreamsEventConsumer(configuration, log!!)
                .withTopics(setOf(topic))
        consumer.start()
        val timeout = configuration.getOrDefault("streams.sink.batch.timeout", 1000).toString().toLong()
        val data = try {
            consumer.read(timeout)
        } catch (e: Exception) {
            if (log?.isDebugEnabled!!) {
                log?.error("Error while consuming data", e)
            }
            emptyMap<String, List<Any>>()
        }
        consumer.stop()

        if (log?.isDebugEnabled!!) {
            log?.debug("Data retrieved from topic $topic after ${configuration["streams.sink.polling.interval"]} milliseconds: $data")
        }
        return data?.values?.flatMap { list -> list.map { StreamResult(mapOf("data" to it)) } }?.stream() ?: Stream.empty()
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