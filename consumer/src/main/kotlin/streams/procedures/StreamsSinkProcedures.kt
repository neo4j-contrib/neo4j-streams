package streams.procedures

import org.apache.commons.lang3.exception.ExceptionUtils
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import streams.StreamsEventConsumer
import streams.StreamsEventSink
import streams.events.KeyValueResult
import streams.events.StreamResult
import streams.events.StreamsPluginStatus
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import java.util.stream.Stream

class StreamsSinkProcedures {

    @JvmField @Context
    var log: Log? = null

    @JvmField @Context
    var db: GraphDatabaseService? = null

    @Procedure(mode = Mode.READ, name = "streams.consume")
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
        val configuration = getStreamsEventSink(db!!)!!.getEventSinkConfigMapper().convert(config = properties)

        val data = readData(topic, config ?: emptyMap(), configuration)

        return data.map { StreamResult(mapOf("data" to it)) }.stream()
    }

    @Procedure("streams.sink.start")
    fun sinkStart(): Stream<KeyValueResult> {
        checkEnabled()
        checkLeader()
        return try {
            getStreamsEventSink(db!!)!!.start()
            sinkStatus()
        } catch (e: Exception) {
            log?.error("Cannot start the Sink because of the following exception", e)
            Stream.concat(sinkStatus(),
                    Stream.of(KeyValueResult("exception", ExceptionUtils.getStackTrace(e))))
        }
    }

    @Procedure("streams.sink.stop")
    fun sinkStop(): Stream<KeyValueResult> {
        checkEnabled()
        checkLeader()
        return try {
            getStreamsEventSink(db!!)?.stop()
            sinkStatus()
        } catch (e: Exception) {
            log?.error("Cannot stopped the Sink because of the following exception", e)
            Stream.concat(sinkStatus(),
                    Stream.of(KeyValueResult("exception", ExceptionUtils.getStackTrace(e))))
        }
    }

    @Procedure("streams.sink.restart")
    fun sinkRestart(): Stream<KeyValueResult> {
        val stopped = sinkStop().collect(Collectors.toList())
        val hasError = stopped.stream().anyMatch { it.name == "exception" }
        if (hasError) {
            return stopped.stream()
        }
        return sinkStart()
    }

    @Procedure(value = "streams.sink.config", deprecatedBy = "streams.configuration.get")
    @Deprecated("This procedure will be removed in a future release, please use `streams.configuration.get` insteadinstead")
    fun sinkConfig(): Stream<KeyValueResult> {
        checkEnabled()
        checkLeader()
        return getStreamsEventSink(db!!)?.streamsSinkConfiguration?.asMap().orEmpty()
                .entries
                .stream()
                .map { KeyValueResult(it.key, it.value) }
    }

    @Procedure("streams.sink.status")
    fun sinkStatus(): Stream<KeyValueResult> {
        checkEnabled()
        checkLeader()
        val value = (getStreamsEventSink(db!!)?.status() ?: StreamsPluginStatus.UNKNOWN).toString()
        return Stream.of(KeyValueResult("status", value))
    }

    private fun checkLeader() {
        if (!Neo4jUtils.isWriteableInstance(db as GraphDatabaseAPI)) {
            throw RuntimeException("You can use it only in the LEADER or in a single instance configuration.")
        }
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

    private fun createConsumer(consumerConfig: Map<String, String>,
                               topic: String): StreamsEventConsumer = getStreamsEventSink(db!!)!!
            .getEventConsumerFactory()
            .createStreamsEventConsumer(consumerConfig, log!!, setOf(topic))

    private fun checkEnabled() {
        if (getStreamsEventSink(db!!)?.streamsSinkConfiguration?.proceduresEnabled != true) {
            throw RuntimeException("In order to use the procedure you must set streams.procedures.enabled=true")
        }
    }


    companion object {
        private val cache = ConcurrentHashMap<String, StreamsEventSink>()

        private fun getStreamsEventSink(db: GraphDatabaseService) = cache[StreamsUtils.getName(db as GraphDatabaseAPI)]

        fun registerStreamsEventSink(db: GraphDatabaseAPI, streamsEventSink: StreamsEventSink) = cache.put(StreamsUtils.getName(db), streamsEventSink)

        fun unregisterStreamsEventSink(db: GraphDatabaseAPI) = cache.remove(StreamsUtils.getName(db))

        fun hasStatus(db: GraphDatabaseAPI, status: StreamsPluginStatus) = getStreamsEventSink(db)?.status() == status

        fun isRegistered(db: GraphDatabaseAPI) = getStreamsEventSink(db) != null

    }
}