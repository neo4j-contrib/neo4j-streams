package streams.procedures

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.exception.ExceptionUtils
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import org.neo4j.procedure.TerminationGuard
import streams.StreamsEventConsumer
import streams.StreamsEventSink
import streams.StreamsSinkConfiguration
import streams.config.StreamsConfig
import streams.events.StreamsPluginStatus
import streams.extensions.isDefaultDb
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import java.util.stream.Stream
import java.util.stream.StreamSupport

class StreamResult(@JvmField val event: Map<String, *>)
class KeyValueResult(@JvmField val name: String, @JvmField val value: Any?)

class StreamsSinkProcedures {

    @JvmField @Context
    var log: Log? = null

    @JvmField @Context
    var db: GraphDatabaseService? = null

    @JvmField @Context
    var terminationGuard: TerminationGuard? = null

    @Procedure(mode = Mode.READ, name = "streams.consume")
    @Description("streams.consume(topic, {timeout: <long value>, from: <string>, groupId: <string>, commit: <boolean>, partitions:[{partition: <number>, offset: <number>}]}) " +
            "YIELD event - Allows to consume custom topics")
    fun consume(@Name("topic") topic: String?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamResult> = runBlocking {
        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            Stream.empty<StreamResult>()
        } else {
            val properties = config?.mapValues { it.value.toString() } ?: emptyMap()
            val configuration = getStreamsEventSink(db!!)!!
                .getEventSinkConfigMapper()
                .convert(config = properties)

            readData(topic, config ?: emptyMap(), configuration)
        }
    }

    @Procedure("streams.sink.start")
    fun sinkStart(): Stream<KeyValueResult> {
        checkEnabled()
        return checkLeader {
            try {
                getStreamsEventSink(db!!)?.start()
                sinkStatus()
            } catch (e: Exception) {
                log?.error("Cannot start the Sink because of the following exception", e)
                Stream.concat(sinkStatus(),
                        Stream.of(KeyValueResult("exception", ExceptionUtils.getStackTrace(e))))
            }
        }
    }

    @Procedure("streams.sink.stop")
    fun sinkStop(): Stream<KeyValueResult> {
        checkEnabled()
        return checkLeader {
            try {
                getStreamsEventSink(db!!)?.stop()
                sinkStatus()
            } catch (e: Exception) {
                log?.error("Cannot stopped the Sink because of the following exception", e)
                Stream.concat(sinkStatus(),
                        Stream.of(KeyValueResult("exception", ExceptionUtils.getStackTrace(e))))
            }
        }
    }

    @Procedure("streams.sink.restart")
    fun sinkRestart(): Stream<KeyValueResult> {
        val stopped = sinkStop().collect(Collectors.toList())
        val hasError = stopped.any { it.name == "exception" }
        if (hasError) {
            return stopped.stream()
        }
        return sinkStart()
    }

    @Procedure("streams.sink.config")
    @Deprecated("Please use streams.configuration.get")
    fun sinkConfig(): Stream<KeyValueResult> {
        checkEnabled()
        return checkLeader {
            StreamsSinkConfiguration
                    .from(configMap = StreamsConfig.getInstance(db!! as GraphDatabaseAPI)
                        .getConfiguration().mapValues { it.value.toString() },
                        dbName = db!!.databaseName(),
                        isDefaultDb = db!!.isDefaultDb())
                    .asMap()
                    .entries.stream()
                    .map { KeyValueResult(it.key, it.value) }
        }
    }

    @Procedure("streams.sink.status")
    fun sinkStatus(): Stream<KeyValueResult> {
        checkEnabled()
        return run {
            val value = (getStreamsEventSink(db!!)?.status() ?: StreamsPluginStatus.UNKNOWN).toString()
            Stream.of(KeyValueResult("status", value))
        }
    }

    private fun checkLeader(lambda: () -> Stream<KeyValueResult>): Stream<KeyValueResult> = if (Neo4jUtils.isWriteableInstance(db as GraphDatabaseAPI)) {
        lambda()
    } else {
        Stream.of(KeyValueResult("error", "You can use this procedure only in the LEADER or in a single instance configuration."))
    }

    private fun readData(topic: String, procedureConfig: Map<String, Any>, consumerConfig: Map<String, String>): Stream<StreamResult?> {
        val cfg = procedureConfig.mapValues { if (it.key != "partitions") it.value else mapOf(topic to it.value) }
        val maxRecords = cfg.getOrDefault("max.records", Int.MAX_VALUE).toString().toInt()
        val timeout = cfg.getOrDefault("timeout", 1000).toString().toLong()
        val data = ArrayBlockingQueue<StreamResult>(maxRecords)
        val tombstone = StreamResult(emptyMap<String, Any>())
        GlobalScope.launch(Dispatchers.IO) {
            val consumer = createConsumer(consumerConfig, topic)
            consumer.start()
            try {
                val start = System.currentTimeMillis()
                while ((System.currentTimeMillis() - start) < timeout) {
                    consumer.read(cfg) { _, topicData ->
                        var elements = topicData.mapNotNull { it.value }.map { StreamResult(mapOf("data" to it)) }
                        if (elements.size + data.size >= maxRecords) {
                            elements = elements.subList(0, maxRecords - data.size - 1)
                        }
                        data.addAll(elements)
                    }
                }
            } catch (e: Exception) {
                if (log?.isDebugEnabled!!) {
                    log?.error("Error while consuming data", e)
                }
            } finally {
                data.add(tombstone)
                consumer.stop()
            }
        }
        if (log?.isDebugEnabled!!) {
            log?.debug("Data retrieved from topic $topic after $timeout milliseconds: $data")
        }

        return StreamSupport.stream(QueueBasedSpliterator(data, tombstone, terminationGuard!!, timeout), false)
    }

    private fun createConsumer(consumerConfig: Map<String, String>, topic: String): StreamsEventConsumer = runBlocking {
        val copy = StreamsConfig.getInstance(db!! as GraphDatabaseAPI).getConfiguration()
            .mapValues { it.value.toString() }
            .toMutableMap()
        copy.putAll(consumerConfig)
        getStreamsEventSink(db!!)!!.getEventConsumerFactory()
                .createStreamsEventConsumer(copy, log!!, setOf(topic))
    }

    private fun checkEnabled() {
        if (!StreamsConfig.getInstance(db!! as GraphDatabaseAPI).hasProceduresEnabled(db?.databaseName() ?: ""))  {
            throw RuntimeException("In order to use the procedure you must set streams.procedures.enabled=true")
        }
    }

    companion object {

        private val streamsEventSinkStore = ConcurrentHashMap<String, StreamsEventSink>()

        private fun getStreamsEventSink(db: GraphDatabaseService) = streamsEventSinkStore[StreamsUtils.getName(db)]

        fun registerStreamsEventSink(db: GraphDatabaseAPI, streamsEventSink: StreamsEventSink) {
            streamsEventSinkStore[StreamsUtils.getName(db)] = streamsEventSink
        }

        fun unregisterStreamsEventSink(db: GraphDatabaseAPI) = streamsEventSinkStore.remove(StreamsUtils.getName(db))

        fun hasStatus(db: GraphDatabaseAPI, status: StreamsPluginStatus) = getStreamsEventSink(db)?.status() == status

        fun isRegistered(db: GraphDatabaseAPI) = getStreamsEventSink(db) != null
    }
}