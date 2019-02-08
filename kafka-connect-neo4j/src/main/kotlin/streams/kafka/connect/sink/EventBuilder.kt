package streams.kafka.connect.sink

import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class EventBuilder {
    private val log: Logger = LoggerFactory.getLogger(EventBuilder::class.java)

    private var batchSize: Int? = null
    private lateinit var topics: Set<String>
    private lateinit var sinkRecords: Collection<SinkRecord>

    fun withBatchSize(batchSize: Int): EventBuilder {
        this.batchSize = batchSize
        return this
    }

    fun withTopics(topics: Set<String>): EventBuilder {
        this.topics = topics
        return this
    }

    fun withSinkRecords(sinkRecords: Collection<SinkRecord>): EventBuilder {
        this.sinkRecords = sinkRecords
        return this
    }

    fun build(): Map<String, List<List<Any>>> { // <Topic, List<List<SinkRecord>>
        return this.sinkRecords
                .groupBy { it.topic() }
                .filterKeys {topic ->
                    val isValidTopic = topics.contains(topic)
                    if (!isValidTopic && log.isDebugEnabled) {
                        log.debug("Topic $topic not present")
                    }
                    isValidTopic
                }
                .mapValues {
                    val value = it.value.mapNotNull { it.value() }
                    value.chunked(this.batchSize!!)
                }
    }

}