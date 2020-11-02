package streams.kafka.connect.sink

import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.kafka.connect.utils.toStreamsSinkEntity
import streams.service.StreamsSinkEntity

class EventBuilder {
    private var batchSize: Int? = null
    private lateinit var sinkRecords: Collection<SinkRecord>

    fun withBatchSize(batchSize: Int): EventBuilder {
        this.batchSize = batchSize
        return this
    }

    fun withSinkRecords(sinkRecords: Collection<SinkRecord>): EventBuilder {
        this.sinkRecords = sinkRecords
        return this
    }

    fun build(): Map<String, List<List<StreamsSinkEntity>>> { // <Topic, List<List<SinkRecord>>
        val batchSize = this.batchSize!!
        return this.sinkRecords
                .groupBy { it.topic() }
                .mapValues { entry ->
                    val value = entry.value.map { it.toStreamsSinkEntity() }
                    if (batchSize > value.size) listOf(value) else value.chunked(batchSize)
                }
    }

}