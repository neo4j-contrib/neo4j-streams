package streams.service.dlq

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.neo4j.util.VisibleForTesting
import java.util.*

class KafkaDLQService: DeadLetterQueueService {

    private var producer: Producer<ByteArray, ByteArray>

    constructor(config: Map<String, Any>,
                headerPrefix: String): super(config, headerPrefix) { // "__connect.errors."
        val props = Properties()
        props.putAll(config)
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java.name
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java.name
        producer = KafkaProducer(props)
    }

    @VisibleForTesting
    constructor(producer: Producer<ByteArray, ByteArray>, headerPrefix: String = ""): super(emptyMap(), headerPrefix) {
        this.producer = producer
    }

    override fun send(deadLetterQueueTopic: String, dlqData: DLQData) {
        val producerRecord = if (dlqData.timestamp == RecordBatch.NO_TIMESTAMP) {
            ProducerRecord(deadLetterQueueTopic, null,
                    dlqData.key, dlqData.value)
        } else {
            ProducerRecord(deadLetterQueueTopic, null, dlqData.timestamp,
                    dlqData.key, dlqData.value)
        }
        val producerHeader = producerRecord.headers()
        populateContextHeaders(dlqData).forEach { key, value -> producerHeader.add(key, value) }
        producer.send(producerRecord)
    }

    @VisibleForTesting
    fun populateContextHeaders(dlqData: DLQData): Map<String, ByteArray> {
        val headers = mutableMapOf<String, ByteArray>()
        headers[ERROR_HEADER_ORIG_TOPIC] = dlqData.originalTopic.toByteArray()
        headers[ERROR_HEADER_ORIG_PARTITION] = dlqData.partition.toByteArray()
        headers[ERROR_HEADER_ORIG_OFFSET] = dlqData.offset.toByteArray()
        if (dlqData.executingClass != null) {
            headers[ERROR_HEADER_EXECUTING_CLASS] = dlqData.executingClass.name.toByteArray()
        }
        if (dlqData.exception != null) {
            headers[ERROR_HEADER_EXCEPTION] = dlqData.exception.javaClass.name.toByteArray()
            if (dlqData.exception.message != null) {
                headers[ERROR_HEADER_EXCEPTION_MESSAGE] = dlqData.exception.message.toString().toByteArray()
            }
            headers[ERROR_HEADER_EXCEPTION_STACK_TRACE] = ExceptionUtils.getStackTrace(dlqData.exception).toByteArray()
        }
        return headers
    }

    override fun close() {
        this.producer.close()
    }

}