package streams.service.dlq

import org.apache.kafka.clients.consumer.ConsumerRecord


data class DLQData(val originalTopic: String,
                   val timestamp: Long,
                   val key: ByteArray,
                   val value: ByteArray,
                   val partition: String,
                   val offset: String,
                   val executingClass: Class<*>?,
                   val exception: Exception?) {

    companion object {
        fun from(consumerRecord: ConsumerRecord<ByteArray, ByteArray>, exception: Exception?, executingClass: Class<*>?): DLQData {
            return DLQData(offset = consumerRecord.offset().toString(),
                    originalTopic = consumerRecord.topic(),
                    partition = consumerRecord.partition().toString(),
                    timestamp = consumerRecord.timestamp(),
                    exception = exception,
                    executingClass = executingClass,
                    key = consumerRecord.key(),
                    value = consumerRecord.value())
        }
    }
}

abstract class DeadLetterQueueService(private val config: Map<String, Any>,
                                             headerPrefix: String) {

    val ERROR_HEADER_ORIG_TOPIC = headerPrefix + "topic"
    val ERROR_HEADER_ORIG_PARTITION = headerPrefix + "partition"
    val ERROR_HEADER_ORIG_OFFSET = headerPrefix + "offset"
    val ERROR_HEADER_EXECUTING_CLASS = headerPrefix + "class.name"
    val ERROR_HEADER_EXCEPTION = headerPrefix + "exception.class.name"
    val ERROR_HEADER_EXCEPTION_MESSAGE = headerPrefix + "exception.message"
    val ERROR_HEADER_EXCEPTION_STACK_TRACE = headerPrefix + "exception.stacktrace"

    abstract fun send(deadLetterQueueTopic: String, dlqData: DLQData)

    abstract fun close()
}