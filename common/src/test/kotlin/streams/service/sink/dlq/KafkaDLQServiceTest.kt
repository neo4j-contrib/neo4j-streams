package streams.service.sink.dlq

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata
import org.apache.kafka.common.record.RecordBatch
import org.junit.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import streams.service.dlq.DLQData
import streams.service.dlq.KafkaDLQService
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals

class KafkaDLQServiceTest {
    @Test
    fun `should send the data to the DLQ`() {
        val producer: MockProducer<ByteArray, ByteArray> = Mockito.mock(MockProducer::class.java) as MockProducer<ByteArray, ByteArray>
        val counter = AtomicInteger(0)
        Mockito.`when`(producer.send(ArgumentMatchers.any<ProducerRecord<ByteArray, ByteArray>>())).then {
            counter.incrementAndGet()
            FutureRecordMetadata(null, 0, RecordBatch.NO_TIMESTAMP, 0L, 0, 0)
        }
        val dlqService = KafkaDLQService(producer)
        val offset = "0"
        val originalTopic = "topicName"
        val partition = "1"
        val timestamp = System.currentTimeMillis()
        val exception = RuntimeException("Test")
        val key = "KEY"
        val value = "VALUE"
        val dlqData = DLQData(offset = offset,
                originalTopic = originalTopic,
                partition = partition,
                timestamp = timestamp,
                exception = exception,
                executingClass = KafkaDLQServiceTest::class.java,
                key = key.toByteArray(),
                value = value.toByteArray())
        dlqService.send("dlqTopic", dlqData)
        assertEquals(1, counter.get())
        dlqService.close()
    }


    @Test
    fun `should create the header map`() {
        val producer: MockProducer<ByteArray, ByteArray> = Mockito.mock(MockProducer::class.java) as MockProducer<ByteArray, ByteArray>
        val dlqService = KafkaDLQService(producer)
        val offset = "0"
        val originalTopic = "topicName"
        val partition = "1"
        val timestamp = System.currentTimeMillis()
        val exception = RuntimeException("Test")
        val key = "KEY"
        val value = "VALUE"
        val dlqData = DLQData(
                offset = offset,
                originalTopic = originalTopic,
                partition = partition,
                timestamp = timestamp,
                exception = exception,
                executingClass = KafkaDLQServiceTest::class.java,
                key = key.toByteArray(),
                value = value.toByteArray()
        )
        val map = dlqService.populateContextHeaders(dlqData)
        assertEquals(String(map["topic"]!!), originalTopic)
        assertEquals(String(map["partition"]!!), partition)
        assertEquals(String(map["offset"]!!), offset)
        assertEquals(String(map["class.name"]!!), KafkaDLQServiceTest::class.java.name)
        assertEquals(String(map["exception.class.name"]!!), exception::class.java.name)
        assertEquals(String(map["exception.message"]!!), exception.message)
        assertEquals(String(map["exception.stacktrace"]!!), ExceptionUtils.getStackTrace(exception))

    }
}