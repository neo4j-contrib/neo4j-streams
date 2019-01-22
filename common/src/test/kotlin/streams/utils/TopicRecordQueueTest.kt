package streams.utils

import kotlin.test.Test
import kotlin.test.assertEquals

class TopicRecordQueueTest {

    @Test
    fun `should store a queue for a topic`() {
        val topicRecordQueue = TopicRecordQueue<Int>()
        val topic = "topic"
        topicRecordQueue.add(topic, 1)
        topicRecordQueue.add(topic, 1)
        assertEquals(2, topicRecordQueue.size(topic))
    }

}