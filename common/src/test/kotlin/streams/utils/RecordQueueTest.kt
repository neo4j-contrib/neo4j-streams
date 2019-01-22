package streams.utils

import kotlinx.coroutines.*
import org.junit.Test
import kotlin.test.assertEquals


class RecordQueueTest {
    @Test
    fun `should return all the data`() = runBlocking {
        val queue = RecordQueue<Int>()

        GlobalScope.launch {
            repeat(10){
                queue.add(it)
                delay(500)
            }
        }

        val batchA = queue.getBatchOrTimeout(5, 10000)
        assertEquals(listOf(0, 1, 2, 3, 4), batchA)

        val batchB = queue.getBatchOrTimeout(5, 10000)
        assertEquals(listOf(5, 6, 7, 8, 9), batchB)
    }

    @Test
    fun `should not return all the data`() = runBlocking {
        val queue = RecordQueue<Int>()

        GlobalScope.launch {
            repeat(10){
                queue.add(it)
                delay(500)
            }
        }

        val data = queue.getBatchOrTimeout(5, 100)
        assertEquals(listOf(0), data)
    }
}