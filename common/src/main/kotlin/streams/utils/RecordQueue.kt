package streams.utils

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue

class RecordQueue<T>: ConcurrentLinkedQueue<T>() {

    fun getOrTimeout(timeout: Long): List<T> {
        val result = mutableListOf<T>()
        val start = System.currentTimeMillis()
        var end = System.currentTimeMillis() - start
        while ((end - start) <= timeout) {
            var record: T = poll()
            end = System.currentTimeMillis()
            if (record == null) {
                continue
            }
            result += record
        }
        if (log.isDebugEnabled) {
            log.debug("Returning batch from queue with size ${result.size}")
        }
        return result
    }


    fun getBatchOrTimeout(batchSize: Int, timeout: Long): List<T> {
        val result = mutableListOf<T>()
        val start = System.currentTimeMillis()
        var end = System.currentTimeMillis() - start
        while (result.size < batchSize && (end - start) <= timeout) {
            var record: T = poll()
            end = System.currentTimeMillis()
            if (record == null) {
                continue
            }
            result += record
        }

        if (log.isDebugEnabled) {
            log.debug("Returning batch from queue with size ${result.size}")
        }
        return result
    }

    companion object {
        private val log = LoggerFactory.getLogger(RecordQueue::class.java)
    }
}