package streams.utils

import java.util.concurrent.ConcurrentHashMap

class TopicRecordQueue<T>: ConcurrentHashMap<String, RecordQueue<T>>() {

    private fun getQueue(topic: String): RecordQueue<T> {
        return getOrPut(topic) { RecordQueue() }
    }

    fun add(topic: String, elem: T): Boolean {
        return getQueue(topic).add(elem)
    }

    fun addAll(topic: String, elems: Collection<T>): Boolean {
        return getQueue(topic).addAll(elems)
    }

    fun addMap(map: Map<String, Collection<T>>) {
        map.forEach {
            getQueue(it.key).addAll(it.value)
        }
    }

    fun getBatchOrTimeout(topic: String, batchSize: Int, timeout: Long): List<T> {
        return getQueue(topic).getBatchOrTimeout(batchSize, timeout)
    }

    fun size(topic: String): Int {
        return getQueue(topic).size
    }

}