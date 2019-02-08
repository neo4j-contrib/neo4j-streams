package streams.service

import streams.serialization.JSONUtils

enum class TopicType { CDC_MERGE, CYPHER }

abstract class StreamsSinkService {

    abstract fun getTopicType(topic: String): TopicType?
    abstract fun getCypherTemplate(topic: String): String?
    abstract fun write(query: String, events: Collection<Any>)

    private fun writeCDCTopic(params: Collection<Any>, strategy: CDCQueryStrategy) {
        val data = params
                .map { JSONUtils.asStreamsTransactionEvent(it) }

        strategy.mergeNodeEvents(data).forEach { write(it.query, it.events) }
        strategy.deleteNodeEvents(data).forEach { write(it.query, it.events) }

        strategy.mergeRelationshipEvents(data).forEach { write(it.query, it.events) }
        strategy.deleteRelationshipEvents(data).forEach { write(it.query, it.events) }
    }

    private fun writeWithCypherTemplate(topic: String, params: Collection<Any>) {
        val query = getCypherTemplate(topic) ?: return
        write(query, params)
    }

    fun writeForTopic(topic: String, params: Collection<Any>) {
        when (getTopicType(topic)) {
            TopicType.CYPHER -> writeWithCypherTemplate(topic, params)
            TopicType.CDC_MERGE -> writeCDCTopic(params, MergeCDCQueryStrategy())
            else -> throw RuntimeException("Unknown topic type")
        }
    }
}