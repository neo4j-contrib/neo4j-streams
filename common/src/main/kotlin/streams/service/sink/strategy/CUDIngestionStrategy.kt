package streams.service.sink.strategy

import streams.events.EntityType
import streams.extensions.quote
import streams.serialization.JSONUtils
import streams.service.StreamsSinkEntity
import streams.service.sink.strategy.CUDIngestionStrategy.Companion.FROM_KEY
import streams.service.sink.strategy.CUDIngestionStrategy.Companion.TO_KEY
import streams.utils.IngestionUtils.getLabelsAsString
import streams.utils.IngestionUtils.getNodeKeysAsString
import streams.utils.StreamsUtils


enum class CUDOperations { create, merge, update, delete }

abstract class CUD {
    abstract val op: CUDOperations
    abstract val type: EntityType
    abstract val properties: Map<String, Any?>
}

data class CUDNode(override val op: CUDOperations,
                   override val properties: Map<String, Any?> = emptyMap(),
                   val ids: Map<String, Any?> = emptyMap(),
                   val detach: Boolean = true,
                   val labels: List<String> = emptyList()): CUD() {
    override val type = EntityType.node
    
    fun toMap(): Map<String, Any> {
        return when (op) {
            CUDOperations.delete -> mapOf("ids" to ids)
            else -> mapOf("ids" to ids, "properties" to properties)
        }
    }
}

data class CUDNodeRel(val ids: Map<String, Any?> = emptyMap(),
                      val labels: List<String>)
data class CUDRelationship(override val op: CUDOperations,
                           override val properties: Map<String, Any?> = emptyMap(),
                           val rel_type: String,
                           val from: CUDNodeRel,
                           val to: CUDNodeRel): CUD() {
    override val type = EntityType.relationship

    fun toMap(): Map<String, Any> {
        val from = mapOf("ids" to from.ids)
        val to = mapOf("ids" to to.ids)
        return when (op) {
            CUDOperations.delete -> mapOf(FROM_KEY to from,
                    TO_KEY to to)
            else -> mapOf(FROM_KEY to from,
                    TO_KEY to to,
                    "properties" to properties)
        }
    }
}


class CUDIngestionStrategy: IngestionStrategy {

    companion object {
        @JvmStatic val ID_KEY = "ids"
        @JvmStatic val PHYSICAL_ID_KEY = "_id"
        @JvmStatic val FROM_KEY = "from"
        @JvmStatic val TO_KEY = "to"
    }

    data class NodeRelMetadata(val labels: List<String>, val ids: Set<String>)

    private fun buildNodeLookupByIds(keyword: String = "MATCH", ids: Set<String>, labels: List<String>, identifier: String = "n", field: String = ""): String {
        val fullField = if (field.isNotBlank()) "$field." else field
        val quotedIdentifier = identifier.quote()
        return when (ids.contains(PHYSICAL_ID_KEY)) {
            true -> "MATCH ($quotedIdentifier) WHERE id($quotedIdentifier) = event.$fullField$ID_KEY._id"
            else -> "$keyword ($quotedIdentifier${getLabelsAsString(labels)} {${getNodeKeysAsString(keys = ids, prefix = "$fullField$ID_KEY")}})"
        }
    }

    private fun buildNodeCreateStatement(labels: List<String>): String = """
            |${StreamsUtils.UNWIND}
            |CREATE (n${getLabelsAsString(labels)})
            |SET n = event.properties
        """.trimMargin()

    private fun buildRelCreateStatement(from: NodeRelMetadata, to: NodeRelMetadata,
                                        rel_type: String): String = """
            |${StreamsUtils.UNWIND}
            |${buildNodeLookupByIds(ids = from.ids, labels = from.labels, identifier = FROM_KEY, field = FROM_KEY)}
            |${buildNodeLookupByIds(ids = to.ids, labels = to.labels, identifier = TO_KEY, field = TO_KEY)}
            |CREATE ($FROM_KEY)-[r:${rel_type.quote()}]->($TO_KEY)
            |SET r = event.properties
        """.trimMargin()

    private fun buildNodeMergeStatement(labels: List<String>, ids: Set<String>): String = """
            |${StreamsUtils.UNWIND}
            |${buildNodeLookupByIds(keyword = "MERGE", ids = ids, labels = labels)}
            |SET n += event.properties
        """.trimMargin()

    private fun buildRelMergeStatement(from: NodeRelMetadata, to: NodeRelMetadata,
                                        rel_type: String): String = """
            |${StreamsUtils.UNWIND}
            |${buildNodeLookupByIds(ids = from.ids, labels = from.labels, identifier = FROM_KEY, field = FROM_KEY)}
            |${buildNodeLookupByIds(ids = to.ids, labels = to.labels, identifier = TO_KEY, field = TO_KEY)}
            |MERGE ($FROM_KEY)-[r:${rel_type.quote()}]->($TO_KEY)
            |SET r += event.properties
        """.trimMargin()

    private fun buildNodeUpdateStatement(labels: List<String>, ids: Set<String>): String = """
            |${StreamsUtils.UNWIND}
            |${buildNodeLookupByIds(ids = ids, labels = labels)}
            |SET n += event.properties
        """.trimMargin()

    private fun buildRelUpdateStatement(from: NodeRelMetadata, to: NodeRelMetadata,
                                       rel_type: String): String = """
            |${StreamsUtils.UNWIND}
            |${buildNodeLookupByIds(ids = from.ids, labels = from.labels, identifier = FROM_KEY, field = FROM_KEY)}
            |${buildNodeLookupByIds(ids = to.ids, labels = to.labels, identifier = TO_KEY, field = TO_KEY)}
            |MATCH ($FROM_KEY)-[r:${rel_type.quote()}]->($TO_KEY)
            |SET r += event.properties
        """.trimMargin()

    private fun buildDeleteStatement(labels: List<String>, ids: Set<String>, detach: Boolean): String = """
            |${StreamsUtils.UNWIND}
            |${buildNodeLookupByIds(ids = ids, labels = labels)}
            |${if (detach) "DETACH " else ""}DELETE n
        """.trimMargin()

    private fun buildRelDeleteStatement(from: NodeRelMetadata, to: NodeRelMetadata,
                                        rel_type: String): String = """
            |${StreamsUtils.UNWIND}
            |${buildNodeLookupByIds(ids = from.ids, labels = from.labels, identifier = FROM_KEY, field = FROM_KEY)}
            |${buildNodeLookupByIds(ids = to.ids, labels = to.labels, identifier = TO_KEY, field = TO_KEY)}
            |MATCH ($FROM_KEY)-[r:${rel_type.quote()}]->($TO_KEY)
            |DELETE r
        """.trimMargin()

    private inline fun <reified T: CUD> toCUDEntity(it: Any): T? {
        return when (it) {
            is T -> it
            is Map<*, *> -> {
                val type = it["type"]?.toString()
                val entityType = if (type == null) null else EntityType.valueOf(type)
                when {
                    entityType == null -> throw RuntimeException("No `type` field found")
                    entityType != null && EntityType.node == entityType && T::class.java != CUDNode::class.java -> null
                    entityType != null && EntityType.relationship == entityType && T::class.java != CUDRelationship::class.java -> null
                    else -> JSONUtils.convertValue<T>(it)
                }
            }
            else -> null
        }
    }

    private fun getLabels(relNode: CUDNodeRel) = if (relNode.ids.containsKey(PHYSICAL_ID_KEY)) emptyList() else relNode.labels
    private fun getLabels(node: CUDNode) = if (node.ids.containsKey(PHYSICAL_ID_KEY)) emptyList() else node.labels

    override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        val data = events
                .mapNotNull {
                    it.value?.let {
                        try {
                            val data = toCUDEntity<CUDNode>(it)
                            when (data?.op)  {
                                CUDOperations.delete, null -> null
                                CUDOperations.merge -> if (data.ids.isNotEmpty() && data.properties.isNotEmpty()) data else null // TODO send to the DLQ the null
                                else -> if (data.properties.isNotEmpty()) data else null // TODO send to the DLQ the null
                            }
                        } catch (e: Exception) {
                            null
                        }
                    }
                }
                .groupBy({ it.op }, { it })

        val create = data[CUDOperations.create]
                .orEmpty()
                .groupBy { getLabels(it) }
                .map { QueryEvents(buildNodeCreateStatement(it.key), it.value.map { it.toMap() }) }
        val merge = data[CUDOperations.merge]
                .orEmpty()
                .groupBy { getLabels(it) to it.ids.keys }
                .map { QueryEvents(buildNodeMergeStatement(it.key.first, it.key.second), it.value.map { it.toMap() }) }
        val update = data[CUDOperations.update]
                .orEmpty()
                .groupBy { getLabels(it) to it.ids.keys }
                .map { QueryEvents(buildNodeUpdateStatement(it.key.first, it.key.second), it.value.map { it.toMap() }) }
        return (create + merge + update) // we'll group the data because of in case of `_id` key is present the generated queries are the same for update/merge
                .map { it.query to it.events }
                .groupBy({ it.first }, { it.second })
                .map { QueryEvents(it.key, it.value.flatten()) }
    }

    override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return events
                .mapNotNull {
                    it.value?.let {
                        try {
                            val data = toCUDEntity<CUDNode>(it)
                            when (data?.op)  {
                                CUDOperations.delete -> if (data.ids.isNotEmpty() && data.properties.isEmpty()) data else null // TODO send to the DLQ the null
                                else -> null // TODO send to the DLQ the null
                            }
                        } catch (e: Exception) {
                            null
                        }
                    }
                }
                .groupBy { Triple(it.labels, it.ids.keys, it.detach) }
                .map {
                    val (labels, keys, detach) = it.key
                    QueryEvents(buildDeleteStatement(labels, keys, detach), it.value.map { it.toMap() })
                }
    }

    override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        val data = events
                .mapNotNull {
                    it.value?.let {
                        try {
                            val data = toCUDEntity<CUDRelationship>(it)
                            when (data?.op)  {
                                CUDOperations.delete, null -> null // TODO send to the DLQ the null
                                else -> if (data.from.ids.isNotEmpty() && data.to.ids.isNotEmpty() && data.properties.isNotEmpty()) data else null // TODO send to the DLQ the null
                            }
                        } catch (e: Exception) {
                            null
                        }
                    }
                }
                .groupBy({ it.op }, { it })

        return data.flatMap { (op, list) ->
            list.groupBy { Triple(NodeRelMetadata(getLabels(it.from), it.from.ids.keys), NodeRelMetadata(getLabels(it.to), it.to.ids.keys), it.rel_type) }
                    .map {
                        val (from, to, rel_type) = it.key
                        val query = when (op) {
                            CUDOperations.create -> buildRelCreateStatement(from, to, rel_type)
                            CUDOperations.merge -> buildRelMergeStatement(from, to, rel_type)
                            else -> buildRelUpdateStatement(from, to, rel_type)
                        }
                        QueryEvents(query, it.value.map { it.toMap() })
                    }
        }
    }

    override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return events
                .mapNotNull {
                    it.value?.let {
                        try {
                            val data = toCUDEntity<CUDRelationship>(it)
                            when (data?.op)  {
                                CUDOperations.delete -> if (data.from.ids.isNotEmpty() && data.to.ids.isNotEmpty() && data.properties.isEmpty()) data else null // TODO send to the DLQ the null
                                else -> null // TODO send to the DLQ the null
                            }
                        } catch (e: Exception) {
                            null
                        }
                    }
                }
                .groupBy { Triple(NodeRelMetadata(getLabels(it.from), it.from.ids.keys), NodeRelMetadata(getLabels(it.to), it.to.ids.keys), it.rel_type) }
                .map {
                    val (from, to, rel_type) = it.key
                    QueryEvents(buildRelDeleteStatement(from, to, rel_type), it.value.map { it.toMap() })
                }
    }

}