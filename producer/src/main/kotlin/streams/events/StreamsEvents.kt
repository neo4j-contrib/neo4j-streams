package streams.events

import kafka.UpdateState
import org.neo4j.graphdb.schema.ConstraintType

enum class UpdateState { created, updated, deleted }

data class Meta(val timestamp: Long,
                val username: String,
                val txId: Long,
                val txEventId: Int,
                val txEventsCount: Int,
                val operation: UpdateState,
                val source: Map<String, Any> = emptyMap())


enum class EntityType { node, relationship }

data class RelationshipNodeChange(val id: Long,
                                  val labels: List<String>?)

abstract class RecordChange{ abstract val properties: Map<String, Any>? }
data class NodeChange(override val properties: Map<String, Any>?,
                      val labels: List<String>?): RecordChange()

data class RelationshipChange(override val properties: Map<String, Any>?,
                              val sourceNode: RelationshipNodeChange,
                              val endNode: RelationshipNodeChange): RecordChange()

abstract class Payload {
    abstract val id: Long
    abstract val type: EntityType
    abstract val before: RecordChange?
    abstract val after: RecordChange?
}
data class NodePayload(override val id: Long,
                       override val before: RecordChange?,
                       override val after: RecordChange?,
                       override val type: EntityType = EntityType.node): Payload()

data class RelationshipPayload(override val id: Long,
                               override val before: RecordChange?,
                               override val after: RecordChange?,
                               val name: String,
                               override val type: EntityType = EntityType.relationship): Payload()

data class Constraint(val label: String?,
                      val name: String?,
                      val properties: List<String>,
                      val type: ConstraintType)

data class Schema(val constraints: List<Constraint>?,
                  val properties: List<String>)

data class StreamsEvent(val meta: Meta, val payload: Payload, val schema: Schema)