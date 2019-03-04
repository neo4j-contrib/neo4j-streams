package streams

import kotlinx.coroutines.*
import org.neo4j.graphdb.DatabaseShutdownException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import streams.events.Constraint
import streams.utils.StreamsUtils
import java.io.Closeable
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class StreamsConstraintsService(private val db: GraphDatabaseService, private val poolInterval: Long): Closeable {
    override fun close() = runBlocking {
        job.cancelAndJoin()
    }

    private val nodeConstraints = ConcurrentHashMap<Label, Set<Constraint>>()
    private val relConstraints = ConcurrentHashMap<RelationshipType, Set<Constraint>>()

    private val job: Job

    init {
        job = GlobalScope.launch(Dispatchers.IO) {
            while (isActive) {
                StreamsUtils.ignoreExceptions({
                    db.beginTx().use {
                        db.schema().constraints
                                .filter { try { it.label; true } catch (e: IllegalStateException) { false } }
                                .groupBy { it.label }
                                .forEach { label, constraints ->
                                    nodeConstraints[label] = constraints.map { Constraint(label.name(), it.propertyKeys.toSet(), it.constraintType) }.toSet()
                                }
                        db.schema().constraints
                                .filter { try { it.relationshipType; true } catch (e: IllegalStateException) { false } }
                                .groupBy { it.relationshipType }
                                .forEach { relationshipType, constraints ->
                                    relConstraints[relationshipType] = constraints.map { Constraint(relationshipType.name(), it.propertyKeys.toSet(), it.constraintType) }.toSet()
                                }
                    }
                }, DatabaseShutdownException::class.java)
                delay(poolInterval)
            }
        }
    }

    fun forLabel(label: Label): Set<Constraint> {
        return nodeConstraints[label] ?: emptySet()
    }

    fun forRelationshipType(relationshipType: RelationshipType): Set<Constraint> {
        return relConstraints[relationshipType] ?: emptySet()
    }

    fun allForLabels(): Map<Label, Set<Constraint>> {
        return Collections.unmodifiableMap(nodeConstraints)
    }

    fun allForRelationshipType(): Map<RelationshipType, Set<Constraint>> {
        return Collections.unmodifiableMap(relConstraints)
    }


}