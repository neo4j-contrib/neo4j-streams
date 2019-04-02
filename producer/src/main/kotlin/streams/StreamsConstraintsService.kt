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

    private val nodeConstraints = ConcurrentHashMap<String, Set<Constraint>>()
    private val relConstraints = ConcurrentHashMap<String, Set<Constraint>>()

    private lateinit var job: Job

    override fun close() = runBlocking {
        if (::job.isInitialized) {
            job.cancelAndJoin()
        }
    }

    fun start() {
        job = GlobalScope.launch(Dispatchers.IO) {
            while (isActive) {
                StreamsUtils.ignoreExceptions({
                    db.beginTx().use {
                        db.schema().constraints
                                .filter { it.isNodeConstraint() }
                                .groupBy { it.label.name() }
                                .forEach { label, constraints ->
                                    nodeConstraints[label] = constraints
                                            .map { Constraint(label, it.propertyKeys.toSet(), it.streamsConstraintType()) }
                                            .toSet()
                                }
                        db.schema().constraints
                                .filter { it.isRelationshipConstraint() }
                                .groupBy { it.relationshipType.name() }
                                .forEach { relationshipType, constraints ->
                                    relConstraints[relationshipType] = constraints
                                            .map { Constraint(relationshipType, it.propertyKeys.toSet(), it.streamsConstraintType()) }
                                            .toSet()
                                }
                    }
                }, DatabaseShutdownException::class.java)
                delay(poolInterval)
            }
        }
    }

    fun forLabel(label: Label): Set<Constraint> {
        return nodeConstraints[label.name()] ?: emptySet()
    }

    fun forRelationshipType(relationshipType: RelationshipType): Set<Constraint> {
        return relConstraints[relationshipType.name()] ?: emptySet()
    }

    fun allForLabels(): Map<String, Set<Constraint>> {
        return Collections.unmodifiableMap(nodeConstraints)
    }

    fun allForRelationshipType(): Map<String, Set<Constraint>> {
        return Collections.unmodifiableMap(relConstraints)
    }


}