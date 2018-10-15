package streams.mocks

import org.neo4j.graphdb.*
import org.neo4j.graphdb.event.LabelEntry
import org.neo4j.graphdb.event.PropertyEntry
import org.neo4j.graphdb.event.TransactionData
import java.time.LocalDateTime

data class MockTransactionData(val assignedNodeProperties: MutableIterable<PropertyEntry<Node>> = mutableListOf(),
                               val removedNodeProperties: MutableIterable<PropertyEntry<Node>> = mutableListOf(),
                               val assignedLabels: MutableIterable<LabelEntry> = mutableListOf(),
                               val removedLabels: MutableIterable<LabelEntry> = mutableListOf(),
                               val createdNodes : MutableIterable<Node> = mutableListOf(),
                               val deletedNodes : MutableIterable<Node> = mutableListOf(),
                               val assignedRelationshipProperties: MutableIterable<PropertyEntry<Relationship>> = mutableListOf(),
                               val removedRelationshipProperties: MutableIterable<PropertyEntry<Relationship>> = mutableListOf(),
                               val createdRelationships : MutableIterable<Relationship> = mutableListOf(),
                               val deletedRelationships : MutableIterable<Relationship> = mutableListOf()) : TransactionData {

    override fun getTransactionId(): Long {
        return 123
    }

    override fun getCommitTime(): Long {
        return LocalDateTime.now().nano / 1000L
    }

    override fun isDeleted(node: Node?): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun isDeleted(relationship: Relationship?): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun removedLabels(): MutableIterable<LabelEntry> {
        return removedLabels
    }

    override fun removedRelationshipProperties(): MutableIterable<PropertyEntry<Relationship>> {
        return removedRelationshipProperties
    }

    override fun removedNodeProperties(): MutableIterable<PropertyEntry<Node>> {
        return removedNodeProperties
    }

    override fun assignedLabels(): MutableIterable<LabelEntry> {
        return assignedLabels
    }

    override fun username(): String {
        return "mock"
    }

    override fun deletedNodes(): MutableIterable<Node> {
        return deletedNodes
    }

    override fun metaData(): MutableMap<String, Any> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deletedRelationships(): MutableIterable<Relationship> {
        return deletedRelationships
    }

    override fun createdNodes(): MutableIterable<Node> {
        return createdNodes
    }

    override fun assignedNodeProperties(): MutableIterable<PropertyEntry<Node>> {
        return assignedNodeProperties
    }

    override fun createdRelationships(): MutableIterable<Relationship> {
        return createdRelationships
    }

    override fun assignedRelationshipProperties(): MutableIterable<PropertyEntry<Relationship>> {
        return assignedRelationshipProperties
    }
}

data class MockPropertyEntry<T : PropertyContainer>(val entity: T,
                                                    val key: String,
                                                    val value: Any?,
                                                    val valueBeforeTx: Any?) : PropertyEntry<T> {

    override fun entity(): T {
        return this.entity
    }

    override fun key(): String {
        return this.key
    }

    override fun value(): Any? {
        return this.value
    }

    override fun previouslyCommitedValue(): Any? {
        return this.valueBeforeTx
    }

}

class MockNode(var nodeId : Long = 0, @JvmField var labels : MutableIterable<Label> = mutableListOf(), val properties:  MutableMap<String, Any> = mutableMapOf() ) : Node {

    override fun getId(): Long {
        return nodeId
    }

    override fun hasProperty(key: String?): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getLabels(): MutableIterable<Label> {
        return labels
    }

    override fun getAllProperties(): MutableMap<String, Any> {
        return properties
    }

    override fun addLabel(label: Label?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getGraphDatabase(): GraphDatabaseService {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setProperty(key: String?, value: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun hasLabel(label: Label?): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getDegree(): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getDegree(type: RelationshipType?): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getDegree(direction: Direction?): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getDegree(type: RelationshipType?, direction: Direction?): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getRelationships(): MutableIterable<Relationship> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getRelationships(vararg types: RelationshipType?): MutableIterable<Relationship> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getRelationships(direction: Direction?, vararg types: RelationshipType?): MutableIterable<Relationship> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getRelationships(dir: Direction?): MutableIterable<Relationship> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getRelationships(type: RelationshipType?, dir: Direction?): MutableIterable<Relationship> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun removeLabel(label: Label?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun removeProperty(key: String?): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getProperties(vararg keys: String?): MutableMap<String, Any> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getProperty(key: String?): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getProperty(key: String?, defaultValue: Any?): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getSingleRelationship(type: RelationshipType?, dir: Direction?): Relationship {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getRelationshipTypes(): MutableIterable<RelationshipType> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createRelationshipTo(otherNode: Node?, type: RelationshipType?): Relationship {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getPropertyKeys(): MutableIterable<String> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun hasRelationship(): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun hasRelationship(vararg types: RelationshipType?): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun hasRelationship(direction: Direction?, vararg types: RelationshipType?): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun hasRelationship(dir: Direction?): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun hasRelationship(type: RelationshipType?, dir: Direction?): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun delete() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

class MockLabelEntry(val label:Label, val node:Node) : LabelEntry {
    override fun label(): Label {
        return label
    }

    override fun node(): Node {
        return node
    }

}

class MockRelationship(private val id: Long, private val type: String, private val startNode: Node, private val endNode: Node,
                       val properties:  MutableMap<String, Any> = mutableMapOf()): Relationship {
    override fun hasProperty(p0: String?): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAllProperties(): MutableMap<String, Any> {
        return properties
    }

    override fun getGraphDatabase(): GraphDatabaseService {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun setProperty(p0: String?, p1: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getId(): Long {
        return id
    }

    override fun getType(): RelationshipType {
        return RelationshipType.withName(type)
    }

    override fun removeProperty(p0: String?): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getProperties(vararg p0: String?): MutableMap<String, Any> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getProperty(p0: String?): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getProperty(p0: String?, p1: Any?): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getNodes(): Array<Node> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getOtherNode(p0: Node?): Node {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getStartNode(): Node {
        return startNode
    }

    override fun isType(p0: RelationshipType?): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getEndNode(): Node {
        return endNode
    }

    override fun getPropertyKeys(): MutableIterable<String> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun delete() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}