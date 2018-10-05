package streams.mocks

import org.neo4j.graphdb.*
import org.neo4j.graphdb.event.LabelEntry
import org.neo4j.graphdb.event.PropertyEntry
import org.neo4j.graphdb.event.TransactionData

data class MockTransactionData(val assignedNodeProperties: MutableIterable<PropertyEntry<Node>> = mutableListOf(),
                               val removedNodeProperties: MutableIterable<PropertyEntry<Node>> = mutableListOf(),
                               val assignedLabels: MutableIterable<LabelEntry> = mutableListOf(),
                               val removedLabels: MutableIterable<LabelEntry> = mutableListOf()) : TransactionData {

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
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun removedNodeProperties(): MutableIterable<PropertyEntry<Node>> {
        return removedNodeProperties
    }

    override fun assignedLabels(): MutableIterable<LabelEntry> {
        return assignedLabels
    }

    override fun username(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deletedNodes(): MutableIterable<Node> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun metaData(): MutableMap<String, Any> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deletedRelationships(): MutableIterable<Relationship> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createdNodes(): MutableIterable<Node> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun assignedNodeProperties(): MutableIterable<PropertyEntry<Node>> {
        return assignedNodeProperties
    }

    override fun createdRelationships(): MutableIterable<Relationship> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun assignedRelationshipProperties(): MutableIterable<PropertyEntry<Relationship>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
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

class MockNode(var nodeId : Long = 0, @JvmField var labels : MutableIterable<Label> = mutableListOf() ) : Node {

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
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
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

class MockLabelEntry(val label:Label, val node:Node) : LabelEntry{
    override fun label(): Label {
        return label
    }

    override fun node(): Node {
        return node
    }

}