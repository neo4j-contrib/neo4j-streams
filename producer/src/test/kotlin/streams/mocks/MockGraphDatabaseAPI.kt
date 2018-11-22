package streams.mocks

import org.neo4j.graphdb.*
import org.neo4j.graphdb.event.KernelEventHandler
import org.neo4j.graphdb.event.TransactionEventHandler
import org.neo4j.graphdb.index.IndexManager
import org.neo4j.graphdb.schema.Schema
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription
import org.neo4j.graphdb.traversal.TraversalDescription
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.logging.LogService
import org.neo4j.kernel.impl.logging.NullLogService
import org.neo4j.kernel.impl.store.StoreId
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import streams.*
import java.io.File
import java.lang.IllegalArgumentException
import java.net.URL
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

class MockGraphDatabaseAPI(private val dependencyResolver: MockDependencyResolver = MockDependencyResolver()) : GraphDatabaseAPI {
    override fun createNode(): Node {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createNode(vararg labels: Label?): Node {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <T : Any?> unregisterTransactionEventHandler(handler: TransactionEventHandler<T>?): TransactionEventHandler<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun index(): IndexManager {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun bidirectionalTraversalDescription(): BidirectionalTraversalDescription {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun registerKernelEventHandler(handler: KernelEventHandler?): KernelEventHandler {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getNodeById(id: Long): Node {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun beginTransaction(type: Transaction.Type?, loginContext: LoginContext?): InternalTransaction {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun beginTransaction(type: Transaction.Type?, loginContext: LoginContext?, timeout: Long, unit: TimeUnit?): InternalTransaction {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAllLabels(): ResourceIterable<Label> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun beginTx(): org.neo4j.graphdb.Transaction {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun beginTx(timeout: Long, unit: TimeUnit?): org.neo4j.graphdb.Transaction {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAllNodes(): ResourceIterable<Node> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAllLabelsInUse(): ResourceIterable<Label> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAllRelationshipTypes(): ResourceIterable<RelationshipType> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getStoreDir(): File {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAllRelationships(): ResourceIterable<Relationship> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun findNodes(label: Label?, key: String?, value: Any?): ResourceIterator<Node> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun findNodes(label: Label?): ResourceIterator<Node> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <T : Any?> registerTransactionEventHandler(handler: TransactionEventHandler<T>?): TransactionEventHandler<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createNodeId(): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun traversalDescription(): TraversalDescription {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun execute(query: String?): Result {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun execute(query: String?, timeout: Long, unit: TimeUnit?): Result {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun execute(query: String?, parameters: MutableMap<String, Any>?): Result {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun execute(query: String?, parameters: MutableMap<String, Any>?, timeout: Long, unit: TimeUnit?): Result {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun shutdown() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getRelationshipById(id: Long): Relationship {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun findNode(label: Label?, key: String?, value: Any?): Node {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAllPropertyKeys(): ResourceIterable<String> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun validateURLAccess(url: URL?): URL {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun unregisterKernelEventHandler(handler: KernelEventHandler?): KernelEventHandler {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun schema(): Schema {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun isAvailable(timeout: Long): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getDependencyResolver(): DependencyResolver {
        return dependencyResolver
    }

    override fun storeId(): StoreId {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAllRelationshipTypesInUse(): ResourceIterable<RelationshipType> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

class MockDependencyResolver(private val nodeRouting: List<NodeRoutingConfiguration> = listOf(NodeRoutingConfiguration()),
                             private val relRouting: List<RelationshipRoutingConfiguration> = listOf(RelationshipRoutingConfiguration())): DependencyResolver {
    override fun <T : Any?> resolveDependency(type: Class<T>?): T {
        if (type == StreamsEventRouterLifecycle::class.java) {
            val dependencies = MockDependencies()
            val lifecycle = StreamsEventRouterLifecycle(db = dependencies.graphdatabaseAPI(),
                    log = dependencies.log(), streamHandler = MockStreamsEventRouter(),
                    streamsEventRouterConfiguration = StreamsEventRouterConfiguration(nodeRouting = nodeRouting, relRouting = relRouting))
            return lifecycle as T
        }
        throw IllegalArgumentException("mock")
    }

    override fun <T : Any?> resolveDependency(type: Class<T>?, selector: DependencyResolver.SelectionStrategy?): T {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <T : Any?> provideDependency(type: Class<T>?, selector: DependencyResolver.SelectionStrategy?): Supplier<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <T : Any?> provideDependency(type: Class<T>?): Supplier<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

class MockDependencies: StreamsExtensionFactory.Dependencies {
    override fun graphdatabaseAPI(): GraphDatabaseAPI {
        return MockGraphDatabaseAPI()
    }

    override fun log(): LogService {
        return NullLogService.getInstance()
    }

    override fun config(): Config {
        return Config.defaults()
    }

}