package integrations.kafka

import kotlinx.coroutines.runBlocking
import extension.newDatabase
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.CoreMatchers
import org.hamcrest.Matchers
import org.junit.Before
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.rules.RuleChain
import org.neo4j.adversaries.ClassGuardedAdversary
import org.neo4j.adversaries.CountingAdversary
import org.neo4j.function.ThrowingSupplier
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.NotFoundException
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.graphdb.TransactionFailureException
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.graphdb.factory.GraphDatabaseBuilder
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.factory.module.PlatformModule
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction
import org.neo4j.helpers.ArrayUtil
import org.neo4j.helpers.collection.BoundedIterable
import org.neo4j.helpers.collection.Iterables
import org.neo4j.helpers.collection.Iterators
import org.neo4j.internal.kernel.api.IndexCapability
import org.neo4j.internal.kernel.api.InternalIndexState
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.pagecache.IOLimiter
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException
import org.neo4j.kernel.api.index.IndexAccessor
import org.neo4j.kernel.api.index.IndexEntryUpdate
import org.neo4j.kernel.api.index.IndexPopulator
import org.neo4j.kernel.api.index.IndexProvider
import org.neo4j.kernel.api.index.IndexUpdater
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.annotations.ReporterFactory
import org.neo4j.kernel.impl.api.index.IndexUpdateMode
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig
import org.neo4j.kernel.impl.index.schema.ByteBufferFactory
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.impl.store.RecordStore
import org.neo4j.kernel.impl.store.StoreFactory
import org.neo4j.kernel.impl.store.StoreType
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord
import org.neo4j.kernel.impl.store.record.RecordLoad
import org.neo4j.kernel.impl.storemigration.ExistingTargetStrategy
import org.neo4j.kernel.impl.storemigration.FileOperation
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant
import org.neo4j.kernel.impl.transaction.command.Command
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder
import org.neo4j.kernel.internal.DatabaseHealth
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.monitoring.Monitors
import org.neo4j.kernel.recovery.RecoveryMonitor
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.NullLog
import org.neo4j.logging.NullLogProvider
import org.neo4j.scheduler.ThreadPoolJobScheduler
import org.neo4j.storageengine.api.NodePropertyAccessor
import org.neo4j.storageengine.api.StorageEngine
import org.neo4j.storageengine.api.schema.IndexReader
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor
import org.neo4j.test.AdversarialPageCacheGraphDatabaseFactory
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.test.TestGraphDatabaseFactoryState
import org.neo4j.test.TestLabels
import org.neo4j.test.assertion.Assert
import org.neo4j.test.rule.RandomRule
import org.neo4j.test.rule.TestDirectory
import org.neo4j.test.rule.fs.DefaultFileSystemRule
import streams.events.StreamsPluginStatus
import streams.serialization.JSONUtils
import java.io.File
import java.io.IOException
import java.io.UncheckedIOException
import java.util.ArrayList
import java.util.Arrays
import java.util.HashMap
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

class KafkaDatabaseRecovery: KafkaEventSinkBase() {

    private val TOKENS = arrayOf("Token1", "Token2", "Token3", "Token4", "Token5")

    private val directory = TestDirectory.testDirectory()
    private val fileSystemRule = DefaultFileSystemRule()
    private val random = RandomRule()
    private val logProvider = AssertableLogProvider(true)

    @Rule @JvmField
    var rules = RuleChain.outerRule(random).around(fileSystemRule).around(directory)
    
    var topic: String? = null

    @Before
    fun setTopic() {
        topic = UUID.randomUUID().toString()
    }

    @Test
    @Throws(IOException::class)
    fun idGeneratorsRebuildAfterRecovery() {
        val database = startDatabase(directory.databaseDir())
        val numberOfNodes = 10
        database.beginTx().use { transaction ->
            for (nodeIndex in 0 until numberOfNodes) {
                database.createNode()
            }
            transaction.success()
        }

        // copying only transaction log simulate non clean shutdown db that should be able to recover just from logs
        val restoreDbStoreDir = copyTransactionLogs()
        val recoveredDatabase = startDatabase(restoreDbStoreDir, true, StreamsPluginStatus.STOPPED)
        sendKafkaEvents()
        recoveredDatabase.beginTx().use { tx ->
            org.junit.Assert.assertEquals(numberOfNodes.toLong(), Iterables.count(recoveredDatabase.allNodes))

            // Make sure id generator has been rebuilt so this doesn't throw null pointer exception
            recoveredDatabase.createNode()
        }
        database.shutdown()
        recoveredDatabase.shutdown()
    }

    @Test
    @Throws(IOException::class)
    fun shouldRecoverIdsCorrectlyWhenWeCreateAndDeleteANodeInTheSameRecoveryRun() {
        val database = startDatabase(directory.databaseDir())
        val testLabel = Label.label("testLabel")
        val propertyToDelete = "propertyToDelete"
        val validPropertyName = "validProperty"
        database.beginTx().use { transaction ->
            val node = database.createNode()
            node.addLabel(testLabel)
            transaction.success()
        }
        database.beginTx().use { transaction ->
            val node = findNodeByLabel(database, testLabel)
            node.setProperty(propertyToDelete, createLongString())
            node.setProperty(validPropertyName, createLongString())
            transaction.success()
        }
        database.beginTx().use { transaction ->
            val node = findNodeByLabel(database, testLabel)
            node.removeProperty(propertyToDelete)
            transaction.success()
        }

        // copying only transaction log simulate non clean shutdown db that should be able to recover just from logs
        val restoreDbStoreDir = copyTransactionLogs()

        // database should be restored and node should have expected properties
        sendKafkaEvents()
        val recoveredDatabase = startDatabase(restoreDbStoreDir)
        recoveredDatabase.beginTx().use { ignored ->
            val node = findNodeByLabel(recoveredDatabase, testLabel)
            org.junit.Assert.assertFalse(node.hasProperty(propertyToDelete))
            org.junit.Assert.assertTrue(node.hasProperty(validPropertyName))
        }
        database.shutdown()
        recoveredDatabase.shutdown()
    }

    @Test(timeout = 60000)
    fun recoveryShouldFixPartiallyAppliedSchemaIndexUpdates() {
        val label = Label.label("Foo")
        val property = "Bar"

        // cause failure during 'relationship.delete()' command application
        val adversary = ClassGuardedAdversary(CountingAdversary(1, true),
                Command.RelationshipCommand::class.java)
        adversary.disable()
        val databaseDir = directory.databaseDir()
        var db = AdversarialPageCacheGraphDatabaseFactory.create(fileSystemRule.get(), adversary)
                .newEmbeddedDatabaseBuilder(databaseDir)
                .newDatabase(StreamsPluginStatus.STOPPED)
        try {
            db.beginTx().use { tx ->
                db.schema().constraintFor(label).assertPropertyIsUnique(property).create()
                tx.success()
            }
            val relationshipId = createRelationship(db)
            var txFailure: TransactionFailureException? = null
            try {
                db.beginTx().use { tx ->
                    val node = db.createNode(label)
                    node.setProperty(property, "B")
                    db.getRelationshipById(relationshipId).delete() // this should fail because of the adversary
                    tx.success()
                    adversary.enable()
                }
            } catch (e: TransactionFailureException) {
                txFailure = e
            }
            org.junit.Assert.assertNotNull(txFailure)
            adversary.disable()
            healthOf(db).healed() // heal the db so it is possible to inspect the data
            db.beginTx().use { tx ->
                org.junit.Assert.assertNotNull(findNode(db, label, property, "B"))
                org.junit.Assert.assertNotNull(db.getRelationshipById(relationshipId))
                tx.success()
            }
            healthOf(db).panic(txFailure!!.cause) // panic the db again to force recovery on the next startup

            // restart the database, now with regular page cache
            val databaseDirectory = (db as GraphDatabaseAPI).databaseLayout().databaseDirectory()
            db.shutdown()
            db = startDatabase(databaseDirectory)
            db.beginTx().use { tx ->
                org.junit.Assert.assertNotNull(findNode(db, label, property, "B"))
                assertRelationshipNotExist(db, relationshipId)
                tx.success()
            }
        } finally {
            db.shutdown()
        }
    }

    @Test
    @Throws(Exception::class)
    fun shouldSeeSameIndexUpdatesDuringRecoveryAsFromNormalIndexApplication() {
        // Previously indexes weren't really participating in recovery, instead there was an after-phase
        // where nodes that was changed during recovery were reindexed. Do be able to do this reindexing
        // the index had to support removing arbitrary entries based on node id alone. Lucene can do this,
        // but at least at the time of writing this not the native index. For this the recovery process
        // was changed to rewind neostore back to how it looked at the last checkpoint and then replay
        // transactions from that point, including indexes. This test verifies that there's no mismatch
        // between applying transactions normally and recovering them after a crash, index update wise.

        // given
        val storeDir = directory.absolutePath()
        val fs = EphemeralFileSystemAbstraction()
        val updateCapturingIndexProvider = UpdateCapturingIndexProvider(IndexProvider.EMPTY, HashMap())
        var db = startDatabase(storeDir, fs, updateCapturingIndexProvider)
        val label: Label = TestLabels.LABEL_ONE
        val key1 = "key1"
        val key2 = "key2"
        db.beginTx().use { tx ->
            db.schema().indexFor(label).on(key1).create()
            db.schema().indexFor(label).on(key1).on(key2).create()
            tx.success()
        }
        db.beginTx().use { tx ->
            db.schema().awaitIndexesOnline(10, TimeUnit.SECONDS)
            tx.success()
        }
        checkPoint(db)
        produceRandomNodePropertyAndLabelUpdates(db, random.intBetween(20, 40), label, key1, key2)
        checkPoint(db)
        val updatesAtLastCheckPoint = updateCapturingIndexProvider.snapshot()

        // when
        produceRandomNodePropertyAndLabelUpdates(db, random.intBetween(40, 100), label, key1, key2)

        // Snapshot
        flush(db)
        val crashedFs = fs.snapshot()
        val updatesAtCrash = updateCapturingIndexProvider.snapshot()

        // Crash and start a new
        val recoveredUpdateCapturingIndexProvider = UpdateCapturingIndexProvider(IndexProvider.EMPTY, updatesAtLastCheckPoint)
        val lastCommittedTxIdBeforeRecovered = lastCommittedTxId(db)
        db.shutdown()
        fs.close()
        // sendKafkaEvents()
        db = startDatabase(storeDir, crashedFs, recoveredUpdateCapturingIndexProvider)
        val lastCommittedTxIdAfterRecovered = lastCommittedTxId(db)
        val updatesAfterRecovery = recoveredUpdateCapturingIndexProvider.snapshot()

        // then
        org.junit.Assert.assertEquals(lastCommittedTxIdBeforeRecovered, lastCommittedTxIdAfterRecovered)
        assertSameUpdates(updatesAtCrash, updatesAfterRecovery)
        db.shutdown()
        crashedFs.close()
    }

    private fun sendKafkaEvents() {
        val totalRecords = 10L
        (1..totalRecords).forEach {
            val dataProperties = mapOf("prop1" to "foo $it", "bar" to it)
            val data = mapOf("id" to it, "properties" to dataProperties)
            val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
            val metadata = kafkaProducer.send(producerRecord).get()
            println("Sent record $it to topic ${metadata.topic()}")
        }
    }

    @Test
    @Throws(Exception::class)
    fun shouldSeeTheSameRecordsAtCheckpointAsAfterReverseRecovery() {
        // given
        val fs = EphemeralFileSystemAbstraction()
        val db = TestGraphDatabaseFactory()
                .setFileSystem(fs)
                .newImpermanentDatabaseBuilder(directory.databaseDir())
                .setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
                .setConfig("kafka.bootstrap.servers", KafkaEventSinkSuiteIT.kafka.bootstrapServers)
                .setConfig("kafka.zookeeper.connect", KafkaEventSinkSuiteIT.kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"])
                .setConfig("streams.sink.enabled", "true")
                .newDatabase()
        produceRandomGraphUpdates(db, 100)
        checkPoint(db)
        val checkPointFs = fs.snapshot()

        // when
        produceRandomGraphUpdates(db, 100)
        flush(db)
        val crashedFs = fs.snapshot()
        db.shutdown()
        fs.close()
        sendKafkaEvents()
        val monitors = Monitors()
        val pageCache = AtomicReference<PageCache>()
        val reversedFs = AtomicReference<EphemeralFileSystemAbstraction>()
        monitors.addMonitorListener(object : RecoveryMonitor {
            override fun reverseStoreRecoveryCompleted(checkpointTxId: Long) {
                try {
                    // Flush the page cache which will fished out of the PlatformModule at the point of constructing the database
                    pageCache.get().flushAndForce()
                } catch (e: IOException) {
                    throw UncheckedIOException(e)
                }

                // The stores should now be equal in content to the db as it was right after the checkpoint.
                // Grab a snapshot so that we can compare later.
                reversedFs.set(crashedFs.snapshot())
            }
        })
        object : TestGraphDatabaseFactory() {
            // This nested constructing is done purely to be able to fish out PlatformModule
            // (and its PageCache inside it). It would be great if this could be done in a prettier way.
            override fun createImpermanentDatabaseCreator(storeDir: File, state: TestGraphDatabaseFactoryState): GraphDatabaseBuilder.DatabaseCreator {
                return object : GraphDatabaseBuilder.DatabaseCreator {
                    override fun newDatabase(config: Config): GraphDatabaseService {
                        val factory: TestGraphDatabaseFacadeFactory = object : TestGraphDatabaseFacadeFactory(state, true) {
                            override fun createPlatform(storeDir: File, config: Config, dependencies: Dependencies): PlatformModule {
                                val platform = super.createPlatform(storeDir, config, dependencies)
                                // nice way of getting the page cache dependency before db is created, huh?
                                pageCache.set(platform.pageCache)
                                return platform
                            }
                        }
                        return factory.newFacade(storeDir, config, GraphDatabaseDependencies.newDependencies(state.databaseDependencies()))
                    }
                }
            }
        }
                .setFileSystem(crashedFs)
                .setMonitors(monitors)
                .newImpermanentDatabaseBuilder(directory.databaseDir())
                .setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
                .setConfig("kafka.bootstrap.servers", KafkaEventSinkSuiteIT.kafka.bootstrapServers)
                .setConfig("kafka.zookeeper.connect", KafkaEventSinkSuiteIT.kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"])
                .setConfig("streams.sink.enabled", "true")
                .newDatabase()
                .shutdown()

        // then
        fs.close()
        try {
            // Here we verify that the neostore contents, record by record are exactly the same when comparing
            // the store as it was right after the checkpoint with the store as it was right after reverse recovery completed.
            assertSameStoreContents(checkPointFs, reversedFs.get(), directory.databaseLayout())
        } finally {
            checkPointFs.close()
            reversedFs.get().close()
        }
    }

    private fun lastCommittedTxId(db: GraphDatabaseService): Long {
        return (db as GraphDatabaseAPI).dependencyResolver.resolveDependency(TransactionIdStore::class.java).lastClosedTransactionId
    }

    private fun assertSameStoreContents(fs1: EphemeralFileSystemAbstraction, fs2: EphemeralFileSystemAbstraction, databaseLayout: DatabaseLayout) {
        val logProvider = NullLogProvider.getInstance()
        val contextSupplier = EmptyVersionContextSupplier.EMPTY
        ThreadPoolJobScheduler().use { jobScheduler ->
            ConfiguringPageCacheFactory(fs1, Config.defaults(), PageCacheTracer.NULL,
                    PageCursorTracerSupplier.NULL, NullLog.getInstance(), contextSupplier, jobScheduler)
                    .orCreatePageCache.use { pageCache1 ->
                        ConfiguringPageCacheFactory(fs2, Config.defaults(), PageCacheTracer.NULL,
                                PageCursorTracerSupplier.NULL, NullLog.getInstance(), contextSupplier, jobScheduler)
                                .orCreatePageCache.use { pageCache2 ->
                                    StoreFactory(databaseLayout, Config.defaults(), DefaultIdGeneratorFactory(fs1),
                                            pageCache1, fs1, logProvider, contextSupplier).openAllNeoStores().use { store1 ->
                                        StoreFactory(databaseLayout, Config.defaults(), DefaultIdGeneratorFactory(fs2),
                                                pageCache2, fs2, logProvider, contextSupplier).openAllNeoStores().use { store2 ->
                                            for (storeType in StoreType.values()) {
                                                if (storeType.isRecordStore) {
                                                    assertSameStoreContents(store1.getRecordStore(storeType), store2.getRecordStore<AbstractBaseRecord>(storeType))
                                                }
                                            }
                                        }
                                    }
                                }
                    }
        }
    }

    private fun <RECORD : AbstractBaseRecord?> assertSameStoreContents(store1: RecordStore<RECORD>, store2: RecordStore<RECORD>) {
        val highId1 = store1.highId
        val highId2 = store2.highId
        val maxHighId = java.lang.Long.max(highId1, highId2)
        val record1 = store1.newRecord()
        val record2 = store2.newRecord()
        for (id in store1.numberOfReservedLowIds until maxHighId) {
            store1.getRecord(id, record1, RecordLoad.CHECK)
            store2.getRecord(id, record2, RecordLoad.CHECK)
            org.junit.Assert.assertEquals(record1, record2)
        }
    }

    private fun flush(db: GraphDatabaseService) {
        (db as GraphDatabaseAPI).dependencyResolver.resolveDependency(StorageEngine::class.java).flushAndForce(IOLimiter.UNLIMITED)
    }

    @Throws(IOException::class)
    private fun checkPoint(db: GraphDatabaseService) {
        (db as GraphDatabaseAPI).dependencyResolver.resolveDependency(CheckPointer::class.java)
                .forceCheckPoint(SimpleTriggerInfo("Manual trigger"))
    }

    private fun produceRandomGraphUpdates(db: GraphDatabaseService, numberOfTransactions: Int) {
        // Load all existing nodes
        val nodes: MutableList<Node> = ArrayList()
        db.beginTx().use { tx ->
            db.allNodes.iterator().use { allNodes ->
                while (allNodes.hasNext()) {
                    nodes.add(allNodes.next())
                }
            }
            tx.success()
        }
        for (i in 0 until numberOfTransactions) {
            val transactionSize = random.intBetween(1, 30)
            db.beginTx().use { tx ->
                for (j in 0 until transactionSize) {
                    val operationType = random.nextFloat()
                    val operation = random.nextFloat()
                    if (operationType < 0.5) {   // create
                        if (operation < 0.5) {   // create node (w/ random label, prop)
                            val node = db.createNode(*if (random.nextBoolean()) ArrayUtil.array(randomLabel()) else arrayOfNulls<Label>(0))
                            if (random.nextBoolean()) {
                                node.setProperty(randomKey(), random.nextValueAsObject())
                            }
                        } else {   // create relationship (w/ random prop)
                            if (!nodes.isEmpty()) {
                                val relationship = random.among(nodes)
                                        .createRelationshipTo(random.among(nodes), randomRelationshipType())
                                if (random.nextBoolean()) {
                                    relationship.setProperty(randomKey(), random.nextValueAsObject())
                                }
                            }
                        }
                    } else if (operationType < 0.8) {   // change
                        if (operation < 0.25) {   // add label
                            random.among(nodes) { node: Node -> node.addLabel(randomLabel()) }
                        } else if (operation < 0.5) {   // remove label
                            random.among(nodes) { node: Node -> node.removeLabel(randomLabel()) }
                        } else if (operation < 0.75) {   // set node property
                            random.among(nodes) { node: Node -> node.setProperty(randomKey(), random.nextValueAsObject()) }
                        } else {   // set relationship property
                            onRandomRelationship(nodes,
                                    Consumer { relationship: Relationship -> relationship.setProperty(randomKey(), random.nextValueAsObject()) })
                        }
                    } else {   // delete
                        if (operation < 0.25) {   // remove node property
                            random.among(nodes) { node: Node -> node.removeProperty(randomKey()) }
                        } else if (operation < 0.5) {   // remove relationship property
                            onRandomRelationship(nodes, Consumer { relationship: Relationship -> relationship.removeProperty(randomKey()) })
                        } else if (operation < 0.9) {   // delete relationship
                            onRandomRelationship(nodes, Consumer { obj: Relationship -> obj.delete() })
                        } else {   // delete node
                            random.among(nodes) { node: Node ->
                                for (relationship in node.relationships) {
                                    relationship.delete()
                                }
                                node.delete()
                                nodes.remove(node)
                            }
                        }
                    }
                }
                tx.success()
            }
        }
    }

    private fun onRandomRelationship(nodes: List<Node>, action: Consumer<Relationship>) {
        random.among(nodes) { node: Node -> random.among(Iterables.asList(node.relationships), action) }
    }

    private fun randomRelationshipType(): RelationshipType? {
        return RelationshipType.withName(random.among(TOKENS))
    }

    private fun randomKey(): String? {
        return random.among(TOKENS)
    }

    private fun randomLabel(): Label {
        return Label.label(random.among(TOKENS))
    }

    private fun assertSameUpdates(updatesAtCrash: Map<Long?, Collection<IndexEntryUpdate<*>>>,
                                  recoveredUpdatesSnapshot: Map<Long?, Collection<IndexEntryUpdate<*>>>) {
        // The UpdateCapturingIndexProvider just captures updates made to indexes. The order in this test
        // should be the same during online transaction application and during recovery since everything
        // is single threaded. However there's a bunch of placing where entries and keys and what not
        // ends up in hash maps and so may change order. The super important thing we need to verify is
        // that updates for a particular transaction are the same during normal application and recovery,
        // regardless of ordering differences within the transaction.
        val crashUpdatesPerNode = splitPerNode(updatesAtCrash)
        val recoveredUpdatesPerNode = splitPerNode(recoveredUpdatesSnapshot)
        org.junit.Assert.assertEquals(crashUpdatesPerNode, recoveredUpdatesPerNode)
    }

    private fun splitPerNode(updates: Map<Long?, Collection<IndexEntryUpdate<*>>>): Map<Long?, Map<Long, MutableCollection<IndexEntryUpdate<*>>>> {
        val result: MutableMap<Long?, Map<Long, MutableCollection<IndexEntryUpdate<*>>>> = HashMap()
        updates.forEach { (indexId: Long?, indexUpdates: Collection<IndexEntryUpdate<*>>) -> result[indexId] = splitPerNode(indexUpdates) }
        return result
    }

    private fun splitPerNode(updates: Collection<IndexEntryUpdate<*>>): Map<Long, MutableCollection<IndexEntryUpdate<*>>> {
        val perNode: MutableMap<Long, MutableCollection<IndexEntryUpdate<*>>> = HashMap()
        updates.forEach(Consumer { update: IndexEntryUpdate<*> -> perNode.computeIfAbsent(update.entityId) { nodeId: Long? -> ArrayList() }.add(update) })
        return perNode
    }

    private fun produceRandomNodePropertyAndLabelUpdates(db: GraphDatabaseService, numberOfTransactions: Int, label: Label, vararg keys: String) {
        // Load all existing nodes
        val nodes: MutableList<Node> = ArrayList()
        db.beginTx().use { tx ->
            db.allNodes.iterator().use { allNodes ->
                while (allNodes.hasNext()) {
                    nodes.add(allNodes.next())
                }
            }
            tx.success()
        }
        for (i in 0 until numberOfTransactions) {
            val transactionSize = random.intBetween(1, 30)
            db.beginTx().use { tx ->
                for (j in 0 until transactionSize) {
                    val operation = random.nextFloat()
                    if (operation < 0.1) {   // Delete node
                        if (!nodes.isEmpty()) {
                            nodes.removeAt(random.nextInt(nodes.size)).delete()
                        }
                    } else if (operation < 0.3) {   // Create node
                        val node = db.createNode(*if (random.nextBoolean()) ArrayUtil.array(label) else arrayOfNulls<Label>(0))
                        for (key in keys) {
                            if (random.nextBoolean()) {
                                node.setProperty(key, random.nextValueAsObject())
                            }
                        }
                        nodes.add(node)
                    } else if (operation < 0.4) {   // Remove label
                        random.among(nodes) { node: Node -> node.removeLabel(label) }
                    } else if (operation < 0.6) {   // Add label
                        random.among(nodes) { node: Node -> node.addLabel(label) }
                    } else if (operation < 0.85) {   // Set property
                        random.among(nodes) { node: Node -> node.setProperty(random.among(keys), random.nextValueAsObject()) }
                    } else {   // Remove property
                        random.among(nodes) { node: Node -> node.removeProperty(random.among(keys)) }
                    }
                }
                tx.success()
            }
        }
    }

    private fun findNodeByLabel(database: GraphDatabaseService, testLabel: Label): Node {
        database.findNodes(testLabel).use { nodes -> return nodes.next() }
    }

    private fun findNode(db: GraphDatabaseService, label: Label, property: String, value: String): Node? {
        db.findNodes(label, property, value).use { nodes -> return Iterators.single(nodes) }
    }

    private fun createRelationship(db: GraphDatabaseService): Long {
        var relationshipId: Long = -1
        db.beginTx().use { tx ->
            val start = db.createNode(Label.label(System.currentTimeMillis().toString() + ""))
            val end = db.createNode(Label.label(System.currentTimeMillis().toString() + ""))
            relationshipId = start.createRelationshipTo(end, RelationshipType.withName("KNOWS")).id
            tx.success()
        }
        return relationshipId
    }

    private fun assertRelationshipNotExist(db: GraphDatabaseService, id: Long) {
        try {
            db.getRelationshipById(id)
            org.junit.Assert.fail("Exception expected")
        } catch (e: Exception) {
            org.junit.Assert.assertThat(e, Matchers.instanceOf(NotFoundException::class.java))
        }
    }

    private fun healthOf(db: GraphDatabaseService): DatabaseHealth {
        val resolver = (db as GraphDatabaseAPI).dependencyResolver
        return resolver.resolveDependency(DatabaseHealth::class.java)
    }

    private fun createLongString(): String? {
        val strings = arrayOfNulls<String>(ByteUnit.kibiBytes(2).toInt())
        Arrays.fill(strings, "a")
        return Arrays.toString(strings)
    }

    @Throws(IOException::class)
    private fun copyTransactionLogs(): File {
        val restoreDbStore = directory.storeDir("restore-db")
        val restoreDbStoreDir = directory.databaseDir(restoreDbStore)
        move(fileSystemRule.get(), directory.databaseDir(), restoreDbStoreDir)
        return restoreDbStoreDir
    }

    @Throws(IOException::class)
    private fun move(fs: FileSystemAbstraction, fromDirectory: File, toDirectory: File) {
        org.junit.Assert.assertTrue(fs.isDirectory(fromDirectory))
        org.junit.Assert.assertTrue(fs.isDirectory(toDirectory))
        val transactionLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder(fromDirectory, fs).build()
        val logFiles = transactionLogFiles.logFiles()
        for (logFile in logFiles) {
            FileOperation.MOVE.perform(fs, logFile.name, fromDirectory, false, toDirectory,
                    ExistingTargetStrategy.FAIL)
        }
    }

    private fun startDatabase(storeDir: File, fs: EphemeralFileSystemAbstraction, indexProvider: UpdateCapturingIndexProvider): GraphDatabaseAPI {
        return TestGraphDatabaseFactory()
                .setFileSystem(fs)
                .addKernelExtension(IndexExtensionFactory(indexProvider))
                .newImpermanentDatabaseBuilder(storeDir)
                .setConfig(GraphDatabaseSettings.default_schema_provider, indexProvider.providerDescriptor.name())
                .setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
                .setConfig("kafka.bootstrap.servers", KafkaEventSinkSuiteIT.kafka.bootstrapServers)
                .setConfig("kafka.zookeeper.connect", KafkaEventSinkSuiteIT.kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"])
                .setConfig("streams.sink.enabled", "true")
                .newDatabase() as GraphDatabaseAPI
    }

    private fun startDatabase(storeDir: File, clusterOnly: Boolean = false, status: StreamsPluginStatus = StreamsPluginStatus.RUNNING): GraphDatabaseService {
        return TestGraphDatabaseFactory()
                .setInternalLogProvider(logProvider)
                .newEmbeddedDatabaseBuilder(storeDir)
                .setConfig("streams.sink.topic.cypher.$topic", cypherQueryTemplate)
                .setConfig("kafka.bootstrap.servers", KafkaEventSinkSuiteIT.kafka.bootstrapServers)
                .setConfig("kafka.zookeeper.connect", KafkaEventSinkSuiteIT.kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"])
                .setConfig("streams.sink.enabled", "true")
                .setConfig("streams.cluster.only", clusterOnly.toString())
                .newDatabase(status)
    }

    class UpdateCapturingIndexProvider internal constructor(private val actual: IndexProvider, private val initialUpdates: Map<Long?, Collection<IndexEntryUpdate<*>>>) : IndexProvider(actual) {
        private val indexes: MutableMap<Long, UpdateCapturingIndexAccessor> = ConcurrentHashMap()
        override fun getPopulator(descriptor: StoreIndexDescriptor, samplingConfig: IndexSamplingConfig, bufferFactory: ByteBufferFactory): IndexPopulator {
            return actual.getPopulator(descriptor, samplingConfig, bufferFactory)
        }

        @Throws(IOException::class)
        override fun getOnlineAccessor(descriptor: StoreIndexDescriptor, samplingConfig: IndexSamplingConfig): IndexAccessor {
            val actualAccessor = actual.getOnlineAccessor(descriptor, samplingConfig)
            return indexes.computeIfAbsent(descriptor.id) { id: Long? -> UpdateCapturingIndexAccessor(actualAccessor, initialUpdates[id]) }
        }

        @Throws(IllegalStateException::class)
        override fun getPopulationFailure(descriptor: StoreIndexDescriptor): String {
            return actual.getPopulationFailure(descriptor)
        }

        override fun getInitialState(descriptor: StoreIndexDescriptor): InternalIndexState {
            return actual.getInitialState(descriptor)
        }

        override fun getCapability(descriptor: StoreIndexDescriptor): IndexCapability {
            return actual.getCapability(descriptor)
        }

        override fun storeMigrationParticipant(fs: FileSystemAbstraction, pageCache: PageCache): StoreMigrationParticipant {
            return actual.storeMigrationParticipant(fs, pageCache)
        }

        fun snapshot(): Map<Long?, Collection<IndexEntryUpdate<*>>> {
            val result: MutableMap<Long?, Collection<IndexEntryUpdate<*>>> = HashMap()
            indexes.forEach { (indexId: Long?, index: UpdateCapturingIndexAccessor) -> result[indexId] = index.snapshot() }
            return result
        }
    }

    class UpdateCapturingIndexAccessor internal constructor(private val actual: IndexAccessor, initialUpdates: Collection<IndexEntryUpdate<*>>?) : IndexAccessor {
        private val updates: MutableCollection<IndexEntryUpdate<*>> = ArrayList()
        override fun drop() {
            actual.drop()
        }

        override fun newUpdater(mode: IndexUpdateMode): IndexUpdater {
            return wrap(actual.newUpdater(mode))
        }

        private fun wrap(actual: IndexUpdater): IndexUpdater {
            return UpdateCapturingIndexUpdater(actual, updates)
        }

        override fun force(ioLimiter: IOLimiter) {
            actual.force(ioLimiter)
        }

        override fun refresh() {
            actual.refresh()
        }

        override fun close() {
            actual.close()
        }

        override fun newReader(): IndexReader {
            return actual.newReader()
        }

        override fun newAllEntriesReader(): BoundedIterable<Long> {
            return actual.newAllEntriesReader()
        }

        override fun snapshotFiles(): ResourceIterator<File> {
            return actual.snapshotFiles()
        }

        @Throws(IndexEntryConflictException::class)
        override fun verifyDeferredConstraints(propertyAccessor: NodePropertyAccessor) {
            actual.verifyDeferredConstraints(propertyAccessor)
        }

        override fun isDirty(): Boolean {
            return actual.isDirty
        }

        fun snapshot(): Collection<IndexEntryUpdate<*>> {
            return ArrayList(updates)
        }

        override fun consistencyCheck(reporterFactory: ReporterFactory): Boolean {
            return actual.consistencyCheck(reporterFactory)
        }

        init {
            if (initialUpdates != null) {
                updates.addAll(initialUpdates)
            }
        }
    }

    class UpdateCapturingIndexUpdater internal constructor(private val actual: IndexUpdater, private val updatesTarget: MutableCollection<IndexEntryUpdate<*>>) : IndexUpdater {
        @Throws(IndexEntryConflictException::class)
        override fun process(update: IndexEntryUpdate<*>) {
            actual.process(update)
            updatesTarget.add(update)
        }

        @Throws(IndexEntryConflictException::class)
        override fun close() {
            actual.close()
        }
    }

    private class IndexExtensionFactory internal constructor(private val indexProvider: IndexProvider) : KernelExtensionFactory<IndexExtensionFactory.Dependencies?>("customExtension") {
        internal interface Dependencies

        override fun newInstance(context: KernelContext?, p1: Dependencies?): Lifecycle? {
            return indexProvider
        }

    }
}