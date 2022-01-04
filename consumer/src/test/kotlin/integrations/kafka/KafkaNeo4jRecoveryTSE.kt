package integrations.kafka


import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric
import org.apache.commons.lang3.exception.ExceptionUtils.getRootCause
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.neo4j.configuration.Config
import org.neo4j.configuration.Config.defaults
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files
import org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold
import org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs
import org.neo4j.dbms.DatabaseStateService
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.DatabaseStartAbortedException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.helpers.collection.Iterables.count
import org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained
import org.neo4j.internal.kernel.api.PropertyIndexQuery.fulltextSearch
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.database.DatabaseTracers.EMPTY
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP
import org.neo4j.kernel.impl.store.MetaDataStore.getRecord
import org.neo4j.kernel.impl.storemigration.LegacyTransactionLogsLocator
import org.neo4j.kernel.impl.transaction.log.files.LogFiles
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.DEFAULT_NAME
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.kernel.recovery.Recovery
import org.neo4j.kernel.recovery.Recovery.performRecovery
import org.neo4j.kernel.recovery.RecoveryHelpers
import org.neo4j.kernel.recovery.RecoveryMonitor
import org.neo4j.lock.LockTracer
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.memory.EmptyMemoryTracker.INSTANCE
import org.neo4j.monitoring.Monitors
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.test.extension.Inject
import org.neo4j.test.extension.Neo4jLayoutExtension
import org.neo4j.test.extension.pagecache.PageCacheExtension
import streams.utils.JSONUtils
import java.lang.String.valueOf
import java.nio.file.DirectoryStream
import java.nio.file.Path
import java.util.*
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.atomic.AtomicBoolean


@PageCacheExtension
@Neo4jLayoutExtension
class KafkaNeo4jRecoveryTSE: KafkaEventSinkBaseTSE() {

    var topic: String? = null

    @BeforeEach
    fun setTopic() {
        topic = UUID.randomUUID().toString()
    }

    private val TEN_KB = ByteUnit.kibiBytes(10).toInt()

    @Inject
    private val fileSystem: DefaultFileSystemAbstraction? = null

    @Inject
    private val pageCache: PageCache? = null

    @Inject
    private val neo4jLayout: Neo4jLayout? = null

    @Inject
    private val databaseLayout: DatabaseLayout? = null
    private var builder: TestDatabaseManagementServiceBuilder? = null
    private var managementService: DatabaseManagementService? = null

    fun enableRelationshipTypeScanStore(): Boolean {
        return false
    }

    @Test
    @Throws(Throwable::class)
    fun recoveryRequiredOnDatabaseWithoutCorrectCheckpoints() {
        val database: GraphDatabaseService = createDatabase()
        generateSomeData(database)
        managementService!!.shutdown()
        sendKafkaEvents()
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        assertTrue(isRecoveryRequired(databaseLayout))
    }

    @Test
    fun recoveryNotRequiredWhenDatabaseNotFound() {
        val absentDatabase = neo4jLayout!!.databaseLayout("absent")
        assertFalse(isRecoveryRequired(absentDatabase))
    }

    @Test
    fun recoverEmptyDatabase() {
        val config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.skip_default_indexes_on_creation, true)
                .set(preallocate_logical_logs, false)
                .build()

        managementService = TestDatabaseManagementServiceBuilder(neo4jLayout)
                .setConfig(config)
                .build()
        managementService!!.database(databaseLayout!!.databaseName) as GraphDatabaseAPI
        managementService!!.shutdown()
        sendKafkaEvents()
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        assertFalse(isRecoveryRequired(databaseLayout, defaults()))
    }

    @Test
    fun recoverDatabaseWithNodes() {
        val database: GraphDatabaseService = createDatabase()
        val numberOfNodes = 10
        for (i in 0 until numberOfNodes) {
            createSingleNode(database)
        }
        managementService!!.shutdown()
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        recoverDatabase()
        val recoveredDatabase: GraphDatabaseService = createDatabase()
        try {
            recoveredDatabase.beginTx().use { tx -> assertEquals(numberOfNodes.toLong(), count(tx.allNodes)) }
        } finally {
            managementService!!.shutdown()
        }
    }

    @Test
    fun tracePageCacheAccessOnDatabaseRecovery() {
        val database: GraphDatabaseService = createDatabase()
        val numberOfNodes = 10
        for (i in 0 until numberOfNodes) {
            createSingleNode(database)
        }
        managementService!!.shutdown()
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        val pageCacheTracer = DefaultPageCacheTracer()
        val tracers = DatabaseTracers(DatabaseTracer.NULL, LockTracer.NONE, pageCacheTracer)
        recoverDatabase(tracers)
        assertThat(pageCacheTracer.pins()).isEqualTo(pageCacheTracer.unpins())
        assertThat(pageCacheTracer.hits() + pageCacheTracer.faults()).isEqualTo(pageCacheTracer.pins())
        val recoveredDatabase: GraphDatabaseService = createDatabase()
        try {
            recoveredDatabase.beginTx().use { tx -> assertEquals(numberOfNodes.toLong(), count(tx.allNodes)) }
        } finally {
            managementService!!.shutdown()
        }
    }

    @Test
    fun recoverDatabaseWithNodesAndRelationshipsAndRelationshipTypes() {
        val database: GraphDatabaseService = createDatabase()
        val numberOfRelationships = 10
        val numberOfNodes = numberOfRelationships * 2
        for (i in 0 until numberOfRelationships) {
            database.beginTx().use { transaction ->
                val start = transaction.createNode()
                val stop = transaction.createNode()
                start.createRelationshipTo(stop, withName(valueOf(i)))
                transaction.commit()
            }
        }
        managementService!!.shutdown()
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        recoverDatabase()
        val recoveredDatabase: GraphDatabaseService = createDatabase()
        try {
            recoveredDatabase.beginTx().use { transaction ->
                assertEquals(numberOfNodes.toLong(), count(transaction.allNodes))
                assertEquals(numberOfRelationships.toLong(), count(transaction.allRelationships))
                assertEquals(numberOfRelationships.toLong(), count(transaction.allRelationshipTypesInUse))
            }
        } finally {
            managementService!!.shutdown()
        }
    }

    @Test
    fun recoverDatabaseWithProperties() {
        val database: GraphDatabaseService = createDatabase()
        val numberOfRelationships = 10
        val numberOfNodes = numberOfRelationships * 2
        for (i in 0 until numberOfRelationships) {
            database.beginTx().use { transaction ->
                val start = transaction.createNode()
                val stop = transaction.createNode()
                start.setProperty("start$i", i)
                stop.setProperty("stop$i", i)
                start.createRelationshipTo(stop, withName(valueOf(i)))
                transaction.commit()
            }
        }
        managementService!!.shutdown()
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        recoverDatabase()
        val recoveredDatabase: GraphDatabaseService = createDatabase()
        try {
            recoveredDatabase.beginTx().use { transaction ->
                assertEquals(numberOfNodes.toLong(), count(transaction.allNodes))
                assertEquals(numberOfRelationships.toLong(), count(transaction.allRelationships))
                assertEquals(numberOfRelationships.toLong(), count(transaction.allRelationshipTypesInUse))
                assertEquals(numberOfNodes.toLong(), count(transaction.allPropertyKeys))
            }
        } finally {
            managementService!!.shutdown()
        }
    }

    @Test
    fun recoverDatabaseWithIndex() {
        val database: GraphDatabaseService = createDatabase()
        val numberOfRelationships = 10
        val numberOfNodes = numberOfRelationships * 2
        val startProperty = "start"
        val stopProperty = "stop"
        val startMarker = Label.label("start")
        val stopMarker = Label.label("stop")
        database.beginTx().use { transaction ->
            transaction.schema().indexFor(startMarker).on(startProperty).create()
            transaction.schema().constraintFor(stopMarker).assertPropertyIsUnique(stopProperty).create()
            transaction.commit()
        }
        awaitIndexesOnline(database)
        for (i in 0 until numberOfRelationships) {
            database.beginTx().use { transaction ->
                val start = transaction.createNode(startMarker)
                val stop = transaction.createNode(stopMarker)
                start.setProperty(startProperty, i)
                stop.setProperty(stopProperty, i)
                start.createRelationshipTo(stop, withName(valueOf(i)))
                transaction.commit()
            }
        }
        var numberOfPropertyKeys: Long
        database.beginTx().use { transaction -> numberOfPropertyKeys = count(transaction.allPropertyKeys) }
        managementService!!.shutdown()
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        recoverDatabase()
        val recoveredDatabase: GraphDatabaseService = createDatabase()
        try {
            recoveredDatabase.beginTx().use { transaction ->
                assertEquals(numberOfNodes.toLong(), count(transaction.allNodes))
                assertEquals(numberOfRelationships.toLong(), count(transaction.allRelationships))
                assertEquals(numberOfRelationships.toLong(), count(transaction.allRelationshipTypesInUse))
                assertEquals(numberOfPropertyKeys.toLong(), count(transaction.allPropertyKeys))
            }
        } finally {
            managementService!!.shutdown()
        }
    }

    @Test
    fun recoverDatabaseWithRelationshipIndex() {
        val database: GraphDatabaseService = createDatabase()
        val numberOfRelationships = 10
        val type = withName("TYPE")
        val property = "prop"
        val indexName = "my index"
        database.beginTx().use { transaction ->
            transaction.schema().indexFor(type).on(property).withIndexType(IndexType.FULLTEXT)
                .withName(indexName).create()
            transaction.commit()
        }
        awaitIndexesOnline(database)
        database.beginTx().use { transaction ->
            val start = transaction.createNode()
            val stop = transaction.createNode()
            for (i in 0 until numberOfRelationships) {
                val relationship = start.createRelationshipTo(stop, type)
                relationship.setProperty(property, "value")
            }
            transaction.commit()
        }
        managementService!!.shutdown()
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        recoverDatabase()
        val recoveredDatabase = createDatabase()
        awaitIndexesOnline(recoveredDatabase)
        try {
            recoveredDatabase.beginTx().use { transaction ->
                val ktx = (transaction as InternalTransaction).kernelTransaction()
                val index = ktx.schemaRead().indexGetForName(indexName)
                val indexReadSession = ktx.dataRead().indexReadSession(index)
                var relationshipsInIndex = 0
                ktx.cursors().allocateRelationshipValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker()).use { cursor ->
                    ktx.dataRead().relationshipIndexSeek(indexReadSession, cursor, unconstrained(), fulltextSearch("*"))
                    while (cursor.next()) {
                        relationshipsInIndex++
                    }
                }
                assertEquals(numberOfRelationships, relationshipsInIndex)
            }
        } finally {
            managementService!!.shutdown()
        }
    }

    @Test
    fun recoverDatabaseWithFirstTransactionLogFileWithoutShutdownCheckpoint() {
        val database: GraphDatabaseService = createDatabase()
        generateSomeData(database)
        managementService!!.shutdown()
        assertEquals(1, countCheckPointsInTransactionLogs())
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        assertEquals(0, countCheckPointsInTransactionLogs())
        assertTrue(isRecoveryRequired(databaseLayout))
        startStopDatabase()
        assertFalse(isRecoveryRequired(databaseLayout))
        // we will have 2 checkpoints: first will be created after successful recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs())
    }

    @Test
    fun failToStartDatabaseWithRemovedTransactionLogs() {
        val database = createDatabase()
        generateSomeData(database)
        managementService!!.shutdown()
        removeTransactionLogs()
        val restartedDb = createDatabase()
        try {
            val dbStateService = restartedDb.dependencyResolver.resolveDependency(
                DatabaseStateService::class.java
            )
            val failure = dbStateService.causeOfFailure(restartedDb.databaseId())
            assertTrue(failure.isPresent)
            assertThat(getRootCause(failure.get()).message).contains("Transaction logs are missing and recovery is not possible.")
        } finally {
            managementService!!.shutdown()
        }
    }

    @Test
    fun failToStartDatabaseWithTransactionLogsInLegacyLocation() {
        val database = createDatabase()
        generateSomeData(database)
        managementService!!.shutdown()
        val logFiles = buildLogFiles()
        var txLogFiles: Array<Path> = fileSystem!!.listFiles(logFiles.logFilesDirectory(),
            DirectoryStream.Filter { path: Path ->
                path.fileName.toString().startsWith(DEFAULT_NAME)
            })
        txLogFiles += logFiles.checkpointFile.detachedCheckpointFiles
        val databasesDirectory: Path = databaseLayout!!.neo4jLayout.databasesDirectory()
        val legacyLayout = Neo4jLayout.ofFlat(databasesDirectory).databaseLayout(databaseLayout.databaseName)
        val logsLocator = LegacyTransactionLogsLocator(defaults(), legacyLayout)
        val transactionLogsDirectory: Path = logsLocator.transactionLogsDirectory
        assertNotNull(txLogFiles)
        assertTrue(txLogFiles.size > 0)
        for (logFile in txLogFiles) {
            fileSystem.moveToDirectory(logFile, transactionLogsDirectory)
        }
        val logProvider = AssertableLogProvider()
        builder!!.setInternalLogProvider(logProvider)
        val restartedDb = createDatabase()
        try {
            val dbStateService = restartedDb.dependencyResolver.resolveDependency(
                DatabaseStateService::class.java
            )
            val failure = dbStateService.causeOfFailure(restartedDb.databaseId())
            assertTrue(failure.isPresent)
            assertThat(failure.get()).hasRootCauseMessage("Transaction logs are missing and recovery is not possible.")
            assertThat(logProvider.serialize()).contains(txLogFiles[0].getFileName().toString())
        } finally {
            managementService!!.shutdown()
        }
    }

    @Test
    fun startDatabaseWithRemovedSingleTransactionLogFile() {
        val database = createDatabase()
        val pageCache = getDatabasePageCache(database)
        generateSomeData(database)
        assertEquals(
            -1,
            getRecord(
                pageCache,
                database.databaseLayout().metadataStore(),
                LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP,
                databaseLayout!!.databaseName,
                CursorContext.NULL
            )
        )
        managementService!!.shutdown()
        removeTransactionLogs()
        startStopDatabaseWithForcedRecovery()
        assertFalse(isRecoveryRequired(databaseLayout))
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs())
        verifyRecoveryTimestampPresent(database)
    }

    @Test
    fun startDatabaseWithRemovedMultipleTransactionLogFiles() {
        val database: GraphDatabaseService = createDatabase(ByteUnit.mebiBytes(1))
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database)
        }
        managementService!!.shutdown()
        removeTransactionLogs()
        startStopDatabaseWithForcedRecovery()
        assertFalse(isRecoveryRequired(databaseLayout))
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs())
    }

    @Test
    fun killAndStartDatabaseAfterTransactionLogsRemoval() {
        val database: GraphDatabaseService = createDatabase(ByteUnit.mebiBytes(1))
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database)
        }
        managementService!!.shutdown()
        removeTransactionLogs()
        assertTrue(isRecoveryRequired(databaseLayout))
        assertEquals(0, countTransactionLogFiles())
        val forcedRecoveryManagementService = forcedRecoveryManagement()
        val service = forcedRecoveryManagementService.database(DEFAULT_DATABASE_NAME)
        createSingleNode(service)
        forcedRecoveryManagementService.shutdown()
        assertEquals(2, countTransactionLogFiles())
        assertEquals(2, countCheckPointsInTransactionLogs())
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        startStopDatabase()
        assertFalse(isRecoveryRequired(databaseLayout))
        // we will have 3 checkpoints: one from logs before recovery, second will be created as part of recovery and another on shutdown
        assertEquals(3, countCheckPointsInTransactionLogs())
    }

    @Test
    fun killAndStartDatabaseAfterTransactionLogsRemovalWithSeveralFilesWithoutCheckpoint() {
        val database: GraphDatabaseService = createDatabase(ByteUnit.mebiBytes(1))
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database)
        }
        managementService!!.shutdown()
        removeFileWithCheckpoint()
        assertEquals(4, countTransactionLogFiles())
        assertEquals(0, countCheckPointsInTransactionLogs())
        assertTrue(isRecoveryRequired(databaseLayout))
        startStopDatabase()
        assertEquals(2, countCheckPointsInTransactionLogs())
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        startStopDatabase()
        assertFalse(isRecoveryRequired(databaseLayout))
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs())
    }

    @Test
    fun startDatabaseAfterTransactionLogsRemovalAndKillAfterRecovery() {
        val logThreshold = ByteUnit.mebiBytes(1)
        val database: GraphDatabaseService = createDatabase(logThreshold)
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database)
        }
        managementService!!.shutdown()
        removeFileWithCheckpoint()
        assertEquals(4, countTransactionLogFiles())
        assertEquals(0, countCheckPointsInTransactionLogs())
        assertTrue(isRecoveryRequired(databaseLayout))
        startStopDatabase()
        assertEquals(2, countCheckPointsInTransactionLogs())
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        startStopDatabase()
        assertFalse(isRecoveryRequired(databaseLayout))
        // we will have 2 checkpoints here because offset in both of them will be the same
        // and 2 will be truncated instead since truncation is based on position
        // next start-stop cycle will have transaction between so we will have 3 checkpoints as expected.
        assertEquals(2, countCheckPointsInTransactionLogs())
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        builder = null // Reset log rotation threshold setting to avoid immediate rotation on `createSingleNode()`.
        val service: GraphDatabaseService = createDatabase(logThreshold * 2) // Bigger log, to avoid rotation.
        createSingleNode(service)
        managementService!!.shutdown()
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        startStopDatabase()
        assertFalse(isRecoveryRequired(databaseLayout))
        assertEquals(3, countCheckPointsInTransactionLogs())
    }

    @Test
    fun recoverDatabaseWithoutOneIdFile() {
        val db = createDatabase()
        generateSomeData(db)
        val layout = db.databaseLayout()
        managementService!!.shutdown()
        fileSystem!!.deleteFileOrThrow(layout.idRelationshipStore())
        assertTrue(isRecoveryRequired(layout))
        performRecovery(fileSystem, pageCache, EMPTY, defaults(), layout, INSTANCE)
        assertFalse(isRecoveryRequired(layout))
        assertTrue(fileSystem.fileExists(layout.idRelationshipStore()))
    }

    @Test
    fun recoverDatabaseWithoutIdFiles() {
        val db = createDatabase()
        generateSomeData(db)
        val layout = db.databaseLayout()
        managementService!!.shutdown()
        for (idFile in layout.idFiles()) {
            fileSystem!!.deleteFileOrThrow(idFile)
        }
        assertTrue(isRecoveryRequired(layout))
        recoverDatabase()
        assertFalse(isRecoveryRequired(layout))
        for (idFile in layout.idFiles()) {
            assertTrue(fileSystem!!.fileExists(idFile))
        }
    }

    @Test
    fun cancelRecoveryInTheMiddle() {
        val db = createDatabase()
        generateSomeData(db)
        val layout = db.databaseLayout()
        managementService!!.shutdown()
        RecoveryHelpers.removeLastCheckpointRecordFromLastLogFile(databaseLayout, fileSystem)
        assertTrue(isRecoveryRequired(layout))
        val monitors = Monitors()
        val guardExtensionFactory = GlobalGuardConsumerTestExtensionFactory()
        val recoveryMonitor = object : RecoveryMonitor {
            private val reverseCompleted = AtomicBoolean()
            private val recoveryCompleted = AtomicBoolean()
            override fun reverseStoreRecoveryCompleted(lowestRecoveredTxId: Long) {
                guardExtensionFactory.providedGuardConsumer!!.globalGuard.stop()
                reverseCompleted.set(true)
            }

            override fun recoveryCompleted(numberOfRecoveredTransactions: Int, recoveryTimeInMilliseconds: Long) {
                recoveryCompleted.set(true)
            }

            fun isReverseCompleted(): Boolean {
                return reverseCompleted.get()
            }

            fun isRecoveryCompleted(): Boolean {
                return recoveryCompleted.get()
            }
        }
        monitors.addMonitorListener(recoveryMonitor)
        val service = createTestDatabaseBuilder(layout.neo4jLayout)
            .addExtension(guardExtensionFactory)
            .setMonitors(monitors).build()
        try {
            val database = service.database(layout.databaseName)
            assertTrue(recoveryMonitor.isReverseCompleted())
            assertFalse(recoveryMonitor.isRecoveryCompleted())
            assertFalse(guardExtensionFactory.providedGuardConsumer!!.globalGuard.isAvailable)
            assertFalse(database.isAvailable(0))
            val e: Exception = assertThrows(Exception::class.java, database::beginTx)
            assertThat(getRootCause(e)).isInstanceOf(DatabaseStartAbortedException::class.java)
        } finally {
            service.shutdown()
        }
    }

    private fun awaitIndexesOnline(database: GraphDatabaseService) {
        database.beginTx().use { transaction ->
            transaction.schema().awaitIndexesOnline(10, MINUTES)
            transaction.commit()
        }
    }

    private fun createSingleNode(service: GraphDatabaseService) {
        service.beginTx().use { transaction ->
            transaction.createNode()
            transaction.commit()
        }
    }

    private fun startStopDatabase() {
        val db: GraphDatabaseService = createDatabase()
        db.beginTx().close()
        managementService!!.shutdown()
    }

    private fun recoverDatabase() {
        recoverDatabase(EMPTY)
    }

    private fun recoverDatabase(databaseTracers: DatabaseTracers) {
        val config = Config.newBuilder().build()
        assertTrue(isRecoveryRequired(databaseLayout, config))
        performRecovery(fileSystem, pageCache, databaseTracers, config, databaseLayout, INSTANCE)
        assertFalse(isRecoveryRequired(databaseLayout, config))
    }

    private fun isRecoveryRequired(layout: DatabaseLayout?): Boolean {
        val config = Config.newBuilder().build()
        return isRecoveryRequired(layout, config)
    }

    private fun isRecoveryRequired(layout: DatabaseLayout?, config: Config): Boolean {
        return Recovery.isRecoveryRequired(fileSystem, layout, config, INSTANCE)
    }

    private fun countCheckPointsInTransactionLogs(): Int {
        val logFiles = buildLogFiles()
        val checkpoints = logFiles.checkpointFile.reachableCheckpoints()
        return checkpoints.size
    }

    private fun buildLogFiles(): LogFiles {
        return LogFilesBuilder
            .logFilesBasedOnlyBuilder(databaseLayout!!.transactionLogsDirectory, fileSystem)
            .withCommandReaderFactory(StorageEngineFactory.defaultStorageEngine().commandReaderFactory())
            .build()
    }

    private fun removeTransactionLogs() {
        val logFiles = buildLogFiles()
        for (logFile in fileSystem!!.listFiles(logFiles.logFilesDirectory())) {
            fileSystem.deleteFile(logFile)
        }
    }

    private fun removeFileWithCheckpoint() {
        val logFiles = buildLogFiles()
        fileSystem!!.deleteFileOrThrow(logFiles.checkpointFile.currentFile)
    }

    private fun countTransactionLogFiles(): Int {
        val logFiles = buildLogFiles()
        return logFiles.logFiles().size
    }

    private fun generateSomeData(database: GraphDatabaseService) {
        for (i in 0..9) {
            database.beginTx().use { transaction ->
                val node1 = transaction.createNode()
                val node2 = transaction.createNode()
                node1.createRelationshipTo(node2, withName("Type$i"))
                node2.setProperty("a", randomAlphanumeric(TEN_KB))
                transaction.commit()
            }
        }
    }

    private fun createDatabase(): GraphDatabaseAPI {
        return createDatabase(logical_log_rotation_threshold.defaultValue())
    }

    private fun createDatabase(logThreshold: Long): GraphDatabaseAPI {
        createBuilder(logThreshold)
        managementService = builder!!.build()
        return managementService!!.database(databaseLayout!!.databaseName) as GraphDatabaseAPI
    }

    private fun createBuilder(logThreshold: Long) {
        if (builder == null) {
            builder = createTestDatabaseBuilder()
                .setConfig(preallocate_logical_logs, false)
                .setConfig(logical_log_rotation_threshold, logThreshold)
        }
    }

    private fun startStopDatabaseWithForcedRecovery() {
        val forcedRecoveryManagementService = forcedRecoveryManagement()
        forcedRecoveryManagementService.shutdown()
    }

    private fun forcedRecoveryManagement(): DatabaseManagementService {
        return createTestDatabaseBuilder()
            .setConfig(fail_on_missing_files, false)
            .build()
    }

    private fun createTestDatabaseBuilder(): TestDatabaseManagementServiceBuilder {
        return createTestDatabaseBuilder(neo4jLayout)
    }

    private fun createTestDatabaseBuilder(neo4jLayout: Neo4jLayout?): TestDatabaseManagementServiceBuilder {
        return TestDatabaseManagementServiceBuilder(neo4jLayout)
    }

    private fun getDatabasePageCache(databaseAPI: GraphDatabaseAPI): PageCache {
        return databaseAPI.dependencyResolver.resolveDependency(PageCache::class.java)
    }

    private fun verifyRecoveryTimestampPresent(databaseAPI: GraphDatabaseAPI) {
        val restartedDatabase = createDatabase()
        try {
            val restartedCache = getDatabasePageCache(restartedDatabase)
            val record = getRecord(
                restartedCache,
                databaseAPI.databaseLayout().metadataStore(),
                LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP,
                databaseLayout!!.databaseName,
                CursorContext.NULL
            )
            assertThat(record).isGreaterThan(0L)
        } finally {
            managementService!!.shutdown()
        }
    }

    internal interface Dependencies {
        fun globalGuard(): CompositeDatabaseAvailabilityGuard
    }

    private class GlobalGuardConsumerTestExtensionFactory internal constructor() :
        ExtensionFactory<Dependencies>("globalGuardConsumer") {
        var providedGuardConsumer: GlobalGuardConsumer? = null
            private set

        override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
            providedGuardConsumer = GlobalGuardConsumer(dependencies)
            return providedGuardConsumer!!
        }
    }

    private class GlobalGuardConsumer internal constructor(dependencies: Dependencies) : LifecycleAdapter() {
        val globalGuard: CompositeDatabaseAvailabilityGuard

        init {
            globalGuard = dependencies.globalGuard()
        }
    }

    private fun sendKafkaEvents() = runBlocking {
        (1..10).forEach {
            val dataProperties = mapOf("prop1" to "foo $it", "bar" to it)
            val data = mapOf("id" to it, "properties" to dataProperties)
            val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
            val metadata = kafkaProducer.send(producerRecord).get()
            println("Sent record $it to topic ${metadata.topic()}")
        }
        delay(5000)
    }

}