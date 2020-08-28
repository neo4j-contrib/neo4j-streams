package integrations.kafka

import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.hamcrest.MatcherAssert
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.DatabaseStateService
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.DatabaseStartAbortedException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.collection.Iterables.count
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.pagecache.PageCache
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP
import org.neo4j.kernel.impl.store.MetaDataStore.getRecord
import org.neo4j.kernel.impl.transaction.log.LogPosition
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader
import org.neo4j.kernel.impl.transaction.log.files.LogFiles
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.kernel.recovery.Recovery
import org.neo4j.kernel.recovery.RecoveryMonitor
import org.neo4j.monitoring.Monitors
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.test.extension.Inject
import org.neo4j.test.extension.Neo4jLayoutExtension
import org.neo4j.test.extension.pagecache.PageCacheExtension
import streams.StreamsEventSinkExtensionFactory
import streams.config.StreamsConfig
import streams.config.StreamsConfigExtensionFactory
import streams.events.StreamsPluginStatus
import streams.extensions.execute
import streams.procedures.StreamsSinkProcedures
import java.util.concurrent.TimeUnit
import kotlin.streams.toList

@PageCacheExtension
@Neo4jLayoutExtension
class KafkaNeo4jRecoveryTSE: KafkaEventSinkBaseTSE() {
    @Inject
    private var fileSystem: DefaultFileSystemAbstraction? = null

    @Inject
    private var pageCache: PageCache? = null

    @Inject
    private var neo4jLayout: Neo4jLayout? = null

    @Inject
    private val databaseLayout: DatabaseLayout? = null

    private var builder: TestDatabaseManagementServiceBuilder? = null
    private var managementService: DatabaseManagementService? = null

    @BeforeEach
    fun beforeEach() {
        StreamsConfig.registerListener {
            it["kafka.bootstrap.servers"] = KafkaEventSinkSuiteIT.kafka.bootstrapServers
            it["streams.sink.enabled"] = "true"
        }
    }

    @Test
    fun recoveryRequiredOnDatabaseWithoutCorrectCheckpoints() {
        val database: GraphDatabaseService = createDatabase()
        generateSomeData(database)
        managementService!!.shutdown()
        removeLastCheckpointRecordFromLastLogFile()
        assertTrue(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
    }

    @Test
    fun recoveryNotRequiredWhenDatabaseNotFound() {
        val absentDatabase = neo4jLayout!!.databaseLayout("absent")
        assertFalse(Recovery.isRecoveryRequired(fileSystem, absentDatabase, Config.defaults()))
    }

    @Test
    fun recoverEmptyDatabase() {
        createDatabase()
        managementService!!.shutdown()
        removeLastCheckpointRecordFromLastLogFile()
        assertFalse(Recovery.isRecoveryRequired(databaseLayout, Config.defaults()))
    }

    @Test
    fun recoverDatabaseWithNodes() {
        val database: GraphDatabaseService = createDatabase()
        val numberOfNodes = 10
        for (i in 0 until numberOfNodes) {
            createSingleNode(database)
        }
        managementService!!.shutdown()
        removeLastCheckpointRecordFromLastLogFile()
        recoverDatabase()
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
                start.createRelationshipTo(stop, RelationshipType.withName(i.toString()))
                transaction.commit()
            }
        }
        managementService!!.shutdown()
        removeLastCheckpointRecordFromLastLogFile()
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
                start.createRelationshipTo(stop, RelationshipType.withName(i.toString()))
                transaction.commit()
            }
        }
        managementService!!.shutdown()
        removeLastCheckpointRecordFromLastLogFile()
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
        database.beginTx().use { transaction ->
            transaction.schema().awaitIndexesOnline(1, TimeUnit.MINUTES)
            transaction.commit()
        }
        for (i in 0 until numberOfRelationships) {
            database.beginTx().use { transaction ->
                val start = transaction.createNode(startMarker)
                val stop = transaction.createNode(stopMarker)
                start.setProperty(startProperty, i)
                stop.setProperty(stopProperty, i)
                start.createRelationshipTo(stop, RelationshipType.withName(i.toString()))
                transaction.commit()
            }
        }
        var numberOfPropertyKeys: Long = database.beginTx()
                .use { transaction -> count(transaction.allPropertyKeys) }
        managementService!!.shutdown()
        removeLastCheckpointRecordFromLastLogFile()
        recoverDatabase()
        val recoveredDatabase: GraphDatabaseService = createDatabase()
        try {
            recoveredDatabase.beginTx().use { transaction ->
                assertEquals(numberOfNodes.toLong(), count(transaction.allNodes))
                assertEquals(numberOfRelationships.toLong(), count(transaction.allRelationships))
                assertEquals(numberOfRelationships.toLong(), count(transaction.allRelationshipTypesInUse))
                assertEquals(numberOfPropertyKeys, count(transaction.allPropertyKeys))
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
        removeLastCheckpointRecordFromLastLogFile()
        assertEquals(0, countCheckPointsInTransactionLogs())
        assertTrue(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
        startStopDatabase()
        assertFalse(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
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
            val dbStateService = restartedDb.dependencyResolver.resolveDependency(DatabaseStateService::class.java)
            val failure = dbStateService.causeOfFailure(restartedDb.databaseId())
            assertTrue(failure.isPresent)
            MatcherAssert.assertThat(ExceptionUtils.getRootCause(failure.get()).message, Matchers.containsString("Transaction logs are missing and recovery is not possible."))
        } finally {
            managementService!!.shutdown()
        }
    }

    @Test
    fun startDatabaseWithRemovedSingleTransactionLogFile() {
        val database = createDatabase()
        val pageCache = getDatabasePageCache(database)
        generateSomeData(database)
        assertEquals(-1, getRecord(pageCache, database.databaseLayout().metadataStore(), LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP))
        managementService!!.shutdown()
        removeTransactionLogs()
        startStopDatabaseWithForcedRecovery()
        assertFalse(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs())
        verifyRecoveryTimestampPresent(database)
    }

    @Test
    fun startDatabaseWithRemovedMultipleTransactionLogFiles() {
        val managementService = TestDatabaseManagementServiceBuilder(neo4jLayout)
                .addExtension(StreamsConfigExtensionFactory())
                .addExtension(StreamsEventSinkExtensionFactory())
                .setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.mebiBytes(1))
                .build()
        val database = managementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database)
        }
        managementService.shutdown()
        removeTransactionLogs()
        startStopDatabaseWithForcedRecovery()
        assertFalse(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs())
    }

    @Test
    fun killAndStartDatabaseAfterTransactionLogsRemoval() {
        val managementService = TestDatabaseManagementServiceBuilder(neo4jLayout)
                .addExtension(StreamsConfigExtensionFactory())
                .addExtension(StreamsEventSinkExtensionFactory())
                .setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.mebiBytes(1))
                .build()
        val database = managementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database)
        }
        managementService.shutdown()
        removeTransactionLogs()
        assertTrue(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
        assertEquals(0, countTransactionLogFiles())
        val forcedRecoveryManagementService = forcedRecoveryManagement()
        val service = forcedRecoveryManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
        createSingleNode(service)
        forcedRecoveryManagementService.shutdown()
        assertEquals(1, countTransactionLogFiles())
        assertEquals(2, countCheckPointsInTransactionLogs())
        removeLastCheckpointRecordFromLastLogFile()
        startStopDatabase()
        assertFalse(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
        // we will have 3 checkpoints: one from logs before recovery, second will be created as part of recovery and another on shutdown
        assertEquals(3, countCheckPointsInTransactionLogs())
    }

    @Test
    fun killAndStartDatabaseAfterTransactionLogsRemovalWithSeveralFilesWithoutCheckpoint() {
        val managementService = TestDatabaseManagementServiceBuilder(neo4jLayout)
                .addExtension(StreamsConfigExtensionFactory())
                .addExtension(StreamsEventSinkExtensionFactory())
                .setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.mebiBytes(1))
                .build()
        val database = managementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database)
        }
        managementService.shutdown()
        removeHighestLogFile()
        assertEquals(4, countTransactionLogFiles())
        assertEquals(0, countCheckPointsInTransactionLogs())
        assertTrue(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
        startStopDatabase()
        assertEquals(2, countCheckPointsInTransactionLogs())
        removeLastCheckpointRecordFromLastLogFile()
        removeLastCheckpointRecordFromLastLogFile()
        startStopDatabase()
        assertFalse(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
        // we will have 2 checkpoints: first will be created as part of recovery and another on shutdown
        assertEquals(2, countCheckPointsInTransactionLogs())
    }

    @Test
    fun startDatabaseAfterTransactionLogsRemovalAndKillAfterRecovery() {
        val managementService = TestDatabaseManagementServiceBuilder(neo4jLayout)
                .addExtension(StreamsConfigExtensionFactory())
                .addExtension(StreamsEventSinkExtensionFactory())
                .setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.mebiBytes(1))
                .build()
        val database = managementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
        while (countTransactionLogFiles() < 5) {
            generateSomeData(database)
        }
        managementService.shutdown()
        removeHighestLogFile()
        assertEquals(4, countTransactionLogFiles())
        assertEquals(0, countCheckPointsInTransactionLogs())
        assertTrue(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
        startStopDatabase()
        assertEquals(2, countCheckPointsInTransactionLogs())
        removeLastCheckpointRecordFromLastLogFile()
        startStopDatabase()
        assertFalse(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
        // we will have 2 checkpoints here because offset in both of them will be the same and 2 will be truncated instead since truncation is based on position
        // next start-stop cycle will have transaction between so we will have 3 checkpoints as expected.
        assertEquals(2, countCheckPointsInTransactionLogs())
        removeLastCheckpointRecordFromLastLogFile()
        val service: GraphDatabaseService = createDatabase()
        createSingleNode(service)
        this.managementService!!.shutdown()
        removeLastCheckpointRecordFromLastLogFile()
        startStopDatabase()
        assertFalse(Recovery.isRecoveryRequired(fileSystem, databaseLayout, Config.defaults()))
        assertEquals(3, countCheckPointsInTransactionLogs())
    }

    @Test
    fun recoverDatabaseWithoutOneIdFile() {
        val db = createDatabase()
        generateSomeData(db)
        val layout = db.databaseLayout()
        managementService!!.shutdown()
        fileSystem!!.deleteFileOrThrow(layout.idRelationshipStore())
        assertTrue(Recovery.isRecoveryRequired(fileSystem, layout, Config.defaults()))
        Recovery.performRecovery(fileSystem, pageCache, Config.defaults(), layout)
        assertFalse(Recovery.isRecoveryRequired(fileSystem, layout, Config.defaults()))
        assertTrue(fileSystem!!.fileExists(layout.idRelationshipStore()))
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
        assertTrue(Recovery.isRecoveryRequired(fileSystem, layout, Config.defaults()))
        Recovery.performRecovery(fileSystem, pageCache, Config.defaults(), layout)
        assertFalse(Recovery.isRecoveryRequired(fileSystem, layout, Config.defaults()))
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
        removeLastCheckpointRecordFromLastLogFile()
        assertTrue(Recovery.isRecoveryRequired(fileSystem, layout, Config.defaults()))
        val monitors = Monitors()
        val recoveryMonitor: RecoveryMonitor = object : RecoveryMonitor {
            override fun reverseStoreRecoveryCompleted(lowestRecoveredTxId: Long) {
                GlobalGuardConsumer.globalGuard.stop()
            }
        }
        monitors.addMonitorListener(recoveryMonitor)
        val service = TestDatabaseManagementServiceBuilder(layout.neo4jLayout)
                .addExtension(GlobalGuardConsumerTestExtensionFactory())
                .addExtension(StreamsConfigExtensionFactory())
                .addExtension(StreamsEventSinkExtensionFactory())
                .setMonitors(monitors).build()
        try {
            val e = assertThrows(Exception::class.java) { service.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME).beginTx() }
            assertThat(ExceptionUtils.getRootCause(e), Matchers.instanceOf(DatabaseStartAbortedException::class.java))
        } finally {
            service.shutdown()
        }
    }

    private fun createSingleNode(service: GraphDatabaseService) {
        service.beginTx().use { transaction ->
            transaction.createNode()
            transaction.commit()
        }
    }

    private fun startStopDatabase() {
        createDatabase()
        managementService!!.shutdown()
    }

    private fun recoverDatabase() {
        assertTrue(Recovery.isRecoveryRequired(databaseLayout, Config.defaults()))
        Recovery.performRecovery(databaseLayout)
        assertFalse(Recovery.isRecoveryRequired(databaseLayout, Config.defaults()))
    }

    private fun countCheckPointsInTransactionLogs(): Int {
        var checkpointCounter = 0
        val logFiles = buildLogFiles()
        val transactionLogFile = logFiles.logFile
        val entryReader = VersionAwareLogEntryReader()
        val startPosition = logFiles.extractHeader(logFiles.highestLogVersion).startPosition
        transactionLogFile.getReader(startPosition).use { reader ->
            var logEntry: LogEntry?
            do {
                logEntry = entryReader.readLogEntry(reader)
                if (logEntry is CheckPoint) {
                    checkpointCounter++
                }
            } while (logEntry != null)
        }
        return checkpointCounter
    }

    private fun buildLogFiles(): LogFiles {
        return LogFilesBuilder.logFilesBasedOnlyBuilder(databaseLayout!!.transactionLogsDirectory, fileSystem).build()
    }

    private fun removeTransactionLogs() {
        val logFiles = buildLogFiles()
        val txLogFiles = logFiles.logFiles()
        for (logFile in txLogFiles) {
            fileSystem!!.deleteFile(logFile)
        }
    }

    private fun removeHighestLogFile() {
        val logFiles = buildLogFiles()
        val highestLogVersion = logFiles.highestLogVersion
        removeFileByVersion(logFiles, highestLogVersion)
    }

    private fun removeFileByVersion(logFiles: LogFiles, version: Long) {
        val versionFile = logFiles.getLogFileForVersion(version)
        assertNotNull(versionFile)
        fileSystem!!.deleteFile(versionFile)
    }

    private fun countTransactionLogFiles(): Int {
        val logFiles = buildLogFiles()
        return logFiles.logFiles().size
    }

    private fun removeLastCheckpointRecordFromLastLogFile() {
        var checkpointPosition: LogPosition? = null
        val logFiles = buildLogFiles()
        val transactionLogFile = logFiles.logFile
        val entryReader = VersionAwareLogEntryReader()
        val startPosition = logFiles.extractHeader(logFiles.highestLogVersion).startPosition
        transactionLogFile.getReader(startPosition).use { reader ->
            var logEntry: LogEntry?
            do {
                logEntry = entryReader.readLogEntry(reader)
                if (logEntry is CheckPoint) {
                    checkpointPosition = logEntry.logPosition
                }
            } while (logEntry != null)
        }
        if (checkpointPosition != null) {
            fileSystem!!.write(logFiles.highestLogFile).use { storeChannel -> storeChannel.truncate(checkpointPosition!!.byteOffset) }
        }
    }

    private fun createDatabase(): GraphDatabaseAPI {
        createBuilder()
        managementService = builder!!.build()
        val db = managementService?.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME) as GraphDatabaseAPI
        // we assert that streams is up and running
        db.dependencyResolver.resolveDependency(GlobalProcedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)
        val expectedRunning = listOf(mapOf("name" to "status", "value" to StreamsPluginStatus.RUNNING.toString()))
        val actual = this.db.execute("CALL streams.sink.status()") {
            it.stream().toList()
        }
        assertEquals(expectedRunning, actual)
        return db
    }

    private fun createBuilder() {
        if (builder == null) {
            builder = TestDatabaseManagementServiceBuilder(neo4jLayout)
                    .addExtension(StreamsConfigExtensionFactory())
                    .addExtension(StreamsEventSinkExtensionFactory())
                    .setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, GraphDatabaseSettings.logical_log_rotation_threshold.defaultValue())
        }
    }

    private fun startStopDatabaseWithForcedRecovery() {
        val forcedRecoveryManagementService = forcedRecoveryManagement()
        forcedRecoveryManagementService.shutdown()
    }

    private fun forcedRecoveryManagement(): DatabaseManagementService {
        return TestDatabaseManagementServiceBuilder(neo4jLayout)
                .addExtension(StreamsConfigExtensionFactory())
                .addExtension(StreamsEventSinkExtensionFactory())
                .setConfig(GraphDatabaseSettings.fail_on_missing_files, false).build()
    }

    private fun getDatabasePageCache(databaseAPI: GraphDatabaseAPI): PageCache {
        return databaseAPI.dependencyResolver.resolveDependency(PageCache::class.java)
    }

    private fun verifyRecoveryTimestampPresent(databaseAPI: GraphDatabaseAPI) {
        val restartedDatabase = createDatabase()
        try {
            val restartedCache = getDatabasePageCache(restartedDatabase)
            assertThat(getRecord(restartedCache, databaseAPI.databaseLayout().metadataStore(), LAST_MISSING_STORE_FILES_RECOVERY_TIMESTAMP),
                    Matchers.greaterThan(0L))
        } finally {
            managementService!!.shutdown()
        }
    }

    private class GlobalGuardConsumerTestExtensionFactory internal constructor() : ExtensionFactory<GlobalGuardConsumerTestExtensionFactory.Dependencies>("globalGuardConsumer") {
        internal interface Dependencies {
            fun globalGuard(): CompositeDatabaseAvailabilityGuard
        }

        override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
            return GlobalGuardConsumer(dependencies)
        }
    }

    private class GlobalGuardConsumer internal constructor(dependencies: GlobalGuardConsumerTestExtensionFactory.Dependencies) : LifecycleAdapter() {
        companion object {
            lateinit var globalGuard: CompositeDatabaseAvailabilityGuard
        }

        init {
            globalGuard = dependencies.globalGuard()
        }
    }

    companion object {
        private val TEN_KB = ByteUnit.kibiBytes(10).toInt()
        private fun generateSomeData(database: GraphDatabaseService) {
            for (i in 0..9) {
                database.beginTx().use { transaction ->
                    val node1 = transaction.createNode()
                    val node2 = transaction.createNode()
                    node1.createRelationshipTo(node2, RelationshipType.withName("Type$i"))
                    node2.setProperty("a", RandomStringUtils.randomAlphanumeric(TEN_KB))
                    transaction.commit()
                }
            }
        }
    }
}