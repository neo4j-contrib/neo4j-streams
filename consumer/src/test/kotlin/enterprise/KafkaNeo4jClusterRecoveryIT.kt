package enterprise

import integrations.kafka.KafkaEventSinkBase
import integrations.kafka.KafkaEventSinkSuiteIT
import junit.framework.Assert.assertTrue
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.neo4j.causalclustering.core.consensus.roles.Role
import org.neo4j.causalclustering.discovery.Cluster
import org.neo4j.causalclustering.discovery.CoreClusterMember
import org.neo4j.consistency.ConsistencyCheckService
import org.neo4j.function.ThrowingSupplier
import org.neo4j.helpers.progress.ProgressMonitorFactory
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.configuration.Settings
import org.neo4j.logging.NullLogProvider
import org.neo4j.test.DbRepresentation
import org.neo4j.test.assertion.Assert.assertEventually
import org.neo4j.test.causalclustering.ClusterRule
import streams.configuration.StreamsConfig
import streams.events.StreamsPluginStatus
import streams.procedures.StreamsSinkProcedures
import streams.serialization.JSONUtils
import streams.utils.StreamsUtils
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.streams.toList

fun Cluster<*>.getStoreDirs(): Set<File> = this.coreMembers()
        .map(CoreClusterMember::databaseDirectory)
        .toSet()

fun Cluster<*>.getDbRepresentations(): Set<DbRepresentation> = this.coreMembers()
        .map(CoreClusterMember::databasesDirectory)
        .map(DbRepresentation::of)
        .toSet()

class KafkaNeo4jClusterRecoveryIT: KafkaEventSinkBase() {

    private val topic: String? = UUID.randomUUID().toString()

    @Rule @JvmField
    val clusterRule = ClusterRule()
            .withSharedCoreParam(Settings.setting("streams.sink.topic.cypher.$topic", Settings.STRING, ""), cypherQueryTemplate)
            .withSharedCoreParam(Settings.setting("kafka.bootstrap.servers", Settings.STRING, ""), KafkaEventSinkSuiteIT.kafka.bootstrapServers)
            .withSharedCoreParam(Settings.setting("kafka.zookeeper.connect", Settings.STRING, ""), KafkaEventSinkSuiteIT.kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"])
            .withSharedCoreParam(Settings.setting("streams.sink.enabled", Settings.STRING, ""), "true")
            .withSharedCoreParam(Settings.setting("dbms.tx_log.rotation.size", Settings.STRING, ""), "1M")
            .withSharedCoreParam(Settings.setting("streams.check.writeable.instance.interval", Settings.STRING, ""), "100")
            .withSharedCoreParam(Settings.setting("streams.cluster.only", Settings.STRING, ""), "true")
            .withSharedCoreParam(Settings.setting("streams.sink.poll.interval", Settings.STRING, ""), "1000")
            .withSharedCoreParam(Settings.setting("unsupported.dbms.tx_log.fail_on_corrupted_log_files", Settings.STRING, ""), "false")
            .withNumberOfCoreMembers(3)
            .withNumberOfReadReplicas(0)

    private lateinit var cluster: Cluster<*>

    @Before
    fun before() {
        val declaredField = clusterRule.javaClass.getDeclaredField("coreParams")
        declaredField.isAccessible = true
        val configMap: Map<String, String> = declaredField.get(clusterRule) as Map<String, String>
        cluster = clusterRule.startCluster()
        cluster.awaitLeader()
        if (configMap.isNotEmpty()) {
            println("Applying the following configuration to the cluster $configMap")
            cluster.coreMembers().forEach {
                StreamsConfig.getInstance(it.database())
                        .setProperties(configMap, false)
            }
        }
        val success = StreamsUtils.blockUntilFalseOrTimeout(100000, 1000) {
            StreamsSinkProcedures.hasStatus(cluster.getMemberWithRole(Role.LEADER).database(), StreamsPluginStatus.RUNNING)
        }
        if (success) {
            println("Plugin successfully started")
        } else {
            println("Plugin not started")
        }
    }

    @Test
    fun shouldBeConsistentAfterShutdown() {
        // given
        initClusterData(cluster)
        fireSomeLoadAtTheCluster(cluster)
        val storeDirs = cluster.getStoreDirs()

        // when
        cluster.shutdown()

        // then
        storeDirs.forEach{ storeDir -> assertConsistent(storeDir) }
        assertEventually("All cores have the same data",
                ThrowingSupplier<Int, Exception> { cluster.getDbRepresentations().size },
                equalTo(1), 10, TimeUnit.SECONDS)
    }

    private fun initClusterData(cluster: Cluster<*>) {
        cluster.coreTx { db, tx ->
            db.execute("MATCH (n) DETACH DELETE n")
            db.execute("""|UNWIND range(1, 100000) AS id
                |CREATE (n:Demo{id: id})
            """.trimMargin())
            tx.success()
        }
    }

    @Test
    fun singleServerWithinClusterShouldBeConsistentAfterRestart() {
        // given
        val clusterSize = cluster.numberOfCoreMembersReportedByTopology()
        initClusterData(cluster)
        //fireSomeLoadAtTheCluster(cluster)
        val storeDirs = cluster.getStoreDirs()

        // when
        for (i in 0 until clusterSize) {
            val coreMemberById = cluster.getCoreMemberById(i)
            val databasesDirectory = coreMemberById.databasesDirectory()
            val neoStoreFiles = Files.walk(databasesDirectory.toPath()).use {
                it.filter { it.fileName.toString().contains("neostore.transaction.db.") }
                        .map(Path::toFile)
                        .toList()
                        .sortedBy { it.name }
            }
            val last = neoStoreFiles.last()
            cluster.removeCoreMemberWithServerId(i)
            println("Removing tx log ${last.name} on cluster id $i")
            FileUtils.forceDelete(last)
            println("Waiting for a new LEADER")
            cluster.awaitLeader()
            fireSomeLoadAtTheCluster(cluster)
            println("Attaching CORE $i back to the cluster")
            cluster.addCoreMemberWithId(i).start()
            println("CORE $i attached to che cluster")
        }

        // then
        assertEventually("All cores have the same data",
                ThrowingSupplier<Int, Exception> { cluster.getDbRepresentations().size },
                equalTo(1), 30, TimeUnit.SECONDS)
        cluster.shutdown()
        storeDirs.forEach { storeDir: File -> assertConsistent(storeDir) }
    }

    private fun assertConsistent(storeDir: File) {
        val result = try {
            ConsistencyCheckService()
                    .runFullConsistencyCheck(DatabaseLayout.of(storeDir), Config.defaults(),
                            ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), true)
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
        assertTrue(result.isSuccessful)
    }

    private fun fireSomeLoadAtTheCluster(cluster: Cluster<*>) = runBlocking {
        delay(1000)
        val records = 1
        (1..records).forEach {
            val dataProperties = mapOf("prop1" to "foo $it", "bar" to it)
            val data = mapOf("id" to it, "properties" to dataProperties)
            val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
            val metadata = kafkaProducer.send(producerRecord).get()
            println("Sent record $it to topic ${metadata.topic()}")
        }
        assertEventually("The data should be consumed successfully",
                ThrowingSupplier<Int, Exception> {
                    val atomicCount = AtomicInteger()
                    cluster.coreTx { db, tx ->
                        val count = db.execute("MATCH (n:Label) RETURN count(n) AS count").columnAs<Long>("count").next()
                        tx.success()
                        atomicCount.set(count.toInt())
                    }
                    atomicCount.get()
                },
                equalTo(records), 30, TimeUnit.SECONDS)
    }
}