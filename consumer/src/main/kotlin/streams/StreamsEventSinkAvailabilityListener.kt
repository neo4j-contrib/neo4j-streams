package streams

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.plugin.configuration.EventType
import streams.configuration.StreamsConfig
import streams.utils.StreamsUtils
import java.util.concurrent.ConcurrentHashMap

class StreamsEventSinkAvailabilityListener(dependencies: StreamsEventSinkExtensionFactory.Dependencies): AvailabilityListener {
    private val db = dependencies.graphdatabaseAPI()
    private val logService = dependencies.log()

    private val mutex = Mutex()

    private val streamsConfig = StreamsConfig.getInstance(db)
    private val listener = StreamsSinkConfigurationListener(db, logService.getUserLog(StreamsSinkConfigurationListener::class.java))

    init {
        streamsConfig.addConfigurationLifecycleListener(EventType.CONFIGURATION_INITIALIZED,
                listener)
    }

    override fun available() {
        runBlocking {
            mutex.withLock {
                setAvailable(db, true)
                streamsConfig.start()
            }
        }
    }

    override fun unavailable() {
        runBlocking {
            mutex.withLock {
                setAvailable(db, false)
                streamsConfig.stop()
            }
        }
    }

    fun shutdown() {
        runBlocking {
            mutex.withLock {
                streamsConfig.stop(true)
                remove(db)
            }
        }
    }

    companion object {
        // this is necessary in case for local testing with a causal cluster
        @JvmStatic private val available = ConcurrentHashMap<String, Boolean>()

        fun isAvailable(db: GraphDatabaseAPI) = available.getOrDefault(StreamsUtils.getName(db), false)

        fun setAvailable(db: GraphDatabaseAPI, isAvailable: Boolean): Unit = available.set(StreamsUtils.getName(db), isAvailable)

        fun remove(db: GraphDatabaseAPI) {
            available.remove(StreamsUtils.getName(db))
            StreamsConfig.removeInstance(db)
        }

    }
}