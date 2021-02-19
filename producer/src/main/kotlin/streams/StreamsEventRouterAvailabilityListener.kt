package streams

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.internal.LogService
import org.neo4j.plugin.configuration.EventType
import streams.config.StreamsConfig
import streams.utils.StreamsUtils
import java.util.concurrent.ConcurrentHashMap

class StreamsEventRouterAvailabilityListener(private val db: GraphDatabaseAPI,
                                             logService: LogService): AvailabilityListener {

    private val listener = StreamsRouterConfigurationListener(db, logService.getUserLog(StreamsRouterConfigurationListener::class.java))

    private val streamsConfig = StreamsConfig.getInstance(db)

    private val mutex = Mutex()

    init {
        streamsConfig.addConfigurationLifecycleListener(EventType.CONFIGURATION_INITIALIZED, listener)
    }

    override fun available() = runBlocking {
        mutex.withLock {
            setAvailable(db, true)
            streamsConfig.start()
        }
    }

    override fun unavailable() = runBlocking {
        mutex.withLock {
            setAvailable(db, false)
            streamsConfig.stop()
        }
    }

    fun shutdown() = runBlocking {
        mutex.withLock {
            remove(db)
        }
    }

    companion object {
        @JvmStatic private val available = ConcurrentHashMap<String, Boolean>()

        fun isAvailable(db: GraphDatabaseAPI) = available.getOrDefault(db.databaseName(), false)

        fun setAvailable(db: GraphDatabaseAPI, isAvailable: Boolean): Unit = available.set(db.databaseName(), isAvailable)

        fun remove(db: GraphDatabaseAPI) {
            available.remove(StreamsUtils.getName(db))
            StreamsConfig.removeInstance(db)
        }

    }

}