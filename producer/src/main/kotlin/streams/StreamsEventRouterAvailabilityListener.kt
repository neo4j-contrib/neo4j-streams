package streams

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.internal.LogService
import org.neo4j.plugin.configuration.EventType
import streams.configuration.StreamsConfig

class StreamsEventRouterAvailabilityListener(private val db: GraphDatabaseAPI,
                                             logService: LogService): AvailabilityListener {
    private val mutex = Mutex()

    private val streamsConfig = StreamsConfig.getInstance(db)
    private val listener = StreamsRouterConfigurationListener(db, logService.getUserLog(StreamsRouterConfigurationListener::class.java))

    init {
        streamsConfig.addConfigurationLifecycleListener(EventType.CONFIGURATION_INITIALIZED,
                listener)
    }

    override fun available() {
        runBlocking {
            mutex.withLock {
                streamsConfig.start()
            }
        }
    }

    override fun unavailable() {
        runBlocking {
            mutex.withLock {
                streamsConfig.stop()
            }
        }
    }

    fun shutdown() {
        runBlocking {
            mutex.withLock {
                streamsConfig.stop(true)
                StreamsConfig.removeInstance(db)
            }
        }
    }

}