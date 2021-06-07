package streams

import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.availability.AvailabilityGuard
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService
import streams.extensions.isSystemDb
import java.util.concurrent.atomic.AtomicReference

class StreamsEventSinkExtensionFactory : ExtensionFactory<StreamsEventSinkExtensionFactory.Dependencies>(ExtensionType.DATABASE,"Streams.Consumer") {

    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        return StreamsEventLifecycle(dependencies)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun dbms(): DatabaseManagementService
        fun log(): LogService
        fun availabilityGuard(): AvailabilityGuard
    }

    class StreamsEventLifecycle(private val dependencies: Dependencies): LifecycleAdapter() {
        private val db = dependencies.graphdatabaseAPI()
        private val logService = dependencies.log()
        private val streamsLog = logService.getUserLog(StreamsEventLifecycle::class.java)
        private val availabilityListener: AtomicReference<StreamsEventSinkAvailabilityListener> = AtomicReference(null)

        private fun createStreamsEventSinkAvailabilityListener() = if (db.isSystemDb()) {
            null
        } else {
            StreamsEventSinkAvailabilityListener(dependencies)
        }

        override fun start() {
            availabilityListener.updateAndGet { it ?: createStreamsEventSinkAvailabilityListener() }
                    ?.let { dependencies.availabilityGuard().addListener(it) }
        }

        override fun stop() {
            try {
                availabilityListener.getAndSet(null)?.let {
                    it.shutdown()
                    dependencies.availabilityGuard().removeListener(it)
                }
            } catch (e : Throwable) {
                val message = e.message ?: "Generic error, please check the stack trace:"
                streamsLog.error(message, e)
            }
        }
    }
}

