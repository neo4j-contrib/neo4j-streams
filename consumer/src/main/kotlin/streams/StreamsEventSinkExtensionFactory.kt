package streams

import org.neo4j.kernel.availability.AvailabilityGuard
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService

class StreamsEventSinkExtensionFactory : KernelExtensionFactory<StreamsEventSinkExtensionFactory.Dependencies>(ExtensionType.DATABASE,"Streams.Consumer") {

    override fun newInstance(context: KernelContext, dependencies: Dependencies): Lifecycle {
        return StreamsEventLifecycle(dependencies)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun config(): Config
        fun availabilityGuard(): AvailabilityGuard
    }

    class StreamsEventLifecycle(private val dependencies: Dependencies): LifecycleAdapter() {
        private val streamsAvailabilityListener = StreamsEventSinkAvailabilityListener(dependencies)
        private val streamsLog = dependencies.log().getUserLog(StreamsEventLifecycle::class.java)

        override fun start() {
            dependencies.availabilityGuard()
                    .addListener(streamsAvailabilityListener)
        }

        override fun stop() {
            try {
                streamsAvailabilityListener.shutdown()
            } catch (e : Throwable) {
                val message = e.message ?: "Generic error, please check the stack trace:"
                streamsLog.error(message, e)
            }
        }
    }
}

