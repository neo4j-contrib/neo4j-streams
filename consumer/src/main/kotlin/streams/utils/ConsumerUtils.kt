package streams.utils

import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.StreamsEventSinkAvailabilityListener

object ConsumerUtils {

    fun isWriteableInstance(db: GraphDatabaseAPI): Boolean = Neo4jUtils
        .isWriteableInstance(db) { StreamsEventSinkAvailabilityListener.isAvailable(db) }

    fun <T> executeInWriteableInstance(db: GraphDatabaseAPI,
                                       action: () -> T?): T? = Neo4jUtils.executeInWriteableInstance(db,
        { StreamsEventSinkAvailabilityListener.isAvailable(db) }, action)

}