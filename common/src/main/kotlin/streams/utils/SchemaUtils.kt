package streams.utils

import streams.events.Constraint
import streams.events.StreamsConstraintType
import streams.events.StreamsTransactionEvent
import streams.serialization.JSONUtils
import streams.service.StreamsSinkEntity
import streams.utils.StreamsUtils.RelKeyStrategy.DEFAULT
import streams.utils.StreamsUtils.RelKeyStrategy.ALL

object SchemaUtils {
    fun getNodeKeys(labels: List<String>, propertyKeys: Set<String>, constraints: List<Constraint>, keyStrategy: StreamsUtils.RelKeyStrategy = DEFAULT): Set<String> =
            constraints
                .filter { constraint ->
                    constraint.type == StreamsConstraintType.UNIQUE
                            && propertyKeys.containsAll(constraint.properties)
                            && labels.contains(constraint.label)
                }
                .let {
                    when (keyStrategy) {
                        DEFAULT -> {
                            // with 'DEFAULT' we order first by properties.size, then by label name and finally by properties name alphabetically
                            // with properties.sorted() we ensure that ("foo", "bar") and ("bar", "foo") are no different
                            // with toString() we force it.properties to have the natural sort order, that is alphabetically
                            it.minWith((compareBy({ it.properties.size }, { it.label }, { it.properties.sorted().toString() })))
                                    ?.properties
                                    .orEmpty()
                        }
                        // with 'ALL' strategy we get all properties sorted alphabetically
                        ALL -> it.flatMap { it.properties }.toSortedSet()
                    }
                }

    fun toStreamsTransactionEvent(streamsSinkEntity: StreamsSinkEntity,
                                  evaluation: (StreamsTransactionEvent) -> Boolean)
            : StreamsTransactionEvent? = if (streamsSinkEntity.value != null) {
        val data = JSONUtils.asStreamsTransactionEvent(streamsSinkEntity.value)
        if (evaluation(data)) data else null
    } else {
        null
    }

}