package streams.utils

import streams.events.Constraint
import streams.events.StreamsConstraintType

object SchemaUtils {
    fun getNodeKeys(labels: List<String>, propertyKeys: Set<String>, constraints: List<Constraint>): Set<String> =
            constraints
                .filter { constraint ->
                    constraint.type == StreamsConstraintType.UNIQUE
                            && propertyKeys.containsAll(constraint.properties)
                            && labels.contains(constraint.label)
                }
                .minBy { it.properties.size }
                ?.properties
                .orEmpty()
//                .ifEmpty { propertyKeys }

}