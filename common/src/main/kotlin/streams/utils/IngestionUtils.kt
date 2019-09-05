package streams.utils

import streams.extensions.quote

object IngestionUtils {
    const val labelSeparator = ":"
    const val keySeparator = ", "

    fun getLabelsAsString(labels: Collection<String>): String = labels
            .map { it.quote() }
            .joinToString(labelSeparator)
            .let { if (it.isNotBlank()) "$labelSeparator$it" else it }

    fun getNodeKeysAsString(prefix: String = "properties", keys: Set<String>): String = keys
            .map { toQuotedProperty(prefix, it) }
            .joinToString(keySeparator)

    private fun toQuotedProperty(prefix: String = "properties", property: String): String {
        val quoted = property.quote()
        return "$quoted: event.$prefix.$quoted"
    }

    fun getNodeMergeKeys(prefix: String, keys: Set<String>): String = keys
            .map {
                val quoted = it.quote()
                "$quoted: event.$prefix.$quoted"
            }
            .joinToString(keySeparator)

    fun containsProp(key: String, properties: List<String>): Boolean = if (key.contains(".")) {
        properties.contains(key) || properties.any { key.startsWith("$it.") }
    } else {
        properties.contains(key)
    }
}