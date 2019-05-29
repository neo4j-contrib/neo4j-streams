package streams.utils

import streams.extensions.quote

object IngestionUtils {
    const val labelSeparator = ":"
    const val keySeparator = ", "

    fun getLabelsAsString(labels: Collection<String>): String = labels
            .map { it.quote() }
            .joinToString(labelSeparator)

    fun getNodeKeysAsString(prefix: String = "properties", keys: Set<String>): String = keys
            .map { toQuotedProperty(prefix, it) }
            .joinToString(keySeparator)

    private fun toQuotedProperty(prefix: String = "properties", property: String): String {
        val quoted = property.quote()
        return "$quoted: event.$prefix.$quoted"
    }
}