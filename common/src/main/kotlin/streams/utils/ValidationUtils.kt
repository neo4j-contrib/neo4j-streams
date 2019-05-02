package streams.utils

object ValidationUtils {

    fun validateTopics(cdcMergeTopics: Set<String>, cdcSchemaTopics: Set<String>,
                       cypherTopics: Set<String>, nodePatternTopics: Set<String>, relPatternTopics: Set<String>) {
        val allTopicsLists = mutableListOf<String>()
        allTopicsLists.addAll(cdcMergeTopics)
        allTopicsLists.addAll(cdcSchemaTopics)
        allTopicsLists.addAll(cypherTopics)
        allTopicsLists.addAll(nodePatternTopics)
        allTopicsLists.addAll(relPatternTopics)
        val crossDefinedTopics = allTopicsLists.map { it to 1 }
                .groupBy({ it.first }, { it.second })
                .mapValues { it.value.reduce { acc, i -> acc + i } }
                .filterValues { it > 1 }
                .keys
        if (crossDefinedTopics.isNotEmpty()) {
            throw RuntimeException("The following topics are cross defined: $crossDefinedTopics")
        }
    }
}