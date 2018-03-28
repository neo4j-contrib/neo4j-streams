package kafka

import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node

data class NodePattern(val topic:String, val labels:List<String> = emptyList(), val all:Boolean = true, val include:List<String> = emptyList(), val exclude:List<String> = emptyList()) {
    companion object {
        private val semicolonReg = "\\s*;\\s*".toRegex()
        private val patternReg = "^(\\w+\\s*(?::\\s*(?:[\\w|\\*]+)\\s*)*)\\s*(?:\\{\\s*(-?[\\w|\\*]+\\s*(?:,\\s*-?[\\w|\\*]+\\s*)*)\\})?$".toRegex()
        private val colonReg = "\\s*:\\s*".toRegex()

        fun forNode(patterns: List<NodePattern>, node: Node) : List<NodePattern> =
                patterns.filter { it.labels.isEmpty() || it.labels.any { l -> node.hasLabel(Label.label(l)) } }

        fun toNodeRecords(node: Node, state: UpdateState, patterns: List<NodePattern>) : Map<String,NodeRecord> =
                NodePattern.forNode(patterns, node).associate { p -> p.topic to NodeRecord.apply(node, p, state) }

        fun parse(pattern: String, topics:List<String>): List<NodePattern> {
            return pattern.split(semicolonReg).map { np ->
                val matcher = patternReg.matchEntire(np)
        //                    println("$np -> ${matcher?.groupValues}")
                if (matcher == null) null
                else {
                    val parts = matcher.groupValues[1].split(colonReg)
                    val props = matcher.groupValues[2].trim().let { if (it.isEmpty()) emptyList() else it.split("\\s*,\\s*".toRegex()) }
                    val include = if (props.isEmpty()) emptyList() else props.filter { it != "*" && !it.startsWith('-') }
                    val exclude = if (props.isEmpty()) emptyList() else props.filter { it != "*" && it.startsWith('-') }.map { it.substring(1) }
                    val all = props.isEmpty() || props.contains("*") || !exclude.isEmpty()
                    val topic = parts.first().trim().let { if (topics.contains(it)) it else topics.first() }
                    val labels = if (parts.size > 1) (if (topics.contains(parts.first().trim())) parts.drop(1) else parts )else emptyList()
                    NodePattern(topic = topic, labels = if (labels.contains("*")) emptyList() else labels, all = all, include = include, exclude = exclude)
                }
            }.filterNotNull()
        }
    }
}
