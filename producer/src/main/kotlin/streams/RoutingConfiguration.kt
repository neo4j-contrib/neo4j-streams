package streams

abstract class RoutingConfiguration(val topic: String = "neo4j",
                                    val all: Boolean = true,
                                    val include: List<String> = emptyList(),
                                    val exclude: List<String> = emptyList()) {
}

data class NodeRoutingConfiguration(val labels: List<String> = emptyList()) : RoutingConfiguration(){

    companion object {
        fun parse(line : String) : NodeRoutingConfiguration{
            val default = NodeRoutingConfiguration()
            //TODO
            return default
        }
    }
}

data class RelationshipRoutingConfiguration(val name: String = "") : RoutingConfiguration(){

    companion object {
        fun parse(line : String) : RelationshipRoutingConfiguration{
            val default = RelationshipRoutingConfiguration()
            //TODO
            return default
        }
    }
}
