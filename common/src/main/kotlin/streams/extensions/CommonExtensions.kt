package streams.extensions

import org.neo4j.graphdb.Node
import javax.lang.model.SourceVersion

fun Map<String,String>.getInt(name:String, defaultValue: Int) = this.get(name)?.toInt() ?: defaultValue

fun Node.labelNames() : List<String> {
    return this.labels.map { it.name() }
}

fun String.toPointCase(): String {
    return this.split("(?<=[a-z])(?=[A-Z])".toRegex()).joinToString(separator = ".").toLowerCase()
}

fun String.quote(): String {
    return if (SourceVersion.isIdentifier(this)) this else "`$this`"
}