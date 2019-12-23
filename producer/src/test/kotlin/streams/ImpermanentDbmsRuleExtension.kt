package streams

import org.neo4j.test.rule.DbmsRule
import streams.config.StreamsConfig

fun DbmsRule.setConfig(key: String, value: String): DbmsRule {
    StreamsConfig.registerListener { it.put(key, value) }
    return this
}
