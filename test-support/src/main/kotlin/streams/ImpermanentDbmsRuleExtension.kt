package streams

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Assume
import org.neo4j.test.rule.DbmsRule
import streams.config.StreamsConfig
import streams.extensions.getSystemDb

fun DbmsRule.setConfig(key: String, value: String): DbmsRule {
    StreamsConfig.registerListener { it[key] = value }
    return this
}

fun DbmsRule.start(timeout: Long = 10000): DbmsRule {
    try {
        this.restartDatabase()
    } catch (e: NullPointerException) {
        val before = DbmsRule::class.java.getDeclaredMethod("before")
        before.isAccessible = true
        before.invoke(this)
    }
    Assume.assumeTrue(this.isAvailable(timeout))
    Assume.assumeTrue(this.managementService.getSystemDb().isAvailable(timeout))
    runBlocking { delay(timeout) }
    return this
}

fun DbmsRule.shutdownSilently(): DbmsRule {
    try { this.shutdown() } catch (ignored: Exception) {}
    return this
}
