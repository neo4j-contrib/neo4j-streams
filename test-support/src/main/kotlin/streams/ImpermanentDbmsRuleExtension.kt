package streams

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.commons.configuration2.ImmutableConfiguration
import org.junit.Assume
import org.neo4j.configuration.SettingImpl
import org.neo4j.configuration.SettingValueParsers
import org.neo4j.graphdb.config.Setting
import org.neo4j.plugin.configuration.ConfigurationLifecycleUtils
import org.neo4j.plugin.configuration.EventType
import org.neo4j.plugin.configuration.listners.ConfigurationLifecycleListener
import org.neo4j.test.rule.DbmsRule
import streams.config.StreamsConfig
import streams.extensions.getSystemDb
import streams.utils.StreamsUtils
import java.util.concurrent.atomic.AtomicBoolean

private val innerConfigPrefix = "test.instance.prefix"
fun DbmsRule.setConfig(key: String, value: String): DbmsRule {
    return this.withSetting(SettingImpl.newBuilder("$innerConfigPrefix.$key", SettingValueParsers.STRING, "").build(), value)
}

private fun addSettings(db: DbmsRule) {
    val globalConfigField = DbmsRule::class.java.getDeclaredField("globalConfig")
    globalConfigField.isAccessible = true
    val globalConfig = globalConfigField.get(db) as MutableMap<Setting<*>, Any>
    val config = globalConfig
        .filterKeys { it.name().startsWith("$innerConfigPrefix.") }
        .mapKeys { it.key.name().substring("$innerConfigPrefix.".length) }
    val isReady = AtomicBoolean()
    if (config.isNotEmpty()) {
        StreamsConfig.getInstance(db).addConfigurationLifecycleListener(EventType.PROPERTY_ADDED) { evt, cfg ->
            isReady.set(true)
            println("Setting Config from APIs: ${ConfigurationLifecycleUtils.toMap(cfg)}")
        }
        StreamsConfig.getInstance(db).setProperties(config, false)
    } else {
        println("No configuration defined")
        isReady.set(true)
    }
    val ready = StreamsUtils.blockUntilFalseOrTimeout(180000, 1000) { isReady.get() }
    if (!ready) {
        println("There is a problem with the config initialization")
        throw RuntimeException("There is a problem with the config initialization")
    }
    val toRemove = globalConfig.keys
        .filter { it.name().startsWith("$innerConfigPrefix.") }
    globalConfig.keys.removeAll(toRemove)
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
    addSettings(this)
    runBlocking { delay(timeout) }
    return this
}

fun DbmsRule.shutdownSilently(): DbmsRule {
    try { this.shutdown() } catch (ignored: Exception) {
        ignored.printStackTrace()
    }
    return this
}
