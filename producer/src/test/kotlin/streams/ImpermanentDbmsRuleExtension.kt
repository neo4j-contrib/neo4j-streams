package streams

import org.junit.Assume
import org.neo4j.configuration.SettingImpl
import org.neo4j.configuration.SettingValueParsers
import org.neo4j.test.rule.DbmsRule
import java.lang.reflect.Method

fun DbmsRule.setConfig(key: String, value: String) = this.withSetting(SettingImpl
        .newBuilder(key, SettingValueParsers.STRING, null).build(), value)

fun DbmsRule.start(timeout: Long = 5000) {
    val before: Method = DbmsRule::class.java.getDeclaredMethod("before")
    before.isAccessible = true
    before.invoke(this)
    Assume.assumeTrue(this.isAvailable(timeout))
}
