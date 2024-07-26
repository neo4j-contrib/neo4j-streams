package streams.kafka.connect.sink

import org.junit.Assert.assertEquals
import org.junit.Test
import streams.kafka.connect.common.ConfigurationMigrator

class ConfigurationMigratorTest {

  @Test
  fun `should migrate keys to new configuration`() {
    // Given a configuration containing normal keys
    val originals =
      mapOf(
        "neo4j.topic.pattern.merge.node.properties.enabled" to "true",
        "${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}foo" to
          "CREATE (p:Person{name: event.firstName})",
      )

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(originals).migrate()

    // Then the keys are updated to new key format containing existing value
    assertEquals(migratedConfig["neo4j.pattern.node.merge-properties"], "true")
    assertEquals(
      migratedConfig["neo4j.cypher.topic.foo"],
      "CREATE (p:Person{name: event.firstName})",
    )
  }

  @Test fun `should not migrate keys with no matching configuration key`() {}

  @Test
  fun `should migrate time-based keys to new configuration format`() {
    // Given a configuration originally defined in milliseconds
    var originals = mapOf("neo4j.retry.backoff.msecs" to "1200")

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(originals).migrate()

    // Then the new configuration should be labelled with its units
    assertEquals(migratedConfig["neo4j.max-retry-time"], "1200ms")
  }

  @Test
  fun `should migrate prefix based keys to new configuration`() {
    // Given a configuration containing prefix/user-defined keys
    val originals =
      mapOf(
        "${Neo4jSinkConnectorConfig.TOPIC_CYPHER_PREFIX}foo" to
          "CREATE (p:Person{name: event.firstName})"
      )

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(originals).migrate()

    // Then the keys are updated to new values still containing the user-defined key part
    assertEquals(
      migratedConfig["neo4j.cypher.topic.foo"],
      "CREATE (p:Person{name: event.firstName})",
    )
  }
}
