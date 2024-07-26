package streams.kafka.connect.sink

import org.junit.Assert.assertEquals
import org.junit.Test
import streams.kafka.connect.common.ConfigurationMigrator
import streams.kafka.connect.source.SourceType

class ConfigurationMigratorTest {

  @Test
  fun `should migrate keys to new configuration`() {
    // Given a configuration containing normal keys
    val originals =
      mapOf(
        "neo4j.topic.pattern.merge.node.properties.enabled" to "true",
        "neo4j.server.uri" to "neo4j+s://x.x.x.x",
        "neo4j.retry.max.attemps" to "1"
      )

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(originals).migrate()

    // Then the keys are updated to new key format containing the original value
    assertEquals(migratedConfig["neo4j.pattern.node.merge-properties"], "true")
    assertEquals(migratedConfig["neo4j.uri"], "neo4j+s://x.x.x.x")
    assertEquals(migratedConfig["neo4j.max-retry-attempts"], "1")
  }

  @Test fun `should not migrate keys with no matching configuration key`() {
    // Given a configuration which has no equivalent in the updated connector
    val originals = mapOf(
      "neo4j.encryption.ca.certificate.path" to "./cert.pem",
      "neo4j.source.type" to SourceType.QUERY.toString(),
      "neo4j.enforce.schema" to "true"
    )

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(originals).migrate()

    // Then the existing key is not outputted
    assertEquals("Migrated configuration should be empty", 0, migratedConfig.keys.size)
  }

  @Test
  fun `should migrate time-based keys to new configuration format`() {
    // Given a configuration originally defined in milliseconds
    var originals = mapOf(
      "neo4j.retry.backoff.msecs" to "1200",
      "neo4j.connection.max.lifetime.msecs" to "1000",
      "neo4j.batch.timeout.msecs" to "500",
      "neo4j.streaming.poll.interval.msecs" to "800"
    )

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(originals).migrate()

    // Then the new configuration should be labelled with its units
    assertEquals(migratedConfig["neo4j.max-retry-time"], "1200ms")
    assertEquals(migratedConfig["neo4j.connection-timeout"], "1000ms")
    assertEquals(migratedConfig["neo4j.batch-timeout"], "500ms")
    assertEquals(migratedConfig["neo4j.query.poll-interval"], "800ms")
  }

  @Test
  fun `should migrate prefix based keys to new configuration`() {
    // Given a configuration containing prefix/user-defined keys
    val originals =
      mapOf(
        "neo4j.topic.cypher.foo" to "CREATE (p:Person{name: event.firstName})",
        "neo4j.topic.pattern.node.bar" to "(:Bar{!barId,barName})"
      )

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(originals).migrate()

    // Then the keys are updated to new values still containing the user-defined key part
    assertEquals(
      migratedConfig["neo4j.cypher.topic.foo"],
      "CREATE (p:Person{name: event.firstName})",
    )
    assertEquals(
      migratedConfig["neo4j.pattern.node.topic.bar"],
      "(:Bar{!barId,barName})",
    )
  }
}
