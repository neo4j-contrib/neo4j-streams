package streams.kafka.connect.common

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.Assert.assertEquals
import org.junit.Test
import streams.kafka.connect.source.SourceType
import java.io.File


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
    val migratedConfig = ConfigurationMigrator(originals).migrateToV51()

    // Then the keys are updated to new key format containing the original value
    assertEquals(originals.size, migratedConfig.size)
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
    val migratedConfig = ConfigurationMigrator(originals).migrateToV51()

    // Then the existing key is not outputted
    assertEquals("Migrated configuration should be empty", 0, migratedConfig.size)
  }

  @Test
  fun `should migrate time-based keys to new configuration format`() {
    // Given a configuration originally defined in milliseconds
    val originals = mapOf(
      "neo4j.retry.backoff.msecs" to "1200",
      "neo4j.connection.max.lifetime.msecs" to "1000",
      "neo4j.batch.timeout.msecs" to "500",
      "neo4j.streaming.poll.interval.msecs" to "800"
    )

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(originals).migrateToV51()

    // Then the new configuration should be labelled with its units
    assertEquals(originals.size, migratedConfig.size)
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
    val migratedConfig = ConfigurationMigrator(originals).migrateToV51()

    // Then the keys are updated to new values still containing the user-defined key part
    assertEquals(originals.size, migratedConfig.size)
    assertEquals(
      migratedConfig["neo4j.cypher.topic.foo"],
      "CREATE (p:Person{name: event.firstName})",
    )
    assertEquals(
      migratedConfig["neo4j.pattern.node.topic.bar"],
      "(:Bar{!barId,barName})",
    )
  }

  @Test
  fun `should migrate across unknown configuration options`() {
    // Given a configuration with non-defined configuration options
    val originals = mapOf(
      "connector.class" to "streams.kafka.connect.source.Neo4jSourceConnector",
      "key.converter" to "io.confluent.connect.avro.AvroConverter",
      "arbitrary.config.key" to "arbitrary.value"
    )

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(originals).migrateToV51()

    // Then those options should still be included
    assertEquals(originals.size, migratedConfig.size)
    assertEquals(migratedConfig["connector.class"], "streams.kafka.connect.source.Neo4jSourceConnector")
    assertEquals(migratedConfig["key.converter"], "io.confluent.connect.avro.AvroConverter")
    assertEquals(migratedConfig["arbitrary.config.key"], "arbitrary.value")
  }

  @Test
  fun `should migrate keys from full source quickstart configuration example`() {
    // Given the configuration from the quickstart example
    val quickstartSettings = loadConfiguration("src/test/resources/exampleConfigs/sourceExample.json")

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(quickstartSettings).migrateToV51()

    // Then the keys are updated correctly
    assertEquals(12, migratedConfig.size)

    assertEquals(migratedConfig["neo4j.query.topic"], "my-topic")
    assertEquals(migratedConfig["connector.class"], "streams.kafka.connect.source.Neo4jSourceConnector")
    assertEquals(migratedConfig["key.converter"], "io.confluent.connect.avro.AvroConverter")
    assertEquals(migratedConfig["key.converter.schema.registry.url"], "http://schema-registry:8081")
    assertEquals(migratedConfig["value.converter"], "io.confluent.connect.avro.AvroConverter")
    assertEquals(migratedConfig["value.converter.schema.registry.url"], "http://schema-registry:8081")
    assertEquals(migratedConfig["neo4j.uri"], "bolt://neo4j:7687")
    assertEquals(migratedConfig["neo4j.authentication.basic.username"], "neo4j")
    assertEquals(migratedConfig["neo4j.authentication.basic.password"], "password")
    assertEquals(migratedConfig["neo4j.query.poll-interval"], "5000ms")
    assertEquals(migratedConfig["neo4j.query.streaming-property"], "timestamp")
    assertEquals(
      migratedConfig["neo4j.query"],
      "MATCH (ts:TestSource) WHERE ts.timestamp > \$lastCheck RETURN ts.name AS name, ts.surname AS surname, ts.timestamp AS timestamp"
    )

    assertEquals(migratedConfig["neo4j.enforce.schema"], null)
    assertEquals(migratedConfig["neo4j.streaming.from"], null)
  }

  @Test
  fun `should migrate keys from full sink quickstart configuration example`() {
    // Given the configuration from the quickstart example
    val quickstartSettings = loadConfiguration("src/test/resources/exampleConfigs/sinkExample.json")

    // When the configuration is migrated
    val migratedConfig = ConfigurationMigrator(quickstartSettings).migrateToV51()

    // Then the keys are updated correctly
    assertEquals(15, migratedConfig.size)

    assertEquals(migratedConfig["topics"], "my-topic")
    assertEquals(migratedConfig["connector.class"], "streams.kafka.connect.sink.Neo4jSinkConnector")
    assertEquals(migratedConfig["key.converter"], "org.apache.kafka.connect.json.JsonConverter")
    assertEquals(migratedConfig["key.converter.schemas.enable"], "false")
    assertEquals(migratedConfig["value.converter"], "org.apache.kafka.connect.json.JsonConverter")
    assertEquals(migratedConfig["value.converter.schemas.enable"], "false")
    assertEquals(migratedConfig["errors.retry.timeout"], "-1")
    assertEquals(migratedConfig["errors.retry.delay.max.ms"], "1000")
    assertEquals(migratedConfig["errors.tolerance"], "all")
    assertEquals(migratedConfig["errors.log.enable"], "true")
    assertEquals(migratedConfig["errors.log.include.messages"], "true")
    assertEquals(migratedConfig["neo4j.uri"], "bolt://neo4j:7687")
    assertEquals(migratedConfig["neo4j.authentication.basic.username"], "neo4j")
    assertEquals(migratedConfig["neo4j.authentication.basic.password"], "password")
    assertEquals(migratedConfig["neo4j.cypher.topic.my-topic"], "MERGE (p:Person{name: event.name, surname: event.surname}) MERGE (f:Family{name: event.surname}) MERGE (p)-[:BELONGS_TO]->(f)")
  }

  private fun loadConfiguration(path: String): Map<String, String> {
    val file = File(path)
    val json = file.readText()

    val mapper = ObjectMapper()
    val node = mapper.readTree(json)
    val config = node.get("config")
    val result: Map<String?, String?>? = mapper.convertValue(config, object : TypeReference<Map<String?, String?>?>() {})
    return result as Map<String, String>
  }

}
