package integrations.kafka

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network


class SchemaRegistryContainer(version: String): GenericContainer<SchemaRegistryContainer>("confluentinc/cp-schema-registry:$version") {

    override fun start() {
        withExposedPorts(PORT)
        waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
        super.start()
    }

    fun withKafka(kafka: KafkaContainer): SchemaRegistryContainer {
        return withKafka(kafka.network, kafka.networkAliases[0] + ":9092")
    }

    fun withKafka(network: Network, bootstrapServers: String): SchemaRegistryContainer {
        withNetwork(network)
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://$bootstrapServers")
        return self()
    }


    fun getSchemaRegistryUrl() = "http://localhost:${getMappedPort(PORT)}"

    companion object {
        @JvmStatic val PORT = 8081
    }
}