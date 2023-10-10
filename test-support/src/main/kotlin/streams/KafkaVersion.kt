package streams

object KafkaVersion {
    /**
     * Kafka TestContainers uses Confluent OSS images.
     * We need to keep in mind which is the right Confluent Platform version for the Kafka version this project uses
     *
     * Confluent Platform | Apache Kafka
     *                    |
     * 6.0.x	          | 2.6.x
     * 6.1.x	          | 2.7.x
     * 6.2.x	          | 2.8.x
     * 7.0.x	          | 3.0.x
     * 7.1.x	          | 3.1.x
     * 7.2.x	          | 3.2.x
     * 7.3.x	          | 3.3.x
     * 7.4.x	          | 3.4.x
     * 7.5.x	          | 3.5.x
     *
     * Please see also https://docs.confluent.io/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility
     */

    const val CURRENT = "6.0.15"
}
