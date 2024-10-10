# Neo4j Connector for Kafka 5.0

![(:Neo4j)-[:LOVES]->(:Kafka:Confluent)](https://github.com/neo4j-contrib/neo4j-streams/raw/gh-pages/3.4/images/neo4j-loves-confluent.png "(:Neo4j)-[:LOVES]->(:Kafka:Confluent)")

This project integrates Neo4j with *Apache Kafka and the Confluent Platform*.

> [!IMPORTANT]  
> Newer versions of this connector are now maintained at https://github.com/neo4j/neo4j-kafka-connector.
> This repository is only kept alive for critical bug and security fixes of 5.0.x versions of the connector.

## Neo4j Kafka Connect Neo4j Connector

You can download the [Kafka Connect plugin](https://www.confluent.io/hub/neo4j/kafka-connect-neo4j) that allows to ingest events from Kafka to Neo4j and generate change events from Neo4j into Kafka.

## Documentation

Refer to [documentation](https://neo4j.com/docs/kafka/) for more information about installation and configuration of the connector.

## Feedback & Suggestions

As highlighted above, 5.0.x version of the connector is kept as a maintenance version for only critical bug and security fixes.
Please raise any feature requests on the [new repository](https://github.com/neo4j/neo4j-kafka-connector).

### Development

## Build locally

In order to build the packages, execute the following command.

```shell
mvn clean package
```

You'll find the build artifact in `<project_dir>/kafka-connect-neo4j/target/neo4j-kafka-connect-neo4j-<VERSION>.jar`

### Docs

The documentation source for this version lives at [this repository](https://github.com/neo4j/docs-kafka-connector).
Please raise any documentation updates by creating a PR against it.

## License

Neo4j Streams is licensed under the terms of the Apache License, version 2.0.  See `LICENSE` for more details. 
