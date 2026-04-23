> [!CAUTION]
> This branch (4.1) has reached EOL (end of life) and you should not expect any new updates.
> There is a newer Neo4j Kafka connector avaialble and at https://github.com/neo4j/neo4j-kafka-connector.

# Neo4j Streaming Data Integrations

![(:Neo4j)-[:LOVES]->(:Kafka:Confluent)](https://github.com/neo4j-contrib/neo4j-streams/raw/gh-pages/3.4/images/neo4j-loves-confluent.png "(:Neo4j)-[:LOVES]->(:Kafka:Confluent)")

This project integrates Neo4j with streaming data solutions.

Currently it provides an integration with Apache Kafka and the Confluent Platform.

The project contains these components:

## Neo4j Kafka Connect Neo4j Connector

A [Kafka Connect Sink plugin](https://www.confluent.io/connector/kafka-connect-neo4j-sink/) that allows to ingest events from Kafka to Neo4j via templated Cypher statements.
([docs](https://neo4j.com/docs/kafka-streams/), [article](https://www.confluent.io/blog/kafka-connect-neo4j-sink-plugin/))

## Neo4j Server Extension

- Source: a Change-Data-Capture (CDC) implementation sends change data to Kafka topics
- Sink: a Neo4j extension that ingest data from Kafka topics into Neo4j via templated Cypher statements
- Neo4j Streams Procedures (Read & Write): Procedures to write to and read from topics interactively/programmatically

## Documentation & Articles

Here are articles, introducing the
[Neo4j Extension](https://medium.com/neo4j/a-new-neo4j-integration-with-apache-kafka-6099c14851d2)
and the
[Kafka Connect Neo4j Connector](https://www.confluent.io/blog/kafka-connect-neo4j-sink-plugin)
.

And practical applications of the extension for
[Building Data Pipelines with Kafka, Spark, Neo4j & Zeppelin](https://www.freecodecamp.org/news/how-to-leverage-neo4j-streams-and-build-a-just-in-time-data-warehouse-64adf290f093)
([part 2](https://www.freecodecamp.org/news/how-to-ingest-data-into-neo4j-from-a-kafka-stream-a34f574f5655)).

And for exchanging results of
[Neo4j Graph Algorithms within a Neo4j Cluster](https://www.freecodecamp.org/news/how-to-embrace-event-driven-graph-analytics-using-neo4j-and-apache-kafka-474c9f405e06)
.

## Installation Server Extension

You can run/test the extension
[locally with Docker](https://neo4j.com/docs/kafka-streams/docker-streams/)
or install it manually into your existing Neo4j server.

1. Download the jar-file from the [latest release](https://github.com/neo4j-contrib/neo4j-streams/releases/latest)
2. Copy `neo4j-streams-<VERSION>.jar` into `$NEO4J_HOME/plugins`
3. Update `$NEO4J_HOME/conf/neo4j.conf` with the necessary configuration.
4. Restart Neo4j

## Development & Contributions

### Build locally

```bash
$ mvn clean install
```

You'll find the build artifact in `<project_dir>/target/neo4j-streams-<VERSION>.jar`

Testing the link: [Kafka Connect Neo4j Connector locally with Docker](https://neo4j.com/docs/kafka-streams/docker-streams/).

### License

Neo4j Streams is licensed under the terms of the Apache License, version 2.0.  See `LICENSE` for more details.
