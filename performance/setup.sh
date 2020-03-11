#!/bin/bash
# This script runs any setup actions needed on the producer or the consumer prior
# to the test running, like index creation.

# PRODUCER
echo "
CREATE INDEX ON :Performance(group);
RETURN true as Producer_Is_Ready;
" | docker exec --interactive neo4j-producer bin/cypher-shell \
    -a localhost -u neo4j -p producer

# CONSUMER
echo "
CREATE INDEX ON :Performance(group);
RETURN true as Consumer_Is_Ready;
" | docker exec --interactive neo4j-consumer bin/cypher-shell \
    -a localhost -u neo4j -p consumer