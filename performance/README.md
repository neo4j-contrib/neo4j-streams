# neo4j-streams performance test

This part of the project aims to give a standard way to understand the performances of the platform and highlight the configuration results.



Currently, it's available only the test of neo4j to neo4j configuration, in remote too.

## Setup

The basic unit of work used to test the performances needs a specific cypher part in order to get the timestamp of creation and receiving of the event. The elapsed time is calculated using these timestamps through cypher queries.

### Producer

- neo4j 3.4.7 community edition
- APOC 3.4.0.3
- 8 GB heap memory
- CREATE INDEX ON :Performance(group)
- default neo4j-streams configuration (without filtering)
- kafka.batch.size=16384

The  creation query used by the tester:

	UNWIND range(1,{msg}) as ran
	CREATE (n:Performance)
	SET n.group = {uuid}, n.creation_time = apoc.date.currentTimestamp()
	RETURN min(n.creation_time) as creation_time
### Consumer

- neo4j 3.4.7 community edition
- APOC 3.4.0.3
- 2 GB heap memory
- CREATE INDEX ON :Performance(group)
- default neo4j-streams configuration
- kafka.max.poll.records=16384

The acquisition query:

```properties
streams.sink.topic.neo4j=WITH event.value.payload AS payload, event.value.meta AS meta CALL apoc.do.case( [\

payload.type = 'node' AND meta.operation = 'created', \

'CREATE (x:Performance {received_time: apoc.date.currentTimestamp()}) SET x+=props RETURN count(x)'] \

,'RETURN 0', \

{props:  payload.after.properties}) \

YIELD value RETURN count(value)
```



## Run the tests

The tester is written in Python. Check your system for:

- Python 3.6
- py2neo
- matplotlib

### config.ini

To run the test you need to edit the *config.ini* file in order to setup the connection data for the *producer* and *consumer* neo4j instances.

The *unit* part is used as default values to run the tests. 

- *repeat* is the number of times each test is executed
- *node* is the number of nodes created for each test

```ini
[producer]
url = bolt://localhost:8687
username = neo4j
password = producer

[consumer]
url = bolt://localhost:7687
username = neo4j
password = consumer

[unit]
repeat = 5
nodes = 1000
```



### start

To check the coniguration or to get a fast result you can run the command

```shell
python3.6 neo4j-streams-pt.py --start
```

The output is a windows that show you the distribution of the test. See result session to better understand the values. You can use the option `--out file.png` to not show the result but save it on file.

### baseline

This is the main command you have to test the system. By command line arguments you can specify the series to test. The arguments are the number of  node's unit to use. 

ES: 3 = 3 * 1000 if 1000 is the value configured as *nodes* in the *[unit]* part.

To get the same result of *start*:

```shell
python3.6 neo4j-streams-pt.py --baseline 1
```

The get more distribution (real case):

```shell
python3.6 neo4j-streams-pt.py --baseline 1 10 100 1000 --out results.png
```

This means the tester execute 5 (if *repeat = 5*) times each series. A series is comped by 1k, 10k, 100k, 1000k nodes (if *nodes = 1000*)

## Results

Here's the results with the setup as described above on a MacBook Pro with

- CPU 2,2 GHz Intel Core i7
- 16 GB DDR3, 1600 MHz
- SSD
- neo4j producer, neo4j consumer, Kafka and Zookeeper run on the same machine, the tester too. The clock is the same, so it's synchronized.
- Kafka and Zookeeper run with default configuration

![Result on standalone mac](https://github.com/neo4j-contrib/neo4j-streams/blob/master/performance/standalone_mac_standard.png)

As you can see, there's not a single number to describe the scenario, but 0.15 ms per node (for the simple CREATE query) colud be a reference time.



