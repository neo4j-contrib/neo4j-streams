# neo4j-streams performance test

This part of the project aims to give a standard way to understand the performances of the platform and highlight the configuration results.

Currently, it's available only the test of neo4j to neo4j configuration, in remote too.

## Setup

The basic unit of work used to test the performances needs a specific cypher part in order to get the timestamp of creation and receiving of the event. The elapsed time is calculated using these timestamps through cypher queries using:
$$
Elapsed = max(tc) - min(tp)
$$
where *tc* is the instant of insert in the cunsumer and *tp* the instant of insert in the producer.

### Kafka

Default kafka configuration, running on sigle instance on the same machine of the other components

- Kafka 2.11-0.10.1.1

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
streams.sink.topic.cypher.neo4j=WITH event.payload AS payload, event.meta AS meta CALL apoc.do.case( [\

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
python3 neo4j-streams-pt.py --start
```

The output is a windows that show you the distribution of the test. See result session to better understand the values. You can use the option `--plot-out file.png` to not show the result but save it on file. The details of the execution are dumped on standard output as CSV or you can redirect it on file using the option `--csv-out file.csv`

### baseline

This is the main command you have to test the system. By command line arguments you can specify the series to test. The arguments are the number of  node's unit to use. 

ES: 3 = 3 * 1000 if 1000 is the value configured as *nodes* in the *[unit]* part.

To get the same result of *start*:

```shell
python3 neo4j-streams-pt.py --baseline 1
```

The get more distribution (real case):

```shell
python3 neo4j-streams-pt.py --baseline 1 10 100 1000 --plot-out results.png
```

This means the tester execute 5 (if *repeat = 5*) times each series. A series is comped by 1k, 10k, 100k, 1000k nodes (if *nodes = 1000*)

### Results

There're lot of variables to tuning the whole process and lot of scenarios to optimize (such as high or low volumes of events per transaction). We introduce here two cases, both on a local machine, one with docker and one without. The computer is a MacBook Pro with

- CPU 2,2 GHz Intel Core i7
- 16 GB DDR3, 1600 MHz
- SSD

### With Docker

Here's the results with the setup as described above with

- Docker Community Edition 18.06.1-ce-mac73 
- docker-compose.yml

![Result on standalone mac](https://github.com/neo4j-contrib/neo4j-streams/blob/master/performance/docker.png)

| Nodes   | Executions | Min      | Max      | Avg      | Median   | St. Dev             |
| ------- | ---------- | -------- | -------- | -------- | -------- | ------------------- |
| 1000    | 5          | 0.325    | 2.351    | 0.8526   | 0.405    | 0.8568589732272167  |
| 10000   | 5          | 0.1352   | 0.2704   | 0.1824   | 0.1521   | 0.06006492320814203 |
| 100000  | 5          | 0.14022  | 0.25575  | 0.200146 | 0.21773  | 0.05124010665484605 |
| 1000000 | 5          | 0.162903 | 0.208513 | 0.185129 | 0.184043 | 0.01629424984465379 |

As you can see, there's not a single number to describe the scenario, but 0.20 ms per node (for the simple CREATE query) colud be a reference time.

### Without Docker

Here's the results with the setup as described above with

- neo4j producer, neo4j consumer, Kafka and Zookeeper run on the same machine, the tester too. The clock is the same, so it's synchronized.
- Kafka and Zookeeper run with default configuration

![Result on standalone mac](https://github.com/neo4j-contrib/neo4j-streams/blob/master/performance/local.png)

| Nodes   | Executions | Min      | Max      | Avg      | Median   | St. Dev               |
| ------- | ---------- | -------- | -------- | -------- | -------- | --------------------- |
| 1000    | 5          | 0.202    | 0.262    | 0.2274   | 0.227    | 0.025510782034269354  |
| 10000   | 5          | 0.1209   | 0.1821   | 0.14404  | 0.1332   | 0.026679261608972618  |
| 100000  | 5          | 0.10559  | 0.12636  | 0.115132 | 0.1099   | 0.01016512518368564   |
| 1000000 | 5          | 0.113919 | 0.128142 | 0.119806 | 0.118307 | 0.0054176914825412505 |

As you can see, there's not a single number to describe the scenario, but 0.13 ms per node (for the simple CREATE query) colud be a reference time.



