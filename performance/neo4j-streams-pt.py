import argparse
import configparser
import uuid
import time
import matplotlib.pyplot as plt

from py2neo import Graph

parser = argparse.ArgumentParser(description='Performance Test suite for neo4j-streams')

parser.add_argument('--out', help='output file', dest='out')
parser.add_argument('--start', nargs='*', metavar=('msg','repeats'), help='print the reference time (avg repeats * msg), in order to find the [unit] values')
parser.add_argument('--baseline', nargs='+', metavar='pow', help='runs the [unit] * pow')

args = parser.parse_args()

config = configparser.ConfigParser()

config.read('config.ini')

producerConfig = config['producer']
consumerConfig = config['consumer']
unit = config['unit']

producer = Graph(producerConfig['url'], user=producerConfig['username'], password=producerConfig['password'])
consumer = Graph(consumerConfig['url'], user=consumerConfig['username'], password=consumerConfig['password'])

def executeTest(nodes):
	"""Returns elapsed per node """
	test_ref = str(uuid.uuid1())

	result = producer.run("""
		UNWIND range(1,{msg}) as ran
		CREATE (n:Performance)
		SET n.group = {uuid}, n.creation_time = apoc.date.currentTimestamp()
		RETURN min(n.creation_time) as creation_time
		""", msg = nodes, uuid=test_ref).data()

	creation_time = result[0]['creation_time']
	#print(test_ref,creation_time)
	received_time = creation_time

	in_streaming = True
	while (in_streaming):			
		time.sleep(1)
		
		result = consumer.run("""
			MATCH (n:Performance)
			WHERE n.group = {uuid}
			RETURN count(n) as cnt, max(n.received_time) as received_time
			""", uuid=test_ref).data()

		#print(result)
		in_streaming = result[0]['cnt'] < nodes
		received_time = result[0]['received_time']

	return (received_time - creation_time) / nodes

if args.start is not None:
	msg = unit['nodes'] 
	repeats = unit['repeat'] 
	if len(args.start) == 1:
		[msg, *_] = args.start
	if len(args.start) >= 2:
		[msg, repeats, *_] = args.start	

	nodes = int(msg)

	series = [executeTest(nodes) for i in range(int(repeats))]
	fig1, ax1 = plt.subplots()
	ax1.set_title("Baselines (repeats for %d times)"%int(repeats))
	ax1.set_ylabel('ms per node')
	ax1.set_xlabel("nodes x %d"%nodes)
	ax1.boxplot(series)	
	
	if args.out is not None:
		fig1.savefig(args.out)
	else:
		plt.show()	

if args.baseline is not None:
	pows = args.baseline
	nodes = int(unit['nodes'])
	repeats = int(unit['repeat'])

	all_series = []
	for p in pows:
		power = int(p)
		print("Execution of ",power)
		currentNodes = nodes * power
		series = [executeTest(currentNodes) for i in range(int(repeats))]
		all_series.append(series)

		series = [executeTest(nodes) for i in range(int(repeats))]

	fig1, ax1 = plt.subplots()
	ax1.set_title("Baselines (repeats for %d times)"%repeats)
	ax1.set_ylabel("ms per node")
	ax1.set_xlabel("nodes x %d"%nodes)

	ax1.boxplot(all_series)	
	plt.xticks(range(1,len(all_series)+1), args.baseline)
	
	if args.out is not None:
		fig1.savefig(args.out)
	else:
		plt.show()	