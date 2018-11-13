import argparse
import configparser
import uuid
import time
import csv
import sys
import statistics
import matplotlib.pyplot as plt

from py2neo import Graph

parser = argparse.ArgumentParser(description='Performance Test suite for neo4j-streams')

parser.add_argument('--plot-out', help='output image of plot file', dest='plotout')
parser.add_argument('--csv-out', help='output csv results file', dest='csvout')
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

def writeCSV(writer, repeats, nodeslist, series):
	csvHeader = ["Nodes","avg","min","max","median","stdev"]+list(range(int(repeats)))
	writer.writerow(csvHeader)
	node_index = 0
	for ser in series:		
		avg = statistics.mean(ser)
		mini = min(ser)
		maxi = max(ser)
		medi = statistics.median(ser)
		stdev = statistics.stdev(ser)
		
		csvRow = [nodeslist[node_index],avg,mini,maxi,medi,stdev]+ser	
		writer.writerow(csvRow)
		node_index += 1

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

	if args.csvout is not None:
		with open(args.csvout, 'w') as filecsv:
			writer = csv.writer(filecsv)
			writeCSV(writer, repeats, [nodes], [series])
	else:
		writer = csv.writer(sys.stdout)
		writeCSV(writer, repeats, [nodes], [series])

	if args.plotout is not None:
		fig1.savefig(args.plotout)
	else:
		plt.show()		


if args.baseline is not None:
	pows = args.baseline
	nodes = int(unit['nodes'])
	repeats = int(unit['repeat'])

	all_series = []
	nodes_list = []
	for p in pows:
		power = int(p)
		print("Execution of ",power)
		currentNodes = nodes * power
		nodes_list.append(currentNodes)
		series = [executeTest(currentNodes) for i in range(int(repeats))]
		all_series.append(series)

		#series = [executeTest(nodes) for i in range(int(repeats))]

	fig1, ax1 = plt.subplots()
	ax1.set_title("Baselines (repeats for %d times)"%repeats)
	ax1.set_ylabel("ms per node")
	ax1.set_xlabel("nodes x %d"%nodes)

	ax1.boxplot(all_series)	
	plt.xticks(range(1,len(all_series)+1), args.baseline)

	if args.csvout is not None:
		with open(args.csvout, 'w') as filecsv:
			writer = csv.writer(filecsv)
			writeCSV(writer, repeats, nodes_list, all_series)
	else:
		writer = csv.writer(sys.stdout)
		writeCSV(writer, repeats, nodes_list, all_series)
	
	if args.plotout is not None:
		fig1.savefig(args.plotout)
	else:
		plt.show()	