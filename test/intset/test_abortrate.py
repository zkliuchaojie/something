#!/usr/bin/python
import sys

file = open(sys.argv[1])

line = file.readline()
line = line.strip()

for i in [1, 8, 16, 24, 32, 40, 48, 56, 64, 72]:
	#print ("threadnum: %d" % i)
	abort_num = 0
	throughput = 0
	txs_num = 0
	for j in range(5):
		for k in range(i):
			while(line.find("#abort      :") == -1):
				line = file.readline()
				line = line.strip()
			abort_num += float(line[line.find(":")+2:len(line)])
			line = file.readline()
			line = line.strip()
		
		while(line.find("#txs          :") == -1):
			line = file.readline()
			line = line.strip()
		txs_num += float(line[line.find(":")+2:line.find("(")-1])
		line = file.readline()
		line = line.strip()

	#print ("throughput: %f, clock overhead: %f%%"%(throughput_avg/5, clock_overhead_avg/i/5/10*100))
	#print ("%f"%(throughput/5/1000000))
        if (abort_num == 0):
            print (0)
        else:
	    print ("%f"%(abort_num/txs_num*100))
