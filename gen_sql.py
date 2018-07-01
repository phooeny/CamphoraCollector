#!/bin/python

import sys

id_list=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,35,36,37,14,23,32,29,44,47,48];

if __name__ == "__main__":
	file_name = sys.argv[1];
	with open(file_name,'r') as fd:
		for line in fd:
			line = line.strip();
			arr = line.split('\t');
			arr = [arr[i] for i in id_list];
			#arr[21]=arr[21][0:-1];
			arr[22]="'%s'"%(arr[22]);
			arr[23]="'%s'"%(arr[23]);
			print("(%s),"%(','.join(arr)));
