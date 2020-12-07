#!/bin/bash

if [ ! -f "$1" ]; then
	`touch "$1"`;
else
	echo "S1 exists already"
	exit -1
	#`rm "$1"`;
	`touch "$1"`;
fi

#for((i=1;i<=72;i++)); do
for i in 1 8 16 24 32 40 48 56 64 72; do
	for j in 1 2 3 4 5; do
		if [ $i -gt 36 ]; then
			`./intset-test -i 10000 -r 20000 -u 100 -n "$i"  >> "$1"`;
		else
			`sudo numactl --cpunodebind 0 ./intset-test -i 10000 -r 20000 -u 100 -n "$i"  >> "$1"`;
		fi
	done
done
