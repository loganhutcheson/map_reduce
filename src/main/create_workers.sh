#!/bin/bash
i=0
if [ -z $1 ]; then
	echo "Please provide number of workers argument!"
	exit 1
fi

while [ $i -lt $1 ]; do
	go run mrworker.go wc.so &
    i=$((i + 1))
done
