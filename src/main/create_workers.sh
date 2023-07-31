#!/bin/bash
i=0
while [ $i -lt $1 ]; do
	go run -race mrworker.go wc.so &
    i=$((i + 1))
done
