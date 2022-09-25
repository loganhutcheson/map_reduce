# map_reduce

This repository is a go implementation of Google's map reduce system.
Map reduce is a abstracted approach to carrying out distributed work loads over many computers.

Compile:
$ go build -race -buildmode=plugin ../mrapps/wc.go

Run Coordinator:
$ go run -race mrcoordinator.go pg-*.txt

Run Worker:
$ go run -race mrworker.go wc.so


My blog post on the is topic:
https://loganhutcheson.com/2022/09/22/map-reduce/


Credit for the project goes to:
MIT 6.824 Lab 1: Map Reduce
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html
