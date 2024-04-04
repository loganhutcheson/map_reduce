# map_reduce
Go implementation of Map Reduce<br>

< === How to Run === ><br>
$ cd src/main/

--  Compile: (example with word count plugin)<br>
$ go build -buildmode=plugin ../mrapps/wc.go

-- Run Coordinator<br>
$ go run mrcoordinator.go pg-*.txt

-- Run Worker:<br>
$ go run mrworker.go wc.so


< === Credit === >
<br>
Credit for the project goes to:<br>
MIT 6.824 Lab 1: Map Reduce<br>
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html
