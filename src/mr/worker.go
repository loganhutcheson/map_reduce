package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// data structure to store key-value pairs for each R bucket
type BucketMap map[int][]KeyValue

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	status := UNASSIGNED
	keepRunning := true

	// Request job from the Coordinator
	for {
		reply := JobReply{}
		startTime := time.Now()
		retry_count := 0
		for {
			CallGetJob(&reply)
			if reply.JobType > 0 {
				// Got a MAP_TASK or REDUCE_TASK
				break
			} else {
				// Try for 30 seconds before exiting.
				if retry_count >= 30 {
					keepRunning = false
					break
				}
				retry_count++
			}
			elapsedTime := time.Since(startTime)
			if elapsedTime >= 5*time.Second {
				fmt.Println("WORKER: timeout. Exiting...")
				break
			}
			time.Sleep(1 * time.Second)
		}
		// Condition to stop the worker due to timeout exceeded
		if !keepRunning {
			break
		}

		// Map Job Assigned - Enter Map Routine
		if reply.JobType == MAP_TASK {

			// Open the input data file
			file, err := os.Open(reply.FileLocation)
			if err != nil {
				fmt.Println("Unable to open file:", err)
				return
			}
			defer file.Close()

			// Move to the desired offset - Not used with this implementation.
			_, err = file.Seek(reply.FileOffset, io.SeekStart)
			if err != nil {
				fmt.Println("Error seeking to the offset:", err)
				return
			}

			// Read the input data into buffer
			buffer := make([]byte, reply.DataLength)
			_, err = file.Read(buffer)
			if err != nil && err != io.EOF {
				fmt.Println("Error reading data from file:", err)
				return
			}

			// Map the input data
			keyvalue_array := mapf(file.Name(), string(buffer))

			// Randomize the mapped kvs into R buckets
			bucketMap := make(BucketMap, reply.NReduce)
			for _, kv := range keyvalue_array {
				rtask := ihash(kv.Key) % reply.NReduce
				bucketMap[rtask] = append(bucketMap[rtask], kv)
			}

			// Encode and store temp map data for each reduce bucket
			for rNum := 0; rNum < reply.NReduce; rNum++ {
				reqBodyBytes := new(bytes.Buffer)
				json.NewEncoder(reqBodyBytes).Encode(bucketMap[rNum])
				temp_filename := fmt.Sprintf("mr-%d-%d", reply.JobId, rNum)
				err = os.WriteFile(temp_filename, reqBodyBytes.Bytes(), 0644)
				if err != nil {
					status = UNASSIGNED
				} else {
					status = FINISHED
				}
			}
			// Notify coordinator status
			ret := CallNotifyDone(reply.JobId, status)
			fmt.Println("WORKER: ", reply.JobId, "Done with status: ", status,
				"Coordinator returned ", ret)
			continue // go get another job
		}

		// Reduce Job Assigned - Enter Reduce Routine
		if reply.JobType == REDUCE_TASK {

			files := reply.IntermediateFiles // Intermediate files
			var kv_array ByKey               // map to hold all aggregated temp data for R
			var prevKey string

			// Read M files for this R bucket:
			for _, file := range files {
				// Read the content of the file
				data, err := os.ReadFile(file)
				if err != nil {
					fmt.Println("Error reading file:", err)
					return
				}
				var tempKVArray ByKey
				// Decode the JSON data into a slice of KeyValue
				if err := json.Unmarshal(data, &tempKVArray); err != nil {
					fmt.Println("Error decoding JSON:", err)
					return
				}
				// Append all decoded key-values to the main slice
				kv_array = append(kv_array, tempKVArray...)
			}
			if kv_array.Len() == 0 {
				// No intermediate data, just return success
				// Notify coordinator status
				ret := CallNotifyDone(reply.JobId, FINISHED)
				fmt.Println("WORKER: ", reply.JobId, "Done with status: ", status,
					"Coordinator returned ", ret)
				continue // Go grab another job
			}
			// Sort the appended temp mapped kv pairs
			sort.Sort(kv_array)

			// Append all reduces kv to mr-out-X file
			final_filename := fmt.Sprintf("mr-out-%d", reply.JobId)
			temp_file, err := os.CreateTemp("", "temp-mr-out-")
			if err != nil {
				fmt.Printf("Error opening file: %s\n", err)
				return
			}
			defer temp_file.Close()

			// Iterate through all kv in the temp bucket, calling reduce when key changes
			var curValues []string
			prevKey = kv_array[0].Key
			for _, kv := range kv_array {

				// Check if we are still on the same key
				if kv.Key == prevKey {
					// Append the value to the temporary slice
					curValues = append(curValues, kv.Value)
				} else {
					// If the key changes, first check if tempValues is not empty to handle the first group
					if len(curValues) > 0 {
						//append values for each KEY for this R and call reduce
						reduced_value := reducef(prevKey, curValues)
						// Write data to file
						fmt.Fprintf(temp_file, "%v %v\n", prevKey, reduced_value)
					}
					// Reset the temporary variables for the next key
					prevKey = kv.Key
					curValues = []string{kv.Value} // Start new slice with the new key's first value
				}
			}

			// Flush the last key values as well
			if len(curValues) > 0 {
				reduced_value := reducef(prevKey, curValues)
				fmt.Fprintf(temp_file, "%v %v\n", prevKey, reduced_value)
			}

			// Mark reduce job as complete
			status = FINISHED
			// Notify coordinator status
			ret := CallNotifyDone(reply.JobId, status)
			if ret == -1 {
				// Go get another job, this one is already finished
				continue
			}
			// Rename the temporary file to the final filename atomically
			if err := os.Rename(temp_file.Name(), final_filename); err != nil {
				log.Fatal(err)
			}
			fmt.Println("WORKER: ", reply.JobId, "Done with status: ", status,
				"Coordinator returned ", ret)
			continue // go get another job
		} // end MAP or REDUCE task

	} // end worker for
}

// Ask the coordinator for a map job
func CallGetJob(reply *JobReply) {
	arg := IntArg{os.Getpid()}
	ok := call("Coordinator.AssignJob", &arg, &reply)

	if ok {
		if reply.JobId != 0 {
			fmt.Printf("WORKER: %v Assigned "+
				"File: %s, Filesize: %v Type: %d\n",
				reply.JobId, reply.FileLocation, reply.FileOffset, reply.JobType)
		}
	}
}

// notify the coordinator that the job is done
func CallNotifyDone(job_id int, status int) int {
	args := NotifyDoneArgs{}
	reply := IntReply{}

	args.JobId = job_id
	args.Status = status
	ok := call("Coordinator.WorkerDone", &args, &reply)
	if ok {
		return reply.Status
	} else {
		fmt.Println("call failed!")
	}
	return -1
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
