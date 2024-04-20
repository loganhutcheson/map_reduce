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
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

//import "sort"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Define a data structure to store key-value pairs for each bucket
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
	// Get job from Coordinator
	for {
		reply := JobReply{}
		startTime := time.Now()
		retry_count := 0
		for {
			CallGetJob(&reply)
			if reply.JobId != -1 {
				break
			} else {
				// Try to get job for 5 seconds before exiting
				if retry_count >= 5 {
					keepRunning = false
					break
				}
				retry_count++
			}
			elapsedTime := time.Since(startTime)
			if elapsedTime >= 5*time.Second {
				fmt.Println("Worker timeout...")
				break
			}
			time.Sleep(1 * time.Second)
		}

		// Map Job Retreived - Enter Map Routine
		if reply.JobType == MAP_TASK {
			// Open the file
			file, err := os.Open(reply.FileLocation)
			if err != nil {
				fmt.Println("Unable to open file:", err)
				return
			}
			defer file.Close()
			// Move to the desired offset
			_, err = file.Seek(reply.FileOffset, io.SeekStart)
			if err != nil {
				fmt.Println("Error seeking to the offset:", err)
				return
			}

			// Read the data from the offset
			buffer := make([]byte, reply.DataLength)
			_, err = file.Read(buffer)
			if err != nil && err != io.EOF {
				fmt.Println("Error reading data from file:", err)
				return
			}

			// Map function
			keyvalue_array := mapf("", string(buffer))
			bucketMap := make(BucketMap, reply.NReduce)

			for _, kv := range keyvalue_array {
				rtask := ihash(kv.Key) % reply.NReduce
				// Append the key-value pair to the corresponding bucket in the BucketMap
				bucketMap[rtask] = append(bucketMap[rtask], kv)
			}

			for i := 0; i < reply.NReduce; i++ {
				// Encode and store map data
				reqBodyBytes := new(bytes.Buffer)
				json.NewEncoder(reqBodyBytes).Encode(bucketMap[i])
				temp_filename := fmt.Sprintf("%s_temp_bucket%d_%d", reply.FileLocation, i, reply.JobId)
				err = os.WriteFile(temp_filename, reqBodyBytes.Bytes(), 0644)
				if err != nil {
					status = UNASSIGNED
				} else {
					status = FINISHED
				}
			}
			// Notify coordinator status
			CallNotifyDone(reply.JobId, status)
			fmt.Println("JobId: ", reply.JobId, " Finished with status: ", status)

		}

		// Reduce Job Retreived - Enter Reduce Routine
		if reply.JobType == REDUCE_TASK {
			// Define a map to hold all aggregated data
			var kv_array ByKey

			rnum := reply.NReduce

			// Read M files for this R
			cwd, err := os.Getwd()
			if err != nil {
				fmt.Println("Error getting current directory:", err)
				return
			}
			dir := cwd

			// Create a regular expression to match files with "_bucketR_" where R is an integer
			regexPattern := fmt.Sprintf("_bucket%d_", rnum)
			regex, err := regexp.Compile(regexp.QuoteMeta(regexPattern))
			if err != nil {
				fmt.Println("Error compiling regex:", err)
				return
			}
			// Walk through the directory
			err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				// Check if the file name matches the pattern
				if regex.MatchString(info.Name()) {
					// Read the content of the file
					data, err := os.ReadFile(path)
					if err != nil {
						fmt.Println("Error reading file:", err)
						return err
					}
					var tempKVArray ByKey
					// Decode the JSON data into a slice of KeyValue
					if err := json.Unmarshal(data, &tempKVArray); err != nil {
						fmt.Println("Error decoding JSON:", err)
						return err
					}
					// Append all decoded key-values to the main slice
					kv_array = append(kv_array, tempKVArray...)

				}

				return nil
			})

			if err != nil {
				fmt.Println("Error walking through directory:", err)
			}

			sort.Sort(kv_array)
			// Temporary variable to store the current group of values
			var tempValues []string
			// Track the current key we are gathering values for
			prevKey := kv_array[0].Key
			for _, kv := range kv_array {
				if prevKey == "zip" {
					fmt.Println("LOGAN DEBUG - WE FOUND ZIP")
				}
				// Check if we are still on the same key
				if kv.Key == prevKey {
					// Append the value to the temporary slice
					tempValues = append(tempValues, kv.Value)
					if prevKey == "zip" {
						fmt.Println("LOGAN DEBUG - WE FOUND ZIP")
					}
				} else {
					if prevKey == "zip" {
						fmt.Println("LOGAN DEBUG - WE WRITING ZIP of length", len(tempValues))
					}
					// If the key changes, first check if tempValues is not empty to handle the first group
					if len(tempValues) > 0 {
						//append values for each KEY for this R and call reduce
						reduced_value := reducef(prevKey, tempValues)
						temp_filename := fmt.Sprintf("mr-out-%d", rnum)
						f, err := os.OpenFile(temp_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
						if err != nil {
							fmt.Printf("Error opening file: %s\n", err)
							return
						}
						// Write data to file
						fmt.Fprintf(f, "%v %v\n", prevKey, reduced_value)

						status = FINISHED
					}
					// Reset the temporary variables for the next key
					prevKey = kv.Key
					tempValues = []string{kv.Value} // Start new slice with the new key's first value
				}
			}

			// Flush the end as well
			// If the key changes, first check if tempValues is not empty to handle the first group
			if len(tempValues) > 0 {
				//append values for each KEY for this R and call reduce
				reduced_value := reducef(prevKey, tempValues)
				temp_filename := fmt.Sprintf("mr-out-%d", rnum)
				f, err := os.OpenFile(temp_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fmt.Printf("Error opening file: %s\n", err)
					return
				}
				// Write data to file
				fmt.Fprintf(f, "%v %v\n", prevKey, reduced_value)

				status = FINISHED
			}

			// Notify coordinator status
			CallNotifyDone(rnum, status)
			fmt.Println("Rnum: ", rnum, " Finished with status: ", status)

		} // endif MAP or REDUCE TASK

		// Condition to stop the worker
		if !keepRunning {
			break
		}
	}

}

// Ask the coordinator for a map job
func CallGetJob(reply *JobReply) {
	arg := IntArg{os.Getpid()}
	ok := call("Coordinator.AssignJob", &arg, &reply)

	if ok {
		fmt.Printf("WORKER: Assigned:\n"+
			"JobId: %v File: %s, Filesize: %v Type: %d\n",
			reply.JobId, reply.FileLocation, reply.FileOffset, reply.JobType)
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
