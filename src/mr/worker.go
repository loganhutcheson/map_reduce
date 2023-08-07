package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io"
import "os"
import "encoding/json"
import "bytes"
//import "sort"

//
// Map functions return a slice of KeyValue.
//
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


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	status := FINISHED

	// Try to get job from Coordinator
	reply := JobReply{}
	CallGetJob(&reply)

	if reply.JobId == -1 {
		fmt.Println("No jobs Assigned")
		status = UNASSIGNED;
		return
	}

	// Open the file
	file, err := os.Open(reply.FileLocation)
	if err != nil {
		fmt.Println("Unable to open file:", err)
		return
	}
	defer file.Close()
	
	// Map Job Retreived - Enter Map Routine
	if reply.JobType == MAP_TASK {

		// Move to the desired offset
		_, err = file.Seek(reply.FileOffset, io.SeekStart)
		if err != nil {
			fmt.Println("Error seeking to the offset:", err)
			return
		}
		
		// Read the data from the offset
		buffer := make([]byte, reply.DataLength)
		_, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			fmt.Println("Error reading data from file:", err)
			return
		}

		// Map
		keyvalue_array := mapf("", string(buffer))
		bucketMap := make(BucketMap, reply.NReduce)

		for _, kv := range keyvalue_array {
			rtask := ihash(kv.Key) % reply.NReduce
			// Append the key-value pair to the corresponding bucket in the BucketMap
			bucketMap[rtask] = append(bucketMap[rtask], kv)
		}

		for i := 0; i < reply.NReduce; i++ {
			// Encode and store map data
			reqBodyBytes := new (bytes.Buffer)
			json.NewEncoder(reqBodyBytes).Encode(bucketMap[i])
			temp_filename := fmt.Sprintf("%s_temp_%d_%d", reply.FileLocation, i, reply.JobId)
			err = os.WriteFile(temp_filename, reqBodyBytes.Bytes(), 0644)
			if err != nil {
				status = UNASSIGNED
			}
		}
	}

	// Reduce Job Retreived - Enter Reduce Routine
    if reply.JobType == REDUCE_TASK {

		// TODO Read M files for this R
		// open file temp_R_(*M)_out.txt into buff

		// TODO sort
		//sort.Sort(ByKey(keyvalue_array))

		// TODO append values for each KEY for this R and call map

	}

	// Notify coordinator status
	ok := CallNotifyDone(reply.JobId, status)
	fmt.Println("JobId: ", reply.JobId, " Finished with status: ", status)
	if ok != 0 {
		log.Fatal(err)
	}

}

// Ask the coordinator for a map job
func CallGetJob(reply *JobReply)  {
	arg := IntArg{ os.Getpid() }
	ok := call("Coordinator.GetJob", &arg, &reply)

	if ok {
		fmt.Printf("Assigned:\n"+
		"JobId: %v File: %s, Filesize: %v\n",
		reply.JobId, reply.FileLocation, reply.FileOffset)
	} else {
		fmt.Println("Job request call failed!\n")
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
		fmt.Printf("call failed!\n")
	}
	return -1
}



//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
