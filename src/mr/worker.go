package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "encoding/json"
import "bytes"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	content, err := ioutil.ReadFile(reply.File)
	fmt.Println(reply.File)
	if err != nil {
		fmt.Println("Unable to open file: ", reply.File)
		status = UNASSIGNED
		return
	}

	
	// Map Job Retreived - Enter Map Routine
	if reply.JobType == MAP_TYPE {

		keyvalue_array := mapf(reply.File, string(content))

		// Encode and store map data
		reqBodyBytes := new (bytes.Buffer)
		json.NewEncoder(reqBodyBytes).Encode(keyvalue_array)
		temp_filename := fmt.Sprintf("%s_temp", reply.File)
		err = os.WriteFile(temp_filename, reqBodyBytes.Bytes(), 0644)
		if err != nil {
			status = UNASSIGNED
		}
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
	arg := IntArg{}
	ok := call("Coordinator.GetJob", &arg, &reply)

	if ok {
		fmt.Printf("Assigned:\n"+
		"JobId: %v File: %s, Filesize: %v\n",
		reply.JobId, reply.File, reply.Length)
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
