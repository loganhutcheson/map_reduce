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

	status := 0

	// Get a job from Coordinator
	job_id, filename := CallGetMJob()

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		status = -1
	}

	// Pass to Map function
	keyvalue_array :=	mapf(filename, string(content))

	// Encode the KV data
	reqBodyBytes := new (bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(keyvalue_array)

	// Store intermediate file
	temp_filename := fmt.Sprintf("%s_temp", filename)
	err = os.WriteFile(temp_filename, reqBodyBytes.Bytes(), 0644)
	if err != nil {
		status = -1
	}

	// Notify coordinator job is done
	ok := CallNotifyDone(job_id, status)
	if ok != 0 {
		log.Fatal(err)
	}

}

// Ask the coordinator for a map job
func CallGetMJob() (int, string) {
	arg := IntArg{}
	reply := MapJobReply{}
	ok := call("Coordinator.GetMJob", &arg, &reply)

	if ok {
		fmt.Printf("This worker is assigned\n "+
		"JobId: %v File: %s, Filesize: %v, Index %v ",
		reply.JobId, reply.File, reply.Length, reply.Index)

		return reply.JobId, reply.File

	} else {
		fmt.Println("Worker MJOB call not OK")
		fmt.Printf("call failed!\n")
	}
	return -1, ""
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
