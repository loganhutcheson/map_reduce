package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

import "fmt"


type Coordinator struct {
	// Your definitions here.
	items	input_data
}

type input_data struct {
	length	int64
	offset	int
	name	string

}



// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}



//
// assign a worker a map job.
//
//
func (c *Coordinator) GetMJob(args *ExampleArgs, reply *MapJobReply) error {
	reply.Index = c.items.offset
	reply.File = c.items.name
	reply.Length = c.items.Length
	return nil
}




//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordina	tor.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// Lets start by printing the file names.
	for i, s := range files {
		fmt.Print("Index ",i," ", s)

		// Now lets get the file size to split into 64KB chunks
		f, err := os.Open(s)
		if err != nil {
  			// Could not obtain file, handle error
		}

		fi, err := f.Stat()
		if err != nil {
  			// Could not obtain stat, handle error
		}

		fmt.Printf(" %d bytes \n", fi.Size())
		// Store the input_data in the coordinator class
		item := input_data{fi.Size(), 0, s}
		c.items = item
		split_size := int64(10000)		
		num_parts := item.length / split_size
		fmt.Println(num_parts)

		
	}




	fmt.Println("Ready to assign map jobs.")
	c.server()
	return &c
}
