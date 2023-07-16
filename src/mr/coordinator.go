package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

import (
	"fmt"
	"time"
	"sync"
)


var debug = true

type Coordinator struct {
	job		map_job
	head 	*map_job
	mu		sync.Mutex
}

type map_job struct {
	next *map_job
	job_id	int
	length	int64
	offset	int
	name	string
	status int
	file_location string
}


//
// GetJob Routine
// assigns a map or reduce job to worker
//
//
func (c *Coordinator) GetJob(arg *IntArg, reply *JobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	job := c.head
	for (job != nil && job.status != UNASSIGNED) {
		job = job.next
	}
	if job == nil {
		fmt.Println("No Jobs to Assign")
		reply.JobId = -1
		return nil
	} else {
		fmt.Println("Assigning Job", job.name)
		reply.JobId = job.job_id
		reply.Index = job.offset
		reply.File = job.name
		reply.Length = job.length
		job.status = ASSIGNED
	}
	return nil
}

//
// WorkerDone
// worker end condition
// process the mapped information from worker.
//
//
func (c *Coordinator) WorkerDone(args *NotifyDoneArgs, reply *IntReply) error {
	// Find matching job and store location
	c.mu.Lock()
	defer c.mu.Unlock()
	job := c.head
	for {
		if job == nil {
			break
		}
		if (job.job_id == args.JobId) {
			job.status = args.Status
			job.file_location = args.Location
			return nil
		}
		job = job.next
	}

	// Notify worker it can die
	reply.Status = 0;
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
// main/mrcoordiator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true
	/* done_time - the period to wait between Done() */
	time.Sleep(5 * time.Second)


	// Iterate over the map_job list and check status of job
	c.mu.Lock()
	defer c.mu.Unlock()
	job := c.head
	total := 0
	complete := 0

	for {

		if job == nil {
			break
		}

		total++
		if (job.status == FINISHED) {
			complete++
		} else {
			ret = false
		}
		job = job.next

	}
	if debug {
		fmt.Printf("Master Stats: \n" +
			"	Total Jobs: %d \n" +
			"	Complete Jobs: %d\n", total, complete)
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	var iterator *map_job

	// Iterate over files
	for i, s := range files {
		fmt.Print("Reading file ",s, "\n")

		f, err := os.Open(s)
		if err != nil {
			fmt.Printf("Could not obtain file\n")
		}

		fi, err := f.Stat()
		if err != nil {
			fmt.Printf("Could not obtain stat\n")
		}

		// Add file to map_job
		item := map_job{nil, i, fi.Size(), 0, s, UNASSIGNED, ""}
		if i == 0 {
			c.job = item
			c.head = &c.job
			iterator = c.head
		} else {
			iterator.next = &item
			iterator = iterator.next
		}

		// TODO: File segmentation
		//split_size := int64(10000)
		//num_parts := item.length / split_size
		//fmt.Println(num_parts)
	}


	fmt.Println("Master is ready.\n")

	// Thread that listens for Jobs
	c.server()

	// Done calls periodically
	for {
		c.Done()
	}

	return &c
}
