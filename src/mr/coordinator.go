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

const (
	UNASSIGNED int  = -1
	FINISHED 		= 0
	ASSIGNED		= 1
)

type Coordinator struct {
	jobs	map_jobs
	head 	*map_jobs
	mu		sync.Mutex
}

type map_jobs struct {
	next *map_jobs
	job_id	int
	length	int64
	offset	int
	name	string
	status int
	file_location string
}


//
// GetMJob
// assign a worker a map job.
//
//
func (c *Coordinator) GetMJob(arg *IntArg, reply *MapJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	job := c.head
	for (job != nil && job.status != UNASSIGNED) {
		job = job.next
	}
	if job == nil {
		fmt.Println("No Jobs to Assign")
		return nil
	} else {
		fmt.Println("Assigning Job")
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
	if job != nil {
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


	// Iterate over the map_jobs list and check status of jobs
	c.mu.Lock()
	defer c.mu.Unlock()
	jobs := c.head
	total := 0
	complete := 0
	if jobs != nil {
		total++
		if (jobs.status == FINISHED) {
			complete++
		} else {
			ret = false
		}
		jobs = jobs.next
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

		// Add file to map_jobs
		item := map_jobs{nil, i, fi.Size(), 0, s, UNASSIGNED, ""}
		if i == 0 {
			c.jobs = item
			c.head = &c.jobs
		} else {
			c.jobs.next = &item
			c.jobs = *c.jobs.next
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
