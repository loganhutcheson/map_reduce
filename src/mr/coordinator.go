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
	job		mr_job
	head 	*mr_job
	mu		sync.Mutex
	numReduce int
}

type mr_job struct {
	next *mr_job
	job_id  int
	job_type int
	status int
	file_location string
	file_index int64
	m_size int64
}

//
// GetJob Routine
// assigns a map or reduce job to worker
//
//
func (c *Coordinator) GetJob(proc_id *IntArg, reply *JobReply) error {
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
		reply.JobId = job.job_id
		reply.FileLocation = job.file_location
		reply.FileOffset = job.file_index
		reply.DataLength = job.m_size
		reply.NReduce = c.numReduce
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


	// Iterate over the mr_job list and check status of job
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
	c.numReduce = nReduce

	var iterator *mr_job

	/* Defaults */
	m_size := int64(64 * 1024 * 1024) // 64MB
//	m_counter := 1
	job_id := 1000


	// Split input data into "M" pieces of m_size
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			fmt.Printf("Cannot access file: %s, error: %v\n", file, err)
			return &c
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			fmt.Printf("Cannot access file: %s, error: %v\n", file, err)
            return &c
		}

		fileSize := fi.Size()		
		var offset int64 = 0
		var chunkSize int64 = m_size
		// Loop through the file in 64KB increments
		for offset < fileSize {

			if fileSize-offset < chunkSize {
				chunkSize = fileSize - offset
			}

			// add map job to linked list
			item := mr_job{nil, job_id, MAP_TASK, UNASSIGNED,
						file, offset, m_size}
			if c.job.job_id == 0 {
				c.job = item
				c.head = &c.job
				iterator = c.head
			} else {
				iterator.next = &item
				iterator = iterator.next
			}

            offset += chunkSize
            job_id = job_id + 1
		}
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
