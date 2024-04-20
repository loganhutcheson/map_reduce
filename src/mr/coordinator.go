package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//var debug = true

type Coordinator struct {
	mJob      map_job
	mHead     *map_job
	rJob      reduce_job
	rHead     *reduce_job
	mu        sync.Mutex
	numReduce int
	mapDone   int
}

type map_job struct {
	next          *map_job
	job_id        int
	job_type      int
	status        int
	file_location string
	file_index    int64
	m_size        int64
}

type reduce_job struct {
	next   *reduce_job
	job_id int
	status int
	rNum   int
}

// AssignJob Routine
// assigns a map or reduce job to worker
func (c *Coordinator) AssignJob(proc_id *IntArg, reply *JobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapDone == 0 {
		job := c.mHead
		for job != nil && job.status != UNASSIGNED {
			job = job.next
		}
		if job == nil {
			reply.JobId = -1
			reply.JobType = UNKNOWN_TASK
			return nil
		} else {
			reply.JobId = job.job_id
			reply.FileLocation = job.file_location
			reply.FileOffset = job.file_index
			reply.DataLength = job.m_size
			reply.NReduce = c.numReduce
			job.status = ASSIGNED
			reply.JobType = MAP_TASK
		}
	} else {
		job := c.rHead
		for job != nil && job.status != UNASSIGNED {
			job = job.next
		}
		if job == nil {
			reply.JobId = -1
			reply.JobType = UNKNOWN_TASK
			return nil
		} else {
			reply.JobId = job.job_id
			reply.NReduce = job.rNum
			job.status = ASSIGNED
			reply.JobType = REDUCE_TASK
		}
	}

	return nil
}

// WorkerDone - end condition
// Processes the reply from worker
func (c *Coordinator) WorkerDone(args *NotifyDoneArgs, reply *IntReply) error {
	// Find matching job and store location
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapDone == 0 {
		job := c.mHead
		for {
			if job == nil {
				break
			}
			if job.job_id == args.JobId {
				job.status = args.Status
				job.file_location = args.Location
				return nil
			}
			job = job.next
		}
	} else {
		job := c.rHead
		for {
			if job == nil {
				break
			}
			if job.rNum == args.JobId {
				job.status = args.Status
				return nil
			}
			job = job.next
		}
	}

	// Notify worker it can die
	reply.Status = 0
	return nil

}

// Start a thread that listens for RPCs from worker.go
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

// main/mrcoordiator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true
	/* done_time - the period to wait between Done() */
	time.Sleep(5 * time.Second)

	// Iterate over the mr_job list and check status of job
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapDone == 0 {
		job := c.mHead
		total := 0
		complete := 0

		for {
			if job == nil {
				break
			}
			total++
			if job.status == FINISHED {
				complete++
			} else {
				ret = false
			}
			job = job.next

		}

		fmt.Printf("Master Stats: \n"+
			"  Total M Jobs: %d \n"+
			"  Complete M Jobs: %d\n", total, complete)

	} else {
		job := c.rHead
		total := 0
		complete := 0

		for {
			if job == nil {
				break
			}
			total++
			if job.status == FINISHED {
				complete++
			} else {
				ret = false
			}
			job = job.next

		}

		fmt.Printf("Master Stats: \n"+
			"  Total R Jobs: %d \n"+
			"  Complete R Jobs: %d\n", total, complete)

	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}
	c.numReduce = nReduce

	var map_list *map_job

	// Default values
	m_size := int64(64 * 1024 * 1024) // 64MB
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
		// Loop through the file in m_size segments
		for offset < fileSize {

			if fileSize-offset < chunkSize {
				chunkSize = fileSize - offset
			}

			// add map job to linked list
			map_job := map_job{nil, job_id, MAP_TASK, UNASSIGNED,
				file, offset, m_size}
			if c.mJob.job_id == 0 {
				c.mJob = map_job
				c.mHead = &c.mJob
				map_list = c.mHead
			} else {
				map_list.next = &map_job
				map_list = map_list.next
			}

			offset += chunkSize
			job_id = job_id + 1
		}
	}
	fmt.Println("Coordinator is ready to assign Map jobs.")

	// Thread that listens for Jobs
	c.server()

	// Done calls periodically
	for {
		if c.Done() {
			break
		}
	}
	fmt.Println("Map jobs are complete. Creating reduce jobs...")
	c.mapDone = 1

	// Create the reduce jobs
	var reduce_list *reduce_job
	for i := 0; i < nReduce; i++ {
		reduce_job := reduce_job{nil, job_id, UNASSIGNED, i}
		// Add reduce job to linked list
		if c.rJob.job_id == 0 {
			c.rJob = reduce_job
			c.rHead = &c.rJob
			reduce_list = c.rHead
		} else {
			reduce_list.next = &reduce_job
			reduce_list = reduce_list.next
		}
	}
	// Done calls periodically
	for {
		if c.Done() {
			break
		}
	}
	fmt.Println("Reduce jobs are complete. Exiting...")

	return &c
}
