package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

import "fmt"
import "time"

type Coordinator struct {
	jobs	map_jobs
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

var head *map_jobs


//
// assign a worker a map job.
//
//
func (c *Coordinator) GetMJob(arg *IntArg, reply *MapJobReply) error {

	fmt.Println(" received MJOB request")
	job := *head
	for (job.next != nil && job.status != 0) {
		fmt.Println("Job name %s", job.name)
		job = *job.next
	}
	if job == (map_jobs{}) {
		fmt.Println(" all jobs assigned")
		return nil
	}
	reply.JobId = job.job_id
	reply.Index = job.offset
	reply.File = job.name
	reply.Length = job.length
	return nil
}

//
// get the mapped information from worker.
//
//
func (c *Coordinator) WorkerDone(args *NotifyDoneArgs, reply *IntReply) error {

	// Find matching job and store location
	jobs := head
	if jobs.next != nil {
		if (jobs.job_id == args.JobId) {
			jobs.status = args.Status
			jobs.file_location = args.Location
			return nil
		}
		jobs = jobs.next
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
	ret := false
	/* done_time - the period to wait between Done() */
	time.Sleep(5 * time.Second)


	// TODO - Iterate over the map_jobs list and check status of jobs

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
		fmt.Print("Index ",i," ", s)

		f, err := os.Open(s)
		if err != nil {
			fmt.Printf("Could not obtain file\n")
		}

		fi, err := f.Stat()
		if err != nil {
			fmt.Printf("Could not obtain stat\n")
		}

		// Add file to map_jobs
		item := map_jobs{nil, i, fi.Size(), 0, s, -1, ""}
		if i == 0 {
			c.jobs = item
			head = &c.jobs
		} else {
			c.jobs.next = &item
			c.jobs = *c.jobs.next
		}

		// TODO: File segmentation
		//split_size := int64(10000)
		//num_parts := item.length / split_size
		//fmt.Println(num_parts)
	}


	fmt.Println("Ready to assign map jobs.")

	// Thread that listens for Jobs
	c.server()

	// Done calls periodically
	for {
		c.Done()
	}

	return &c
}
