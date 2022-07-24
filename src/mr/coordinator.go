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
	job_id	int				// incremented from 1
	length	int64			// total length of file
	offset	int				// offset in file
	name	string			// file name
	status int 				// -1 = failed or unassigned, 0 = assigned, 1 = completed
	file_location string	// not set until Job done
}

// global head variable of the map_jobs
var head *map_jobs


//
// assign a worker a map job.
//
//
func (c *Coordinator) GetMJob(arg *IntArg, reply *MapJobReply) error {

	fmt.Println(" received MJOB request")
	if c.jobs == (map_jobs{}) {
		fmt.Println(" no more map jobs to give")
		return nil
	}
	reply.JobId = c.jobs.job_id
	reply.Index = c.jobs.offset
	reply.File = c.jobs.name
	reply.Length = c.jobs.length
	c.jobs = *c.jobs.next
	return nil
}

//
// get the confirmation from the Worker that a job is done
//
//
func (c *Coordinator) WorkerDone(args *NotifyDoneArgs, reply *IntReply) error {


	fmt.Printf("\nWorker is done %v", args.JobId)

	// Iterate over the map_jobs to find matching job and store its location
	jobs := head
	if jobs.next != nil {
		if (jobs.job_id == args.JobId) {
			jobs.status = args.Status
			jobs.file_location = args.Location
			return nil
		}
		jobs = jobs.next
	}
	// Notify worker we got its data, it can die
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
		// Store the map_jobs in the coordinator class
		item := map_jobs{nil, i, fi.Size(), 0, s, -1, ""}
		c.jobs.next = &item
		if i == 0 {
			head = &c.jobs
		}
		split_size := int64(10000)		
		num_parts := item.length / split_size
		fmt.Println(num_parts)

		
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
