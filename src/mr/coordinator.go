package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"sync"
	"time"
)

type Coordinator struct {
	mapJobs    []map_job    // Slice of map jobs
	reduceJobs []reduce_job // Slice of reduce jobs
	numReduce  int          // Total number of reduce jobs
	mapDone    int          // Count of completed map jobs
	mu         sync.Mutex   // mutex to lock mapJobs and reduceJobs
}

type map_job struct {
	job_id        int
	job_type      int
	status        int
	file_location string
	file_index    int64
	m_size        int64
}

type reduce_job struct {
	job_id            int
	status            int
	intermediateFiles []string
}

func GetIntermediateFiles(rNum int) []string {
	var matchedFiles []string
	pattern := fmt.Sprintf(`^mr-(\d+)-%d$`, rNum)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		fmt.Println("Regex error:", err)
		return nil
	}

	// Get the current working directory
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current working directory:", err)
		return nil
	}

	// Read all files in the current directory
	files, err := os.ReadDir(cwd)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return nil
	}

	for _, file := range files {
		if file.IsDir() {
			continue // skip directories
		}
		fileName := file.Name()
		if regex.MatchString(fileName) {
			matchedFiles = append(matchedFiles, fileName)
		}
	}
	fmt.Println("Matched Files:", matchedFiles)
	return matchedFiles
}

// AssignJob Routine
// assigns a map or reduce job to worker
func (c *Coordinator) AssignJob(proc_id *IntArg, reply *JobReply) error {

	// RPC lock
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapDone == 0 {
		// Assign worker a map job
		for i, job := range c.mapJobs {
			if job.status == UNASSIGNED {
				reply.JobId = job.job_id
				reply.JobType = MAP_TASK
				reply.FileLocation = job.file_location
				reply.FileOffset = job.file_index
				reply.DataLength = job.m_size
				reply.NReduce = c.numReduce
				// Mark this job as assigned
				c.mapJobs[i].status = ASSIGNED
				return nil
			}
		}
	} else {
		// Assign worker a reduce job
		for i, job := range c.reduceJobs {
			if job.status == UNASSIGNED {
				reply.JobId = job.job_id
				reply.JobType = REDUCE_TASK

				reply.IntermediateFiles = job.intermediateFiles
				fmt.Println("LOGAN worker job assigned with: ", reply.JobId, reply.JobType, reply.IntermediateFiles)

				// Mark this job as assigned
				c.reduceJobs[i].status = ASSIGNED
				return nil
			}
		}
	}

	return nil
}

// WorkerDone - end condition
// Processes the reply from worker
func (c *Coordinator) WorkerDone(args *NotifyDoneArgs, reply *IntReply) error {

	// RPC lock
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapDone == 0 {
		// Find matching map job
		for i := range c.mapJobs {
			if c.mapJobs[i].job_id == args.JobId {
				c.mapJobs[i].status = args.Status
				// c.mapJobs[i].file_location = args.Location TODO - store intermediate locations ?
				return nil
			}
		}
	} else {
		// Find matching reduce job
		for i := range c.reduceJobs {
			fmt.Println("LOGAN worker done reduce called with jobId", args.JobId)
			if c.reduceJobs[i].job_id == args.JobId {
				fmt.Println("LOGAN setting reduce as complete")
				c.reduceJobs[i].status = args.Status
				return nil
			}
		}
	}

	// Notify worker it's job is done
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
	if c.mapDone == 0 {
		total := 0
		complete := 0

		for _, job := range c.mapJobs {
			total++
			if job.status == FINISHED {
				complete++
			} else {
				ret = false
			}
		}
		fmt.Printf("Coordinator Status: \n"+
			"  Total Map Jobs: %d \n"+
			"  Complete Map Jobs: %d\n", total, complete)

	} else {
		total := 0
		complete := 0

		for _, job := range c.reduceJobs {
			total++
			if job.status == FINISHED {
				complete++
			} else {
				ret = false
			}
		}
		fmt.Printf("Coordinator Status: \n"+
			"  Total Reduce Jobs: %d \n"+
			"  Complete Reduce Jobs: %d\n", total, complete)
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}

	// Parameter values
	map_input_size := int64(64 * 1024 * 1024) // Map input data size == 64MB
	job_id := 1000                            // Starting map job ID
	c.numReduce = nReduce                     // Number of reduce tasks

	// Read input data files
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
		var chunkSize int64 = map_input_size
		// Split input data into "M" pieces of map_input_size
		for offset < fileSize {

			if fileSize-offset < chunkSize {
				chunkSize = fileSize - offset
			}

			// Add new map job to coordinator's map slice
			map_job := map_job{job_id, MAP_TASK, UNASSIGNED,
				file, offset, map_input_size}
			c.mapJobs = append(c.mapJobs, map_job)
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
			fmt.Println("Map jobs are complete. Creating reduce jobs...")
			break
		}
	}

	c.mapDone = 1 // Trigger reduce stage

	// grab RPC lock while creating reduce jobs
	c.mu.Lock()
	// Generate nReduce reduce jobs
	for rNum := 0; rNum < nReduce; rNum++ {
		// grep for the intermediate files of bucket R
		// TODO store this from RPC of map worker done
		var intermediateFiles = GetIntermediateFiles(rNum)
		reduce_job := reduce_job{rNum, UNASSIGNED, intermediateFiles}
		// Add new reduce job to coordinator's reduce slice
		c.reduceJobs = append(c.reduceJobs, reduce_job)
	}
	// unlock RPC lock to let reduce workers get jobs
	c.mu.Unlock()

	// Done calls periodically
	for {
		if c.Done() {
			fmt.Println("Reduce jobs are complete. Exiting...")
			break
		}
	}

	return &c
}
