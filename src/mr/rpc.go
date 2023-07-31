package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "fmt"
import "runtime"


const (
    UNASSIGNED int  = -1
    FINISHED        = 0
    ASSIGNED        = 1
)

const (
    MAP_TASK int = 0
    REDUCE_TASK = 1
)

// A simple Integer argument
type IntArg struct {
	Status int
}

// A simple Integer reply, used for confirmation
type IntReply struct {
	Status int
}

// The Coordinate replies with Map Job request
// and gives details on the job to perform.
type JobReply struct {
	JobId int
	JobType int
	FileLocation string
	FileOffset int64
	DataLength int64
	NReduce int
}

// The worker must notify the Coordinator
// that the map/reduce is done and where the
// intermediate files are located.
type NotifyDoneArgs struct {
	JobId int
	Status int
	Location string
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// Logger function takes the function name and prints it.
func Logger() {
    pc, _, _, _ := runtime.Caller(1)
    fmt.Printf("Function called: %s\n", runtime.FuncForPC(pc).Name())
}
