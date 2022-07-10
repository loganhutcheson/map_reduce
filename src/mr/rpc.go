package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type MapJobReply struct {
	File string
	Index int
	Length int64
}

// The worker must notify the Coordinator
// that the map/reduce is done and where the
// intermediate files are located.
type NotifyDoneArgs struct {
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
