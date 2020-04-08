package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	mapConst = iota + 1
	reduceConst
)

// this is the first time I use go rpc, so it's hard to
type GetTaskReq struct {
}

type GetTaskResp struct {
	Valid       bool
	Done        bool
	TaskType    int
	File        string
	ReduceN     int
	MapOrder    int
	MapN        int
	ReduceOrder int
}

type CommitWorkReq struct {
	TaskType    int
	File        string
	ReduceN     int
	MapOrder    int
	MapN        int
	ReduceOrder int
}

type CommitWorkResp struct {
	Done bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
