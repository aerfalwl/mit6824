package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"sync"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	TASK_STATUS_NONE   = 0
	TASK_STATUS_MAP    = 1
	TASK_STATUS_REDUCE = 2
	TASK_STATUS_DONE   = 3
)

const (
	STATUS_CODE_OK    = 0
	STATUS_CODE_ERROR = 1
)

type Task struct {
	WorkerId int32
	TaskId   int32
	Deadline time.Time
	Status   int32 // Map or Reduce
	Done     bool
	File     string
	m        sync.Mutex
}

// Add your RPC definitions here.
type GetTaskInfoArgs struct {
	WorkerId int32
}

type PostTaskDoneArgs struct {
	WorkerId int32
	Status   int32
	TaskId   int32
}

type GetTaskInfoReply struct {
	Id      int32
	NReduce int32
	NMap    int32
	ATask   *Task
	Status  int32
}

type PostTaskDoneReply struct {
	StatusCode int32
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
