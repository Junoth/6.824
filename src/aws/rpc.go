package aws

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type GetTaskArgs struct{}

type GetTaskReply struct {
	IsMap  bool
	File   string
	TaskId int
	Num    int // for map worker is the reduce file num, for reduce worker is the map file num
}

type FinishTaskArgs struct {
	TaskId int
}

type FinishTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
