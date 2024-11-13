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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Task struct {
	TaskType   TaskType
	TaskId     int
	ReducerNum int
	FileSlice  []string
}

type TaskArgs struct{}
type TaskType int
type Phase int
type State int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask
	ExitTask
)

const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)

const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行
	Done                 // 此阶段已经做完
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
