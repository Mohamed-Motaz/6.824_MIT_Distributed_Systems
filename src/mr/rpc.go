package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// Add your RPC definitions here.

type TaskType int;

const (
	Mao TaskType = 1;
	Reduce TaskType = 2;
	Done TaskType = 3;
)

//No args to send the coordinator for tasks
type GetTaskArgs struct{}

//Tasks sent from the coordinator to the workers
type GetTaskReply struct{

	//Type of the task
	TaskType TaskType

	//Num of the task (either map or reduce)
	TaskNum int

	//needed for Map (to know which file to write)
	NReduceTasks int

	//needed for Map (to know which file to read)
	MapFile string

	//needed for Reduce (to know how many intermediate files to read)
	NMapTasks int

}


//Finished tasks sent from the idle workers to the coordinator to indicate that a task has been finished
type FinishedTasksArgs struct{
	//Type of the task
	TaskType TaskType

	//Num of the task (either map or reduce)
	TaskNum int
}

//No reply needed from the workers to the coordinator
type FinishedTaskReply struct{
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
