package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	
	mapFiles []string
	nMapTasks int
	nReduceTasks int

	//keep track of when tasks are assigned
	//and which tasks have finished
	mapTasksFinished []bool
	mapTasksIssued []time.Time  //time when each task was assigned
	reduceTasksFinished []bool
	reduceTasksIssued []time.Time //time when each task was assigned

	//true when all reduce tasks are complete
	isDone bool
}

// Your code here -- RPC handlers for the worker to call.
//
// the RPC argument and reply types are defined in rpc.go.
//


func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock();
	defer c.mu.Unlock();
	fmt.Println("Received a request from a worker containing args: ", args);
	
	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	//TODO
	//issue all map and reduce tasks


	reply.TaskType = Done
	c.isDone = true

	return nil
}

func (c *Coordinator) HandleFinishedTask(args *FinishedTasksArgs, reply *FinishedTaskReply) error{
	c.mu.Lock();
	defer c.mu.Unlock()

	switch args.TaskType{
	case Map:
		c.mapTasksFinished[args.TaskNum] = true;
	case Reduce:
		c.reduceTasksFinished[args.TaskNum] = true;
	default:
		log.Fatalf("Bad finished task? %s", args.TaskType)
	}
	return nil;
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
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	
	// Your code here.

	return len(c.doneJobs) == len(c.fileNames)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	fmt.Println("These are all the file names", files);
	c.fileNames = files;

	c.server()
	return &c
}
