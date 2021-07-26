package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)


type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	doneJobs map[string]bool
	fileNames []string
}

// Your code here -- RPC handlers for the worker to call.
//
// the RPC argument and reply types are defined in rpc.go.
//


func (c *Coordinator) SendingATaskToWorker(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock();
	defer c.mu.Unlock();
	fmt.Println("Received a request from a worker containing args: ", args);
	
	taskFileName, err := c.decideTaskToGiveOut();
	if err != nil{
		//no more jobs
		fmt.Println("All jobs done, about to exit the program")
	}
	reply.FileName = taskFileName;
	reply.TaskToDo = TaskType()
	return nil
}

func (c *Coordinator) decideTaskToGiveOut() (string, error){
	
	for _, v := range c.fileNames{
		if done, ok := c.doneJobs[v]; !ok && !done{
			return v, nil;
		}
	}
	return "", fmt.Errorf("no more files to serve");
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
