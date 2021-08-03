package mr

import (
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

	cond *sync.Cond

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
	//fmt.Println("Received a request from a worker containing args: ", args);
	
	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	//issue all map tasks
	for {
		mapDone := true
		for m, done := range c.mapTasksFinished{
			if !done{   
				//we need to send tasks that arent done yet
				if c.mapTasksIssued[m].IsZero() || 
					time.Since(c.mapTasksIssued[m]).Seconds() > 10 {  //been more than 10 seconds with no response
					
					reply.MapFile = c.mapFiles[m];
					reply.TaskNum = m;
					reply.TaskType = Map;  
					c.mapTasksIssued[m] = time.Now()
					return nil //sucessfuly gave out a task to the calling worker 
					
				}else{
					mapDone = false;
				}
			}
		}
		//if all maps are in progress and havent timed out, wait to give another task
		//so if one indeed times out, give it to the current worker
		if !mapDone{
			//wait for the task to be done
			c.cond.Wait()
		}else{
			break;
		}
	}
	//issue all reduce tasks as all map tasks are done
	for {
		redDone := true;
		for r, done := range c.reduceTasksFinished{
			if !done{
				if c.reduceTasksIssued[r].IsZero() || 
					time.Since(c.reduceTasksIssued[r]).Seconds() > 10 {  //been more than 10 seconds with no response
				
					reply.TaskNum = r;
					reply.TaskType = Reduce;  
					c.reduceTasksIssued[r] = time.Now()
					return nil //sucessfuly gave out a task to the calling worker 
					
				}else{
					redDone = false;
				}
			}
		}
		//if all reduces are in progress and havent timed out, wait to give another task
		//so if one indeed times out, give it to the current worker
		if !redDone{
			//wait for the task to be done
			c.cond.Wait()
		}else{
			break;
		}
	}

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
		log.Fatalf("bad finished task? %v", args.TaskType)
	}

	//wake up the getTask handler as a task has finished, so we might be able to assign another
	c.cond.Broadcast()

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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isDone;
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//fmt.Println("These are all the file names", files);

	c.cond = sync.NewCond(&c.mu)

	c.mapFiles = files;
	c.nMapTasks = len(files);
	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, len(files))

	c.nReduceTasks = nReduce;
	c.reduceTasksFinished = make([]bool, nReduce)
	c.reduceTasksIssued = make([]time.Time, nReduce)

	//wake up GetTask handler thread every once in awhile to check if
	//some task has timed out, so we can reissue it to a different worker
	go func(){
		for {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
	c.server()
	return &c
}
