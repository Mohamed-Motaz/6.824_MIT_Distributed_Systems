package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	//keep asking for tasks continuously
	for {
		args := GetTaskArgs{};
		reply := GetTaskReply{};
		call("Coordinator.HandleGetTask", &args, &reply) //blocking call that waits until we get a reply
		fmt.Println("I sent a request: ", args)
		fmt.Println("And got this back", reply);

		switch reply.TaskType{
		case Map:
			performMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf);
		case Reduce:
			performReduce(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			//no more tasks, so we as workers exit
			os.Exit(0);
		default:
			fmt.Errorf("Bad task type? %s", reply.TaskType)
		}

		//tell the coordinator that we are done
		finArgs := FinishedTasksArgs{
			TaskType: reply.TaskType,
			TaskNum: reply.TaskNum,
		}
		finReply := FinishedTaskReply{}
		call("Coordinator.HandleFinishedTask", &finArgs, &finReply) 

		


	}
	

	// uncomment to send the Example RPC to the coordinator.
	//  CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	fmt.Println("About to start the call");
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("This is the error", err)
	return false
}
