package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
//finalizeReduceFile atomically renames temporary refuce file to a completed reduce task file
//
func finalizeReduceFile(tmpFileName string, taskN int){
	finalFileName := fmt.Sprintf("mr-out-%d", taskN);
	os.Rename(tmpFileName, finalFileName);
}

//
//get name of the intermediate file, given the map and reduce task numbers
//
func getIntermediateFile(mapTaskN int, redTaskN int) string{
	return fmt.Sprintf("mr-%d-%d", mapTaskN, redTaskN);
}

//
//finalizeIntermediateFile atomically renames temporary intermediate file to a completed intermediate task file
//
func finalizeIntermediateFile(tmpFileName string, mapTaskN int, redTaskN int){
	finalFileName := getIntermediateFile(mapTaskN, redTaskN)
	os.Rename(tmpFileName, finalFileName);
}

func performMap(fileName string, taskNum int, nReduceTasks int, mapf func(string, string) []KeyValue){
	//read file contents
	file, err := os.Open(fileName)
	if err != nil{
		log.Fatalf("cannot open %v", fileName)
	}

	content, err := ioutil.ReadAll(file);
	if err != nil{
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close();

	//apply map function and collect kv pairs
	kva := mapf(fileName, string(content))

	//create temporary files and encoders for each file
	tmpFiles := []*os.File{}
	tmpFileNames := []string{}
	encoders := []*json.Encoder{}

	for r := 0; r < nReduceTasks; r++{
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("cannot open tmpfile");
		}
		tmpFiles = append(tmpFiles, tmpFile);
		tmpFileName := tmpFile.Name();
		tmpFileNames = append(tmpFileNames, tmpFileName)
		enc := json.NewEncoder(tmpFile);
		encoders = append(encoders, enc);
	}

	//write output to the temporary files
	for _, kv := range kva{
		r := ihash(kv.Key) % nReduceTasks //decide where each keys goes
		encoders[r].Encode(&kv);
	}
	for _, f := range tmpFiles{
		f.Close()
	}

	//atomically rename tmp files to final intermediate files
	for r := 0; r < nReduceTasks; r++{
		finalizeIntermediateFile(tmpFileNames[r], taskNum, r)
	}
}

func performReduce(taskNum int, nMapTasks int, reducef func(string, []string) string){
	//get all intermediate files corresponding to said reduce task,
	//and collect the corresponding kv pairs
	kva := []KeyValue{}
	for m := 0; m < nMapTasks; m++{
		fileName := getIntermediateFile(0, taskNum)
		file, err := os.Open(fileName);
		if err != nil{
			log.Fatalf("cannot open %v", fileName)
		}

		dec := json.NewDecoder(file);
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil{
				break
			}
			kva = append(kva, kv);
		}
	}
	sort.Sort(ByKey(kva))

	//create a temp reduce file to write the values
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil{
		log.Fatalf("cannot open tmpfile")
	}
	tmpFileName := tmpFile.Name();
	key_begin := 0
	for key_begin < len(kva) {
		key_end := key_begin + 1
		for key_end < len(kva) && kva[key_end].Key == kva[key_begin].Key {
			key_end++
		}
		values := []string{}
		for k := key_begin; k < key_end; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[key_begin].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", kva[key_begin].Key, output)

		key_begin = key_end
	}
	//atomically rename reduce file to final reduce file
	finalizeReduceFile(tmpFileName, taskNum);
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
