package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		DoWork(mapf, reducef)
		//if !ok {
		//	log.Fatalf("Worker failed.\n")
		//	break
		//}
	}
}

func DoWork(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	// declare an argument structure.
	args := GetTaskInfoArgs{}

	// fill in the argument(s).
	pid := os.Getegid()
	args.WorkerId = int32(pid)

	// declare a reply structure.
	reply := GetTaskInfoReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Schedule", &args, &reply)
	if !ok {
		//log.Printf("call Coordinator.Schedule failed!\n")
		return false
	}

	status := reply.Status
	if TASK_STATUS_DONE == status {
		return DoCleaningWork(pid)
	}

	//log.Printf("Worker get work, worker id [%v], job name [%v], job id [%v] \n", pid, getTaskName(reply.Status), reply.ATask.TaskId)
	if TASK_STATUS_MAP == status {
		return MapTaskPipeline(&reply, mapf)
	} else if TASK_STATUS_REDUCE == status {
		return ReduceTaskPipeline(&reply, reducef)
	} else {
		return DoUnExpectedWork(&reply)
	}
}

func getTaskName(taskType int32) string {
	switch taskType {
	case TASK_STATUS_MAP:
		return "[*Map task*]"
	case TASK_STATUS_REDUCE:
		return "[*Reduce task*]"
	case TASK_STATUS_DONE:
		return "[*Task Done*]"
	default:
		return "[*Not Known task*]"
	}
}

func DoMapTask(reply *GetTaskInfoReply, mapf func(string, string) []KeyValue) bool {
	filename := reply.ATask.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %v", filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", filename)
		return false
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("Close file %v failed, error message: %v", filename, err)
		return false
	}

	kva := mapf(filename, string(content))
	hashMap := make(map[int][]KeyValue)

	nReduce := int(reply.NReduce)
	for _, kv := range kva {
		hashKey := ihash(kv.Key) % nReduce
		hashMap[hashKey] = append(hashMap[hashKey], kv)
	}

	for i := 0; i < nReduce; i++ {
		fileName := getTmpMapResultFileName(int(reply.ATask.WorkerId), int(reply.ATask.TaskId), i)
		file, err = os.Create(fileName)
		if err != nil {
			log.Fatalf("Create file %v failed, error message: %v", fileName, err)
			return false
		}

		enc := json.NewEncoder(file)
		for _, kv := range hashMap[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Encode value to file %v failed, error message: %v", fileName, err)
				return false
			}
		}

		err = file.Close()
		if err != nil {
			log.Fatalf("Close file failed, error message: %v", err)
			return false
		}
	}

	return true
}

func getTmpMapResultFileName(workerId int, mapTaskId int, reduceId int) string {
	return fmt.Sprintf("tmp-mr-%v-%v-%v", workerId, mapTaskId, reduceId)
}

func getMapResultFileName(mapTaskId int, reduceId int) string {
	return fmt.Sprintf("mr-%v-%v", mapTaskId, reduceId)
}

func getTmpReduceResultFileName(workerId int, reduceId int) string {
	return fmt.Sprintf("tmp-mr-out-%v-%v", workerId, reduceId)
}

func getReduceResultFileName(reduceId int) string {
	return fmt.Sprintf("mr-out-%v", reduceId)
}

func ReduceTaskPipeline(reply *GetTaskInfoReply, reducef func(string, []string) string) bool {
	ok := DoReduceTask(reply, reducef)
	if !ok {
		return false
	}
	return MarkReduceWorkDone(reply)
}

func MapTaskPipeline(reply *GetTaskInfoReply, mapf func(string, string) []KeyValue) bool {
	ok := DoMapTask(reply, mapf)
	if !ok {
		return false
	}
	return MarkMapWorkDone(reply)
}

func MarkReduceWorkDone(reply *GetTaskInfoReply) bool {
	// declare an argument structure.
	args := PostTaskDoneArgs{}

	// fill in the argument(s).
	args.Status = TASK_STATUS_REDUCE
	args.TaskId = reply.ATask.TaskId
	args.WorkerId = reply.ATask.WorkerId

	// declare a reply structure.
	postReply := PostTaskDoneReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.MarkTaskDone", &args, &postReply)
	if !ok {
		log.Fatalf("Call Coordinator.MarkTaskDone failed. \n")
		return false
	}

	//log.Printf("Call Coordinator.MarkTaskDone successfully!\n")
	taskId := int(reply.ATask.TaskId)
	if postReply.StatusCode == STATUS_CODE_OK {
		// rename file
		oldFilePath := getTmpReduceResultFileName(int(args.WorkerId), taskId)
		newFilePath := getReduceResultFileName(taskId)
		err := os.Rename(oldFilePath, newFilePath)
		if nil != err {
			log.Fatalf("Rename from [%v] to [%v] failed, error message: %v\n", oldFilePath, newFilePath, err)
			return false
		}
	}
	//log.Printf("Reduce task done: [%v]", taskId)
	return true
}

func MarkMapWorkDone(reply *GetTaskInfoReply) bool {
	// declare an argument structure.
	args := PostTaskDoneArgs{}

	// fill in the argument(s).
	args.Status = TASK_STATUS_MAP
	args.TaskId = reply.ATask.TaskId
	args.WorkerId = reply.ATask.WorkerId

	// declare a reply structure.
	postReply := PostTaskDoneReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.MarkTaskDone", &args, &postReply)
	if !ok {
		log.Fatalf("Call Coordinator.MarkTaskDone failed. \n")
		return false
	}

	//log.Printf("Call Coordinator.MarkTaskDone successfully!\n")
	taskId := int(reply.ATask.TaskId)
	workerId := int(reply.ATask.WorkerId)
	if postReply.StatusCode == STATUS_CODE_OK {
		// rename file
		for i := 0; i < int(reply.NReduce); i++ {
			oldFilePath := getTmpMapResultFileName(workerId, taskId, i)
			newFilePath := getMapResultFileName(taskId, i)
			err := os.Rename(oldFilePath, newFilePath)
			if nil != err {
				log.Fatalf("Rename from [%v] to [%v] failed.\n", oldFilePath, newFilePath)
				return false
			}
		}
	}
	//log.Printf("Map task done: [%v]", taskId)
	return true
}

func DoReduceTask(reply *GetTaskInfoReply, reducef func(string, []string) string) bool {
	reduceTaskId := int(reply.ATask.TaskId)
	workerId := int(reply.ATask.WorkerId)

	nMap := reply.NMap
	// read all files
	intermediate := []KeyValue{}
	for i := 0; i < int(nMap); i++ {
		fileName := getMapResultFileName(i, reduceTaskId)
		file, err := os.Open(fileName)
		if err != nil {
			//log.Fatalf("cannot open %v, error message: %v", fileName, err) // see this as an failed task, return and the coordinator will reschedule it
			return false
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				//fmt.Printf("End of file found!")
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	outputFileName := getTmpReduceResultFileName(workerId, reduceTaskId)
	outputFile, _ := os.Create(outputFileName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	err := outputFile.Close()
	if nil != err {
		log.Fatalf("Close file [%s] failed, error message: %v", outputFileName, err)
		return false
	}

	return true
}

func DoCleaningWork(workerId int) bool {
	//log.Printf("Woker[%v]: All work Done!", workerId)
	os.Exit(0)
	return true
}

func DoUnExpectedWork(reply *GetTaskInfoReply) bool {
	//log.Printf("Woker[%v]: get unexpected work", reply.ATask.WorkerId)
	os.Exit(0)
	return true
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
