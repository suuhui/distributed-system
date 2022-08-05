package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//

var exitChan chan bool
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
	exitChan = make(chan bool)
	for {
		select {
		case <-exitChan:
			return
		default:
			task := GetOneTask()
			if task == nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			//fmt.Printf("get phase-%d, task-%d\n", task.TaskPhase, task.TaskId)
			switch task.TaskPhase {
			case MapPhase:
				doMapTask(task, mapf)
			case ReducePhase:
				doReduceTask(task, reducef)
			}
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(task *Task, mapf func(string, string) []KeyValue) {
	var intermediate []KeyValue
	for _, filename := range task.FileList {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println(err)
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	intermediateMap := make(map[string][]KeyValue, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		tempFilename := getIntermediateFilename(task.TaskId, i)
		intermediateMap[tempFilename] = make([]KeyValue, 0)
	}

	for _, kv := range intermediate {
		reduceId := ihash(kv.Key) % task.NReduce
		tempName := getIntermediateFilename(task.TaskId, reduceId)
		intermediateMap[tempName] = append(intermediateMap[tempName], kv)
	}

	fileList := make([]string, 0, len(intermediateMap))
	for filename, kvList := range intermediateMap {
		fileList = append(fileList, filename)

		rmIfFileExists(filename)
		ofile, _ := os.Create(filename)
		encoder := json.NewEncoder(ofile)
		for _, kv := range kvList {
			encoder.Encode(kv)
		}
		ofile.Close()
	}
	task.FileList = fileList

	ReportTaskResult(task)
}

func doReduceTask(task *Task, reducef func(string, []string) string) {
	var intermediateKv []KeyValue

	for _, filename := range task.FileList {
		ofile, _ := os.Open(filename)
		decoder := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediateKv = append(intermediateKv, kv)
		}
		ofile.Close()
	}

	sort.Sort(ByKey(intermediateKv))

	outputFile := fmt.Sprintf("mr-out-%d", task.TaskId)
	rmIfFileExists(outputFile)
	ofile, _ := os.Create(outputFile)

	i := 0
	for i < len(intermediateKv) {
		j := i + 1
		for j < len(intermediateKv) && intermediateKv[j].Key == intermediateKv[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediateKv[k].Value)
		}
		output := reducef(intermediateKv[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediateKv[i].Key, output)

		i = j
	}
	ofile.Close()
	ReportTaskResult(task)
}

func getIntermediateFilename(mapId, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func GetOneTask() *Task {
	args := GetArgs{}
	reply := GetReply{}
	ok := call("Coordinator.GetOneTask", &args, &reply)
	if ok {
		return reply.RealTask
	} else {
		return nil
	}
}

func ReportTaskResult(task *Task) {
	args := ReportArgs{
		task, true,
	}
	reply := ReportReply{}
	call("Coordinator.ReportTaskResult", &args, &reply)
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
		netErr, ok := err.(*net.OpError)
		if ok && netErr.Op == "dial" {
			t := reflect.TypeOf(netErr.Err)
			fmt.Println(t.String())
			sysErr, ok := netErr.Err.(*os.SyscallError)
			if ok {
				fmt.Println(sysErr.Syscall)
				s := reflect.TypeOf(sysErr.Err)
				fmt.Println(s.String())
			}
			go func(c chan bool) {
				c <- true
			}(exitChan)
		}
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

func rmIfFileExists(filename string) {
	if fileExists(filename) {
		_ = os.Remove(filename)
	}
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err != nil {
		return os.IsExist(err)
	}
	return true
}
