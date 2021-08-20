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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

func mapWorker(filename string, taskId int, reduceNum int, mapf func(string, string) []KeyValue) {
	// for every input file, open and read content
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// mapf function should return kv array
	kva := mapf(filename, string(content))

	// we need a map for kv in different buckets
	buckets := map[int][]KeyValue{}

	for i := 0; i < len(kva); i++ {
		index := ihash(kva[i].Key) % reduceNum
		buckets[index] = append(buckets[index], kva[i])
	}

	for i := 0; i < reduceNum; i++ {
		// create temp file
		oname := fmt.Sprintf("mr-%d-%d", taskId, i)
		ofile, _ := ioutil.TempFile("", "map")
		defer ofile.Close()

		// write kv as json into files
		enc := json.NewEncoder(ofile)
		for j, _ := range buckets[i] {
			err := enc.Encode(&buckets[i][j])
			if err != nil {
				log.Fatal("Encoding:", err)
			}
		}

		// rename the file to intermediate file
		os.Rename(ofile.Name(), oname)
	}
}

func reduceWorker(taskId int, mapNum int, reducef func(string, []string) string) {
	// read all result from every intermediate file
	var kva []KeyValue
	for i := 0; i < mapNum; i++ {
		oname := fmt.Sprintf("mr-%d-%d", i, taskId)
		file, err := os.Open(oname)
		if err != nil {
			log.Fatalf("cannot open %v", oname)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	// sort intermediate file
	sort.Sort(ByKey(kva))

	// create output file
	oname := fmt.Sprintf("mr-out-%v", taskId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// give thread id to worker

	for {
		reply := GetTaskReply{}
		ret := CallGetTask(&reply)
		if !ret {
			// Get error in RPC call
			break
		}

		if reply.TaskId != -1 {
			// if get a task
			if reply.IsMap {
				mapWorker(reply.File, reply.TaskId, reply.Num, mapf)
			} else {
				reduceWorker(reply.TaskId, reply.Num, reducef)
			}

			CallFinishTask(reply.TaskId)
		}

		time.Sleep(time.Second)
	}
}

//
// function to call coordinator for task
//
// the RPC argument and reply types are defined in rpc.go
//
func CallGetTask(reply *GetTaskReply) bool {
	// send the RPC request, wait for the reply
	args := &GetTaskArgs{}
	return call("Coordinator.GetTask", args, reply)
}

//
// function to call coordinator for finishing a task
//
// the RPC argument and reply types are defined in rpc.go
//
func CallFinishTask(taskId int) bool {
	// declare get task arg and reply
	args := FinishTaskArgs{taskId}
	reply := &FinishTaskReply{}

	// send the RPC request
	return call("Coordinator.FinishTask", &args, reply)
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

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
