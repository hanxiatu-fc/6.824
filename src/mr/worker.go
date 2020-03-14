package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


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


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	conti := true
	for conti {
		conti = callMaster(mapf, reducef)
	}

	time.Sleep(time.Microsecond * 50)
}

//
// example function to show how to make an RPC call to the master.
//
func callMaster(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	request := Request{}
	reply := Reply{}

	// send the RPC request, wait for the reply.
	ok := call("Master.GetTask", &request, &reply)
	if !ok {
		return false
	}

	task := reply.Task
	if task.TaskType == Map {
		callMap(mapf, task)
	} else if task.TaskType == Reduce {
		callReduce(reducef, task)
	}

	return true
}

func callMap(mapf func(string, string) []KeyValue, task *Task)  {
	if task != nil {
		// get all intermediate data
		var intermediate []KeyValue
		for _, filename := range task.FileList {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			_ = file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
		}

		sort.Sort(ByKey(intermediate))

		// group intermediate date by (key hash % nReduce) ==> a key's all value will be at one reduce task
		tmp := make(map[int][]KeyValue)
		for _, kv := range intermediate {
			key := ihash(kv.Key) % task.NReduce
			tmp[key] = append(tmp[key], kv)
		}

		// save intermediate data into local file
		for k, v := range tmp {
			dir, _ := os.Getwd()
			file, _ := ioutil.TempFile(dir, "tmp-")

			enc := json.NewEncoder(file)
			for _, kv := range v {
				err := enc.Encode(&kv)
				if err != nil {
					break
				}
			}

			finalName := fmt.Sprintf("mr-%d-%d", task.Id, k)
			if Debug {
				fmt.Printf("rename %s --> %s \n", file.Name(), finalName)
			}
			_ = os.Rename(file.Name(), finalName)
			_ = file.Close()
		}

		// report
		request := Request{}
		request.Task = task
		request.Task.TaskState = Success
		reply := Reply{}
		call("Master.ReportTask", &request, &reply)
	}
}

func callReduce(reducef func(string, []string) string, task *Task)  {
	if task != nil {
		intermediate := []KeyValue{}
		if Debug {
			fmt.Printf("fileList %d %s \n", len(task.FileList), task.FileList)
		}
		for _, filename := range task.FileList {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			file.Close()
		}

		sort.Sort(ByKey(intermediate))

		dir, _ := os.Getwd()
		ofile, _ := ioutil.TempFile(dir, "tmp-")
		defer ofile.Close()

		if Debug {
			fmt.Printf("intermediate : %d \n", len(intermediate))
		}

		tmp := make(map[string][]string)
		for i := 0; i < len(intermediate); i++ {
			tmp[intermediate[i].Key] = append(tmp[intermediate[i].Key], intermediate[i].Value)
		}

		for k,v := range tmp {
			output := reducef(k, v)
			fmt.Fprintf(ofile, "%v %v\n", k, output)
		}

		// report
		request := Request{}
		request.Task = task
		request.Task.TaskState = Success
		reply := Reply{}
		call("Master.ReportTask", &request, &reply)

		if reply.Task != nil { // master accept
			oname := fmt.Sprintf("mr-out-%d", task.Id)
			if Debug {
				fmt.Printf("rename %s --> %s \n", ofile.Name(), oname)
			}
			_ = os.Rename(ofile.Name(), oname)
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", SocketFile)
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

