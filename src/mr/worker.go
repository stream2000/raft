package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type mapTask struct {
	File     string
	ReduceN  int
	MapOrder int
}

type reduceTask struct {
	ReduceOrder int
	MapN        int
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
// Worker main function of worker process
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		reply, ok := GetTaskCall()
		if !ok || reply.Done {
			return
		}
		if !reply.Valid {
			// should use back off algorithm
			time.Sleep(time.Second * 2)
			continue
		}
		if reply.TaskType == mapConst {
			processMapTask(mapTask{
				File:     reply.File,
				ReduceN:  reply.ReduceN,
				MapOrder: reply.MapOrder,
			}, mapf)
			commitWorkArgs := CommitWorkReq{
				TaskType: mapConst,
				File:     reply.File,
				ReduceN:  reply.ReduceN,
				MapOrder: reply.MapOrder,
			}
			done, ok := CommitWorkCall(commitWorkArgs)
			if !ok || done {
				return
			}
		} else {
			processReduceTask(reduceTask{
				ReduceOrder: reply.ReduceOrder,
				MapN:        reply.MapN,
			}, reducef)
			commitWorkArgs := CommitWorkReq{
				TaskType:    reduceConst,
				ReduceOrder: reply.ReduceOrder,
				MapN:        reply.MapN,
			}
			done, ok := CommitWorkCall(commitWorkArgs)
			if !ok || done {
				return
			}
		}
	}
}

func GetTaskCall() (GetTaskResp, bool) {
	args := GetTaskReq{}
	reply := GetTaskResp{}
	// send the RPC request, wait for the reply.
	ok := call("Master.GetTask", &args, &reply)
	return reply, ok
}

func CommitWorkCall(args CommitWorkReq) (done, ok bool) {
	reply := CommitWorkResp{}
	ok = call("Master.CommitWork", &args, &reply)
	return reply.Done, ok
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

func processMapTask(task mapTask, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.File)
	if err != nil {
		log.Fatalf("cannot open %v", task.File)
	}
	// 不用缓冲有点暴力的
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}
	file.Close()
	kva := mapf(task.File, string(content))
	splits := make([][]KeyValue, task.ReduceN)
	for _, kv := range kva {
		hash := ihash(kv.Key) % task.ReduceN
		if splits[hash] != nil {
			splits[hash] = append(splits[hash], kv)
		} else {
			splits[hash] = []KeyValue{kv}
		}
	}
	for i, split := range splits {
		intermediateName := fmt.Sprintf("mr-%d-%d", task.MapOrder, i)
		f, err := ioutil.TempFile(".", "mr")
		if err != nil {
			log.Fatalf("error create temp file %+v", err)
		}
		for _, kv := range split {
			f.WriteString(fmt.Sprintf("%s,%s\n", kv.Key, kv.Value))
		}
		err = os.Rename(f.Name(), intermediateName)
		if err != nil {
			log.Fatalf("error rename file %+v", err)
		}
	}

	return
}

func processReduceTask(task reduceTask, reducef func(string, []string) string) {
	intermediate := make([]KeyValue, 0)
	for i := 0; i < task.MapN; i++ {
		intermediateName := fmt.Sprintf("mr-%d-%d", i, task.ReduceOrder)
		f, err := os.Open(intermediateName)
		if err != nil {
			log.Fatalf("cannot open file %s", intermediateName)
		}
		br := bufio.NewReader(f)
		for {
			a, _, c := br.ReadLine()
			if c == io.EOF {
				break
			}
			split := strings.Split(string(a), ",")
			if len(split) < 2 {
				log.Fatalf("error format value")
			}
			intermediate = append(intermediate, KeyValue{
				Key:   split[0],
				Value: split[1],
			})
		}
		f.Close()
	}
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})
	f, err := ioutil.TempFile(".", "mr")
	if err != nil {
		log.Fatalf("error create temp file %+v", err)
	}
	oname := fmt.Sprintf("mr-out-%d", task.ReduceOrder)
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	err = os.Rename(f.Name(), oname)
	if err != nil {
		log.Fatalf("error rename file %+v", err)
	}
	f.Close()
	return
}
