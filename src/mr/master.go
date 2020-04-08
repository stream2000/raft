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

type Master struct {
	mapCompleted    bool
	reduceCompleted bool
	mTasks          *mapTasks
	rTasks          *reduceTasks
	reduceN         int
	sync.Mutex
}

func (m *Master) GetTask(args *GetTaskReq, reply *GetTaskResp) (err error) {
	m.Lock()
	defer m.Unlock()
	reply.Valid = true
	if m.reduceCompleted {
		reply.Done = true
		return
	}
	if !m.mapCompleted {
		// nothing to do, return and sleep 5 seconds
		if m.mTasks.ready.Len() == 0 {
			reply.Valid = false
			return
		}
		reply.Valid = true
		n := m.mTasks.ready.FetchOne().(int)
		file := m.mTasks.tasks[n]
		reply.TaskType = mapConst
		reply.File = file
		reply.ReduceN = m.reduceN
		reply.MapOrder = n
		m.mTasks.inProgress.Add(n)
		// a timer to reschedule map task
		time.AfterFunc(time.Second*10, func() {
			m.Lock()
			defer m.Unlock()
			if m.mTasks.inProgress.Contains(n) {
				m.mTasks.ready.Add(n)
				m.mTasks.inProgress.Delete(n)
			}
		})
	} else {
		if m.rTasks.ready.Len() == 0 {
			reply.Valid = false
			return
		}
		reply.Valid = true
		n := m.rTasks.ready.FetchOne().(int)
		reply.TaskType = reduceConst
		reply.MapN = len(m.mTasks.tasks)
		reply.ReduceOrder = n
		m.rTasks.inProgress.Add(n)
		// a timer to reschedule map task
		time.AfterFunc(time.Second*10, func() {
			m.Lock()
			defer m.Unlock()
			if m.rTasks.inProgress.Contains(n) {
				m.rTasks.ready.Add(n)
				m.rTasks.inProgress.Delete(n)
			}
		})
	}
	return
}

func (m *Master) CommitWork(arg *CommitWorkReq, reply *CommitWorkResp) (err error) {
	m.Lock()
	defer m.Unlock()
	reply = new(CommitWorkResp)

	if m.reduceCompleted {
		reply.Done = true
		return
	}
	if arg.TaskType == mapConst {
		defer func() {
			if m.mTasks.completed.Len() == len(m.mTasks.tasks) {
				m.mapCompleted = true
			}
		}()
		// the task is already committed by another worker
		if m.mTasks.completed.Contains(arg.MapOrder) {
			return
		}
		// mark as completed
		if m.mTasks.inProgress.Contains(arg.MapOrder) {
			m.mTasks.inProgress.Delete(arg.MapOrder)
			m.mTasks.completed.Add(arg.MapOrder)
			return
		}
		// the task is timeout and in ready state
		if m.mTasks.ready.Contains(arg.MapOrder) {
			m.mTasks.ready.Delete(arg.MapOrder)
			m.mTasks.completed.Add(arg.MapOrder)
		}
	} else {
		defer func() {
			if m.rTasks.completed.Len() == m.reduceN {
				m.reduceCompleted = true
				reply.Done = true
			}
		}()
		// the task is already committed by another worker
		if m.rTasks.completed.Contains(arg.ReduceOrder) {
			return
		}
		// mark as completed
		if m.rTasks.inProgress.Contains(arg.ReduceOrder) {
			m.rTasks.inProgress.Delete(arg.ReduceOrder)
			m.rTasks.completed.Add(arg.ReduceOrder)
			return
		}
		// the task is timeout and in ready state
		if m.rTasks.ready.Contains(arg.ReduceOrder) {
			m.rTasks.ready.Delete(arg.ReduceOrder)
			m.rTasks.completed.Add(arg.ReduceOrder)
		}
	}
	return
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// weather all works have done
func (m *Master) Done() bool {
	m.Lock()
	defer m.Unlock()
	return m.reduceCompleted
}

// init master struct
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mTasks = initMapTasks(files)
	m.rTasks = initReduceTasks(nReduce)
	m.reduceN = nReduce
	m.server()
	return &m
}
