package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type JobStatus int

const (
	Ready JobStatus = iota
	Progress
	Finish
)

type MapTask struct {
	taskName string
	status   JobStatus
	stTime   time.Time
}

type ReduceTask struct {
	status JobStatus
	stTime time.Time
}

// SafeMapTaskManager define a safe map manager struct for concurrent case
type SafeMapTaskManager struct {
	tasks    []MapTask  // task queue ready to be assigned
	lock     sync.Mutex // lock
	taskDone int        // number of finished tasks
}

// SafeReduceTaskManager define a safe reduce manager struct for concurrent case
type SafeReduceTaskManager struct {
	tasks    []ReduceTask
	lock     sync.Mutex // lock
	taskDone int        // number of finished tasks
}

// Coordinator
// for coordinator, we need a map to know the input file name and according worker
// we need a list of input files that are not allocated yet
// need a list of input files that are already done
//
type Coordinator struct {
	mapManager    *SafeMapTaskManager
	reduceManager *SafeReduceTaskManager
	nReduce       int
}

// AssignTask define safe assign task method for map task manager
func (m *SafeMapTaskManager) AssignTask() (int, string) {
	// make sure we add lock at first and unlock at last
	m.lock.Lock()
	defer m.lock.Unlock()

	for i, t := range m.tasks {
		now := time.Now()
		switch t.status {
		case Finish:
			continue
		case Ready:
			m.tasks[i].stTime = now
			m.tasks[i].status = Progress
			log.Printf("Map task %d, %s is assigned\n", i, t.taskName)
			return i, t.taskName
		case Progress:
			if now.Sub(t.stTime).Seconds() > 10 {
				// assume the worker is dead, try to reassign
				m.tasks[i].stTime = now
				return i, t.taskName
			}
		default:
			panic("Invalid status")
		}
	}

	// all tasks in progress or finished
	return -1, ""
}

func (m *SafeMapTaskManager) FinishTask(taskId int) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.tasks[taskId].status = Finish
	m.taskDone = m.taskDone + 1
}

// MapDone define safe method to check if map work is done
func (m *SafeMapTaskManager) MapDone() bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.taskDone == len(m.tasks)
}

// AssignTask define safe assign task method for reduce task manager
func (m *SafeReduceTaskManager) AssignTask() int {
	// make sure we add lock at first and unlock at last
	m.lock.Lock()
	defer m.lock.Unlock()

	for i, t := range m.tasks {
		now := time.Now()
		switch t.status {
		case Finish:
			continue
		case Ready:
			m.tasks[i].stTime = now
			m.tasks[i].status = Progress
			log.Printf("Reduce task %d is assigned\n", i)
			return i
		case Progress:
			if now.Sub(t.stTime).Seconds() > 10 {
				// assume the worker is dead, try to reassign
				m.tasks[i].stTime = now
				return i
			}
		default:
			panic("Invalid status")
		}
	}

	// all tasks in progress or finished
	return -1
}

func (m *SafeReduceTaskManager) FinishTask(taskId int) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.tasks[taskId].status = Finish
	m.taskDone = m.taskDone + 1
}

// ReduceDone define safe method to check if reduce work is done
func (m *SafeReduceTaskManager) ReduceDone() bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.taskDone == len(m.tasks)
}

// GetTask rpc function for worker to call when try to get task
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.mapManager.MapDone() {
		if c.reduceManager.ReduceDone() {
			return errors.New("Mr task done, pls exit")
		}

		// map work is done, we're doing reduce
		i := c.reduceManager.AssignTask()

		reply.IsMap = false
		reply.TaskId = i
		reply.File = "" // we don't need file name for reduce
		reply.Num = len(c.mapManager.tasks)
		return nil
	}

	i, file := c.mapManager.AssignTask()

	reply.IsMap = true
	reply.File = file
	reply.TaskId = i
	reply.Num = c.nReduce
	return nil
}

// FinishTask rpc function for worker to call when task finished
func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	if c.mapManager.MapDone() {
		// reduce now
		c.reduceManager.FinishTask(args.TaskId)
	} else {
		c.mapManager.FinishTask(args.TaskId)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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
	return c.reduceManager.ReduceDone()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapManager = &SafeMapTaskManager{}
	c.reduceManager = &SafeReduceTaskManager{}
	c.nReduce = nReduce
	for _, f := range files {
		mapTask := MapTask{taskName: f, status: Ready, stTime: time.Now()}
		c.mapManager.tasks = append(c.mapManager.tasks, mapTask)
	}
	for i := 0; i < nReduce; i++ {
		reduceTask := ReduceTask{status: Ready, stTime: time.Now()}
		c.reduceManager.tasks = append(c.reduceManager.tasks, reduceTask)
	}
	c.server()
	return &c
}
