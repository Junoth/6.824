package mr

import (
	"errors"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// SafeMapTaskManager define a safe map manager struct for concurrent case
type SafeMapTaskManager struct {
	taskQueue     []string               // task queue ready to be assigned
	lock          sync.Mutex             // lock
	nextId		  int					 // next task id
	taskDone      int					 // number of finished tasks
}

// SafeReduceTaskManager define a safe reduce manager struct for concurrent case
type SafeReduceTaskManager struct {
	reduceNum     int                    // total reduce number
	lock          sync.Mutex         	 // lock
	nextId        int 				 	 // next task id
	taskDone      int                	 // number of finished tasks
}

// Coordinator
// for coordinator, we need a map to know the input file name and according worker
// we need a list of input files that are not allocated yet
// need a list of input files that are already done
//
type Coordinator struct {
	mapManager				*SafeMapTaskManager
	reduceManager           *SafeReduceTaskManager
	mapDone					bool
}

// AssignTask define safe assign task method for map task manager
func (m *SafeMapTaskManager) AssignTask() (int, string) {
	// make sure we add lock at first and unlock at last
	m.lock.Lock()
	defer m.lock.Unlock()

	if len(m.taskQueue) == m.nextId {
		return -1, ""
	}
	ele := m.taskQueue[m.nextId]
	m.nextId = m.nextId + 1

	log.Printf("Map task %v assigned\n", m.nextId - 1)
	return m.nextId - 1, ele
}

func (m *SafeMapTaskManager) FinishTask(taskId int) {
	m.lock.Lock()
	defer m.lock.Unlock()

	log.Printf("Map task %v finished\n", taskId)
	m.taskDone = m.taskDone + 1
}

// MapDone define safe method to check if map work is done
func (m *SafeMapTaskManager) MapDone() bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.taskDone == len(m.taskQueue)
}

// AssignTask define safe assign task method for reduce task manager
func (m *SafeReduceTaskManager) AssignTask() int {
	// make sure we add lock at first and unlock at last
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.nextId == m.reduceNum {
		return -1
	}
	m.nextId = m.nextId + 1

	log.Printf("Reduce task %v assigned\n", m.nextId - 1)
	return m.nextId - 1
}

func (m *SafeReduceTaskManager) FinishTask(taskId int) {
	m.lock.Lock()
	defer m.lock.Unlock()

	log.Printf("Reduce task %v finished\n", taskId)
	m.taskDone = m.taskDone + 1
}

// ReduceDone define safe method to check if reduce work is done
func (m *SafeReduceTaskManager) ReduceDone() bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.taskDone == m.reduceNum
}

// GetTask rpc function for worker to call when try to get task
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.mapDone {
		if c.Done() {
			return errors.New("Mr task done, pls exit")
		}

		// map work is done, we're doing reduce
		i := c.reduceManager.AssignTask()

		reply.IsMap = false
		reply.TaskId = i
		reply.File = "" // we don't need file name for reduce
		reply.Num = len(c.mapManager.taskQueue)
		return nil
	}

	i, file := c.mapManager.AssignTask()

	reply.IsMap = true
	reply.File = file
	reply.TaskId = i
	reply.Num = c.reduceManager.reduceNum
	return nil
}

// FinishTask rpc function for worker to call when task finished
func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	if c.mapDone {
		// reduce now
		c.reduceManager.FinishTask(args.TaskId)
	} else {
		c.mapManager.FinishTask(args.TaskId)
		if c.mapManager.MapDone() {
			c.mapDone = true
		}
	}

	return nil
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
	c.mapManager.taskQueue = files[:]
	c.reduceManager.reduceNum = nReduce
	c.server()
	return &c
}
