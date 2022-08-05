package mr

import (
	"errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const maxExecSeconds = 10

type phase int

const (
	MapPhase    phase = iota
	ReducePhase
	FinishPhase
)

type taskStatus int
const(
	TaskStatusInit taskStatus = iota
	TaskStatusRunning
	TaskStatusFinish
)

type Task struct {
	TaskId     int
	TaskPhase  phase
	TaskStatus taskStatus
	StartTime int64
	FileList   []string
	NReduce    int
	ReduceId int
}

type Coordinator struct {
	// Your definitions here.
	ProcessPhase phase
	MapTask      map[int]*Task
	ReduceTask   map[int]*Task
	NReduce int
	Mutex sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetOneTask(args *GetArgs, reply *GetReply) error {
	if c.ProcessPhase == FinishPhase {
		return errors.New("all task has finished")
	}
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	allFinish := c.allTaskFinished()

	if allFinish {
		if c.ProcessPhase == MapPhase {
			c.ProcessPhase = ReducePhase
		} else if c.ProcessPhase == ReducePhase {
			c.ProcessPhase = FinishPhase
		}
	}

	taskMap := make(map[int]*Task)
	if c.ProcessPhase == MapPhase {
		taskMap = c.MapTask
	} else if c.ProcessPhase == ReducePhase {
		taskMap = c.ReduceTask
	} else {
		return errors.New("invalid phase")
	}

	now := time.Now().Unix()
	findTask := false
	for taskId, task := range taskMap {
		if c.ProcessPhase == task.TaskPhase && task.TaskStatus == TaskStatusInit {
			reply.RealTask = task
			if c.ProcessPhase == MapPhase {
				c.MapTask[taskId].TaskStatus = TaskStatusRunning
				c.MapTask[taskId].StartTime = now
			} else if c.ProcessPhase == ReducePhase {
				c.ReduceTask[taskId].TaskStatus = TaskStatusRunning
				c.ReduceTask[taskId].StartTime = now
			}
			findTask = true
			break
		}
	}
	if !findTask {
		return errors.New("empty task list")
	}

	return nil
}

func (c *Coordinator) ReportTaskResult(args *ReportArgs, reply *ReportReply) error {
	task := args.RealTask
	success := args.Success

	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.ProcessPhase != task.TaskPhase {
		return errors.New("error phase")
	}

	taskId := task.TaskId
	if !success {
		task.TaskStatus = TaskStatusInit
		task.StartTime = 0
		switch c.ProcessPhase {
		case MapPhase:
			c.MapTask[taskId] = task
		case ReducePhase:
			c.ReduceTask[taskId] = task
		}
		return nil
	}

	switch c.ProcessPhase {
	//超时重新派发的任务，即使之前的任务已经完成上报了回来，也不处理了
	case MapPhase:
		if c.MapTask[taskId].StartTime != task.StartTime {
			return nil
		}
	case ReducePhase:
		if c.ReduceTask[taskId].StartTime != task.StartTime {
			return nil
		}
	}

	switch c.ProcessPhase {
	case MapPhase:
		mapTask := c.MapTask[taskId]
		mapTask.TaskStatus = TaskStatusFinish
		c.MapTask[taskId] = mapTask

		for _, filename := range task.FileList {
			filenameSep := strings.Split(filename, "-")
			reduceTaskId, _ := strconv.Atoi(filenameSep[2])
			reduceTask := c.ReduceTask[reduceTaskId]
			reduceTask.FileList = append(reduceTask.FileList, filename)
			c.ReduceTask[reduceTaskId] = reduceTask
		}
	case ReducePhase:
		task.TaskStatus = TaskStatusFinish
		c.ReduceTask[taskId] = task
	}

	return nil
}

func (c *Coordinator) allTaskFinished() bool {
	allFinish := true
	ts := time.Now().Unix()
	var taskMap map[int]*Task
	if c.ProcessPhase == MapPhase {
		taskMap = c.MapTask
	} else if c.ProcessPhase == ReducePhase {
		taskMap = c.ReduceTask
	}
	for id, task := range taskMap {
		if task.TaskStatus != TaskStatusFinish {
			allFinish = false
		}
		//timeout
		if task.TaskStatus == TaskStatusRunning && ts - task.StartTime > maxExecSeconds {
			//fmt.Println("timeout")
			if c.ProcessPhase == MapPhase {
				c.MapTask[id].TaskStatus = TaskStatusInit
				c.MapTask[id].StartTime = 0
			} else if c.ProcessPhase == ReducePhase {
				c.ReduceTask[id].TaskStatus = TaskStatusInit
				c.ReduceTask[id].StartTime = 0
			}
		}
	}

	return allFinish
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := true

	c.Mutex.RLock()
	defer c.Mutex.RUnlock()
	if c.ProcessPhase == FinishPhase {
		return true
	} else if c.ProcessPhase == MapPhase {
		return false
	}

	for _, task := range c.ReduceTask {
		if task.TaskStatus != TaskStatusFinish {
			ret = false
			break
		}
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ProcessPhase: MapPhase,
		NReduce:      nReduce,
		Mutex:        sync.RWMutex{},
	}
	nFiles := len(files)
	c.MapTask = make(map[int]*Task, nFiles)
	c.ReduceTask = make(map[int]*Task, nReduce)

	for i, file := range files {
		task := &Task{
			TaskId:     i,
			TaskPhase:  MapPhase,
			TaskStatus: TaskStatusInit,
			StartTime: 0,
			FileList:   []string{file},
			NReduce:    nReduce,
		}
		c.MapTask[i] = task
	}

	for i := 0; i < nReduce; i++ {
		task := &Task{
			TaskId:     i,
			TaskPhase:  ReducePhase,
			TaskStatus: TaskStatusInit,
			StartTime: 0,
			FileList:   make([]string, 0, nFiles),
			NReduce:    nReduce,
		}
		c.ReduceTask[i] = task
	}

	c.server()
	return &c
}
