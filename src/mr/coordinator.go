package mr

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files                 []string
	nReduce               int32
	nMap                  int32
	m                     sync.Mutex
	status                int // the status of the coordinator, TASK_STATUS_MAP/TASK_STATUS_REDUCE/TASK_STATUS_DONE/TASK_STATUS_NIL
	mapTask               []Task
	reduceTask            []Task
	finishedMapTaskCnt    int32
	finishedReduceTaskCnt int32
	todoMapTaskCnt        int32
	todoReduceTaskCnt     int32
	taskIndexQ            chan int32
}

// Your code here -- RPC handlers for the worker to call.

// start a thread that listens for RPCs from worker.go
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

func (c *Coordinator) Schedule(args *GetTaskInfoArgs, reply *GetTaskInfoReply) error {
	workId := args.WorkerId
	sleepTime := 1
	maxSleepTime := 32
	for {
		oldStatus := c.status
		// all task Done
		if oldStatus == TASK_STATUS_DONE {
			reply.Status = TASK_STATUS_DONE
			return nil
		}

		if oldStatus == TASK_STATUS_MAP {
			reply.Status = TASK_STATUS_MAP

			oldToDoCnt := c.todoMapTaskCnt
			newToDoCnt := oldToDoCnt - 1
			if oldToDoCnt > 0 {
				res := atomic.CompareAndSwapInt32(&(c.todoMapTaskCnt), oldToDoCnt, newToDoCnt)
				if res {
					index := <-c.taskIndexQ
					curr := &(c.mapTask[index])
					curr.WorkerId = workId
					curr.Deadline = time.Now().Add(time.Second * 10)
					reply.ATask = curr

					reply.NMap = c.nMap
					reply.NReduce = c.nReduce
					reply.Id = workId
					//log.Printf("++++++++++++ Map task get: [%v], todoMapTask: %v", index, c.todoMapTaskCnt)
					break
				}
			}
		} else if oldStatus == TASK_STATUS_REDUCE {
			reply.Status = TASK_STATUS_REDUCE
			oldIndex := c.todoReduceTaskCnt
			newIndex := oldIndex - 1
			if oldIndex > 0 {
				res := atomic.CompareAndSwapInt32(&(c.todoReduceTaskCnt), oldIndex, newIndex)
				if res {
					index := <-c.taskIndexQ
					curr := &(c.reduceTask[index])
					curr.WorkerId = workId
					curr.Deadline = time.Now().Add(time.Second * 10)
					reply.ATask = curr

					reply.NMap = c.nMap
					reply.NReduce = c.nReduce
					reply.Id = workId

					break
				}
			}
		}

		time.Sleep(time.Duration(time.Microsecond * time.Duration(sleepTime)))
		sleepTime = sleepTime * 2
		if sleepTime > maxSleepTime {
			sleepTime = 1
		}
	}
	return nil
}

func (c *Coordinator) MarkTaskDone(args *PostTaskDoneArgs, reply *PostTaskDoneReply) error {
	taskId := args.TaskId
	status := args.Status
	workerId := args.WorkerId

	if TASK_STATUS_MAP == status {
		task := &c.mapTask[taskId]

		if task.Done {
			//log.Printf("Task[%v] already done by worker [%v].", task.TaskId, task.WorkerId)
			return nil
		}

		if task.WorkerId != workerId {
			reply.StatusCode = STATUS_CODE_ERROR
			//log.Printf("Woker Id does not match, expect [%v], actual [%v]", task.WorkerId, workerId)
		}

		if task.Status != TASK_STATUS_MAP {
			reply.StatusCode = STATUS_CODE_ERROR
			log.Fatalf("Task Status does not match, expect [%v], actual [%v]", TASK_STATUS_MAP, task.Status)
		}

		c.m.Lock()
		c.finishedMapTaskCnt++
		//log.Printf("------------ Map task finished : [%v], finished task number : %v", taskId, c.finishedMapTaskCnt)
		if c.finishedMapTaskCnt == c.nMap {
			c.initReduceTask()
			// wait for worker rename done
			//time.Sleep(time.Second)
			c.status = TASK_STATUS_REDUCE
		}
		c.m.Unlock()

		task.m.Lock()
		task.Done = true
		task.m.Unlock()

	} else if TASK_STATUS_REDUCE == status {
		task := &c.reduceTask[taskId]
		if task.WorkerId != workerId {
			reply.StatusCode = STATUS_CODE_ERROR
			log.Fatalf("Woker Id does not match, expect [%v], actual [%v]", task.WorkerId, workerId)
		}

		if task.Status != TASK_STATUS_REDUCE {
			reply.StatusCode = STATUS_CODE_ERROR
			log.Fatalf("Task Status does not match, expect [%v], actual [%v]", TASK_STATUS_MAP, task.Status)
		}

		c.m.Lock()
		c.finishedReduceTaskCnt++
		if c.finishedReduceTaskCnt == c.nReduce {
			c.status = TASK_STATUS_DONE
		}
		c.m.Unlock()

		task.m.Lock()
		task.Done = true
		task.m.Unlock()
	} else {
		log.Fatalf("Status not know: [%v]", status)
	}

	reply.StatusCode = STATUS_CODE_OK
	return nil
}

func (c *Coordinator) checkCrashedTask() bool {
	for {
		if c.status == TASK_STATUS_DONE {
			return true
		}

		if c.status == TASK_STATUS_MAP {
			for i := 0; i < int(c.nMap); i++ {
				curr := &c.mapTask[i]
				curr.m.Lock()
				if curr.WorkerId != -1 && curr.Done != true && curr.Deadline.Before(time.Now()) {
					c.taskIndexQ <- curr.TaskId
					c.m.Lock()
					c.todoMapTaskCnt++
					//log.Printf("Schedule ************** new task found : [%v], todoMapTask: %v", curr.TaskId, c.todoMapTaskCnt)
					c.m.Unlock()
				}
				curr.m.Unlock()

			}
		} else if c.status == TASK_STATUS_REDUCE {
			for i := 0; i < int(c.nReduce); i++ {
				curr := &c.reduceTask[i]
				curr.m.Lock()
				if curr.WorkerId != -1 && curr.Done != true && curr.Deadline.Before(time.Now()) {
					c.taskIndexQ <- curr.TaskId
					c.m.Lock()
					c.todoReduceTaskCnt++
					c.m.Unlock()
				}
				curr.m.Unlock()
			}
		}

		time.Sleep(time.Second * 10)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for {
		if c.status == TASK_STATUS_DONE {
			time.Sleep(5 * time.Second)
			return true
		}
		time.Sleep(time.Second)
	}
	// Your code here.

	return false
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int32) *Coordinator {
	c := Coordinator{}

	// initialization.
	c.nReduce = nReduce
	c.files = files
	c.nMap = int32(len(files)) // one file one map task
	c.finishedReduceTaskCnt = 0
	c.finishedMapTaskCnt = 0
	c.todoMapTaskCnt = c.nMap
	c.todoReduceTaskCnt = c.nReduce
	c.status = TASK_STATUS_MAP

	c.initMapTask(files)
	// c.initReduceTask()

	go c.checkCrashedTask()
	c.server()
	return &c
}

func (c *Coordinator) initMapTask(files []string) {
	c.taskIndexQ = make(chan int32, c.nMap)

	c.mapTask = make([]Task, c.nMap)
	var i int32 = 0
	for i = 0; i < c.nMap; i++ {
		c.mapTask[i] = Task{
			File:     files[i],
			TaskId:   i,
			WorkerId: -1,
			Deadline: time.Now().Add(time.Second * 10),
			Status:   TASK_STATUS_MAP,
			Done:     false,
		}
		c.taskIndexQ <- i
	}
}

func (c *Coordinator) initReduceTask() {
	close(c.taskIndexQ)
	c.taskIndexQ = make(chan int32, c.nReduce)

	c.reduceTask = make([]Task, c.nReduce)
	var i int32 = 0
	for i = 0; i < c.nReduce; i++ {
		c.reduceTask[i] = Task{
			TaskId:   i,
			WorkerId: -1,
			Deadline: time.Now().Add(time.Second * 10),
			Status:   TASK_STATUS_REDUCE,
			Done:     false,
		}
		c.taskIndexQ <- i
	}
}
