package mr

import (
	"fmt"
	"io/ioutil"
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

var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int
	TaskId            int
	DistPhase         Phase
	ReduceTaskChannel chan *Task
	MapTaskChannel    chan *Task
	taskMetaHolder    TaskMetaHolder
	files             []string
}

type TaskMetaInfo struct {
	state     State
	StartTime time.Time
	TaskAdr   *Task
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	c.makeMapTasks(files)

	c.server()
	return &c
}

func (c *Coordinator) makeMapTasks(files []string) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{v},
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		//fmt.Println("make a map task :", &task)
		c.MapTaskChannel <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i),
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		//fmt.Println("make a reduce task :", &task)
		c.ReduceTaskChannel <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		//fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel
				//fmt.Printf("poll-Map-taskid[ %d ]\n", reply.TaskId)
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("Map-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel
				//fmt.Printf("poll-Reduce-taskid[ %d ]\n", reply.TaskId)
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("Reduce-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{

			reply.TaskType = ExitTask
		}
	default:
		panic("The phase undefined ! ! !")

	}
	return nil
}
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}
func (t *TaskMetaHolder) checkTaskDone() bool {

	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}

	}
	log.Printf("map tasks  are finished %d/%d, reduce task are finished %d/%d \n",
		mapDoneNum, mapDoneNum+mapUnDoneNum, reduceDoneNum, reduceDoneNum+reduceUnDoneNum)
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false

}
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}
func (c *Coordinator) generateTaskId() int {

	res := c.TaskId
	c.TaskId++
	return res
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	newReply := ExampleReply{
		1000,
	}
	reply = &newReply
	fmt.Println("example")
	return nil
}

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
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}
		break
	case ReduceTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}
		break

	default:
		panic("The task type undefined ! ! !")
	}
	return nil

}
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}

}
