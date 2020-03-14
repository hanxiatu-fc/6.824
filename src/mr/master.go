package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	mapTasks map[int]*Task
	reduceTasks map[int]*Task

	doneMaps    int
	doneReduces int

	mapNum int
	reduceNum int

	ch chan *Task
	mux sync.Mutex

	phase TaskType

	fileList []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
func (m *Master) GetTask(request *Request, reply *Reply) error {
	//m.mux.Lock()
	//defer m.mux.Unlock()

	task := <-m.ch

	if task.TaskType == Map {
		m.mapTasks[task.Id].TaskState = Running
	} else if task.TaskType == Reduce {
		m.reduceTasks[task.Id].TaskState = Running
	}

	reply.Task = task

	return nil
}

func (m *Master) ReportTask(request *Request, reply *Reply) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	task := request.Task

	if task.TaskType == Map {
		if task.Round == m.mapTasks[task.Id].Round && task.TaskState == Success {
			m.doneMaps++
			if Debug {
				fmt.Printf("map done num %d with task %d; round(%d -> %d)\n",
					m.doneMaps, task.Id, task.Round, m.mapTasks[task.Id].Round)
			}
			m.mapTasks[task.Id].TaskState = Success

			reply.Task = task
		} else {
			if Debug {
				fmt.Printf("map not accept num %d with task %d; round(%d -> %d)\n",
					m.doneMaps, task.Id, task.Round, m.mapTasks[task.Id].Round)
			}
			reply.Task = nil  // master not accept
		}
	} else if task.TaskType == Reduce {
		if task.Round == m.reduceTasks[task.Id].Round && task.TaskState == Success {
			m.doneReduces++
			if Debug {
				fmt.Printf("reduce done num %d with task %d; round(%d -> %d) \n",
					m.doneReduces, task.Id, task.Round == m.reduceTasks[task.Id].Round)
			}
			m.reduceTasks[task.Id].TaskState = Success

			reply.Task = task
		} else {
			if Debug {
				fmt.Printf("reduce not accept num %d with task %d; round(%d -> %d)\n",
					m.doneMaps, task.Id, task.Round, m.mapTasks[task.Id].Round)
			}
			reply.Task = nil // master not accept
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove(SocketFile)
	l, e := net.Listen("unix", SocketFile)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	if Debug {
		fmt.Printf("    DONE(%d, %d), need(%d, %d) \n",
			m.doneMaps, m.doneReduces, m.mapNum, m.reduceNum)
	}

	done := m.doneReduces == m.reduceNum

	if m.doneReduces > m.reduceNum {
		log.Fatalf("ERROR!! done : %d, has : %d", m.doneReduces, m.reduceNum)
	}

	if done {
		os.Create("mr-done")
	}

	//if done {//clean TODO
	//	dir, _ := os.Getwd()
	//	dirRead, _ := os.Open(dir)
	//	dirFiles, _ := dirRead.Readdir(0)
	//
	//	// Loop over the directory's files.
	//	for index := range(dirFiles) {
	//		err := os.Remove(path.Join(dir, dirFiles[index].Name()))
	//		fmt.Printf("remove ==> %d , error : %v \n", path.Join(dir, dirFiles[index].Name()), err)
	//	}
	//}

	return done
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapTasks:    make(map[int]*Task),
		reduceTasks: make(map[int]*Task),
		doneMaps:    0,
		doneReduces: 0,
		mapNum:		 len(files),
		reduceNum:   nReduce,
		ch:          make(chan *Task),
		phase: 		 Map,
		fileList:    files,
	}

	m.server()
	go m.schedule(len(files), nReduce)
	go m.watch()

	return &m
}

func (m *Master) schedule(nMap int, nReduce int) {
	// schedule map tasks
	for i := 0; i < nMap; i ++ {
		if Debug {
			fmt.Printf("schedule map task : %d \n", i)
		}
		task := m.getTask(i, Map)
		task.TaskState = Queue
		task.Round = 1
		m.ch <- task
	}

	// wait all map tasks done
	for m.doneMaps < m.mapNum {
		time.Sleep(time.Second)
	}

	// set phase state after all reduce task are inited
	m.phase = Reduce

	// schedule reduce tasks
	for i := 0; i < nReduce; i ++ {
		if Debug {
			fmt.Printf("schedule map task : %d \n", i)
		}
		task := m.getTask(i, Reduce)
		task.TaskState = Queue
		task.Round = 1
		m.ch <- task
	}
}



func (m *Master) watch() {

	for !m.Done() {
		if m.phase == Map {
			// update
			newMapTasks := make(map[int]*Task)
			for i, task := range m.mapTasks {
				if task != nil && task.TaskState == Running {
					newMapTasks[i] = m.getTask(i, Map)
					if Debug {
						fmt.Printf("task(type : %d, id : %d, round : %d) timeout! \n",
							newMapTasks[i].TaskType, newMapTasks[i].Id, newMapTasks[i].Round)
					}
				} else {
					newMapTasks[i] = m.mapTasks[i]
				}
			}
			m.mapTasks = newMapTasks

			// schedule
			for _, task := range m.mapTasks {
				if task.TaskState == Init && task.Round >= 1 {
					if Debug {
						fmt.Printf("re schedule map task : %d \n", task.Id)
					}
					task.TaskState = Queue
					task.Round ++
					m.ch <- task
				}
			}
		} else if m.phase == Reduce {
			// update
			newReduceTasks := make(map[int]*Task)
			for i, task := range m.reduceTasks {
				if task != nil && task.TaskState == Running {
					newReduceTasks[i] = m.getTask(i, Reduce)
					if Debug {
						fmt.Printf("task(type : %d, id : %d, round : %d) timeout! \n",
							newReduceTasks[i].TaskType, newReduceTasks[i].Id, newReduceTasks[i].Round)
					}
				} else {
					newReduceTasks[i] = m.reduceTasks[i]
				}
			}
			m.reduceTasks = newReduceTasks

			// schedule
			for _, task := range m.reduceTasks {
				if task.TaskState == Init  && task.Round >= 1 {
					if Debug {
						fmt.Printf("re schedule map task : %d \n", task.Id)
					}
					task.TaskState = Queue
					task.Round ++
					m.ch <- task
				}
			}
		}

		time.Sleep(5 * time.Second)
	}

}

func (m *Master) getTask(i int, ty TaskType) *Task{
	if ty == Map {
		var task *Task
		if m.mapTasks[i] == nil {
			task = new(Task)
			task.TaskState = Init
			task.TaskType = Map
			task.Id = i
			task.NReduce = m.reduceNum
			task.FileList = []string{m.fileList[i]}
		} else {
			task = cloneTask(m.mapTasks[i])
		}
		m.mapTasks[i] = task

		return task
	} else if ty == Reduce {
		var task *Task
		if m.reduceTasks[i] == nil {
			task = new(Task)
			task.TaskState = Init
			task.TaskType = Reduce
			task.NReduce = m.reduceNum
			task.Id = i

			for j:=0; j < m.mapNum; j ++ {
				filename := fmt.Sprintf("mr-%d-%d", j, i)
				if _, err := os.Stat(filename); err == nil {
					task.FileList = append(task.FileList,
						fmt.Sprintf("mr-%d-%d", j, i))
				} else {
					if Debug {
						fmt.Printf("*** %s not existed ***\n", filename)
					}
				}
			}
		} else {
			task = cloneTask(m.reduceTasks[i])
		}
		m.reduceTasks[i] = task

		return task
	}

	log.Fatal("Must not be here!!")

	return nil
}


func cloneTask(t *Task) *Task {
	task := new(Task)
	task.TaskState = Init
	task.TaskType = t.TaskType
	task.Id = t.Id
	task.NReduce = t.NReduce
	task.FileList = t.FileList
	task.Round = t.Round

	return task
}
