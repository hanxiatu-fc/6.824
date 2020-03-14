package mr

type Task struct {
	TaskState TaskState
	TaskType TaskType

	FileList []string
	Id int

	NReduce int

	Round int
}

type TaskState int

const (
	Init TaskState = iota
	Queue
	Running
	Success
	Fail
	TimeOut
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const Debug = false