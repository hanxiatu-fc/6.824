package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const SocketFile = "/tmp/6.824-mr"
//const SocketFile = "mr-socket"

type Request struct {
	Task *Task
}

type Reply struct {
	Task *Task
}

// Add your RPC definitions here.

