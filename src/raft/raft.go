package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "mit/labrpc"

// import "bytes"
// import "mit/labgob"



//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role       Role
	serverNum  int
	voteGot    int
	voteNeeded int

	applyCh chan ApplyMsg
	eventCh chan Event
	timerCh chan Timer
	heartBeatCh chan HeartBeat

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	///////////////////// State /////////////////////
	// Persistent state on all servers
	// (Updated on stable storage before responding to RPCs)
	currentTerm int
	votedFor int // -1 represent nil?
	logEntries []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	// (Reinitialized after election)
	nextIndex []int
	matchIndex []int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.isLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type LogEntry struct {
    Term int
    Index int
    Cmd string
}

type AppendEntriesArgs struct {
	Term        int
	LeaderId    int
	PreLogIndex int
	PrevLogTerm int

	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	if args == nil || reply == nil{
		logE("[AppendEntries][Raft_%d] receive : args %+v, reply %+v!", rf.me, args, reply)
		return
	}

	rf.eventCh <- AppendEntriesRPC

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	} else if args.Term < rf.currentTerm { // TODO
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args == nil || reply == nil{
		logE("[RequestVote][Raft_%d] receive : args %+v, reply %+v!", rf.me, args, reply)
		return
	}

	logD("[RequestVote] [Raft_%d] ask vote.\n",args.CandidateId)
	logD("[RequestVote] [Raft_%d] all ready vote to %d =? %d\n", rf.me, rf.votedFor, args.CandidateId)

	votedFor := -1

	if args.Term >=  rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		logLen := len(rf.logEntries)
		if logLen <= 0 || upToDate(args.LastLogIndex, args.LastLogTerm, rf.logEntries[logLen - 1]) {
			votedFor = args.CandidateId
		}
	}

	if votedFor != -1 {
		rf.currentTerm = args.Term // TODO

		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		logD("[RequestVote] [Raft_%d] vote OK to [Raft_%d]\n", rf.me, args.CandidateId)
		return
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		logD("[RequestVote] [Raft_%d] vote NO to [Raft_%d]\n", rf.me, args.CandidateId)
		return
	}


}

func upToDate(lastIndex, lastTerm int, log LogEntry) bool {
	if lastTerm > log.Term {
		return true
	} else if lastTerm < log.Term {
		return false
	} else {
		return lastIndex > log.Index
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	logD("[sendRequestVote] get reply %+v, Got : %d, Needed : %d",
		reply, rf.voteGot, rf.voteNeeded)

	if reply.VoteGranted {
		rf.voteGot ++
		if rf.voteGot >= rf.voteNeeded {
			logD("[sendRequestVote] election success!!! ~~~~~~~~~")
			rf.eventCh <- ElectionSuccess
			//rf.fallInto(Leader)
		}
	}

	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
//// code to set rf.dead (without needing a lock),
//// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	serverNum := len(peers)

	rf.role = Follower
	rf.serverNum = serverNum
	rf.voteNeeded = serverNum / 2 + 1
	rf.eventCh = make(chan Event)
	rf.timerCh = make(chan Timer)
	rf.heartBeatCh = make(chan HeartBeat)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logEntries = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, serverNum)
	rf.matchIndex = make([]int, serverNum)

	if me == serverNum - 1 {
		rf.fallInto(Candidate)
	} else {
		rf.fallInto(Follower)
	}

	return rf
}

func (rf *Raft) isLeader() bool {
	return rf.role == Leader
}

func (rf *Raft) isFollower() bool {
	return rf.role == Follower
}

func (rf *Raft) isCandidate() bool {
	return rf.role == Candidate
}

func (rf *Raft) fallInto(role Role) {
	go func() {
		logD("[fallInto] %v", role)

		stateChanged := rf.role != role

		rf.role = role

		switch  rf.role {
		case Leader:
			rf.leader()
		case Follower:
			rf.follower()
		case Candidate:
			rf.candidate(stateChanged)
		default:
			fmt.Printf("Must Not here")
		}
	}()

}

func (rf *Raft) leader() {
	rf.heartbeat() // has own routine
}

func (rf *Raft) follower() {

	rf.timer(Follower) // has own routine

	for {
		event := <- rf.eventCh
		switch event {
		case AppendEntriesRPC: {
			rf.timerCh <- Reset
		}
		case GrantVote: {
			rf.timerCh <- Reset
		}
		case TimeOut: {
			rf.fallInto(Candidate)
			goto end
		}
		}
	}

end :
	logD("[follower] end")
}

func (rf *Raft) candidate(stateChanged bool) {
	if stateChanged {
		rf.currentTerm ++
	}

	rf.launchElection() // has own routine
	rf.timer(Candidate) // has own routine

	event := <-rf.eventCh
	switch event {
		case AppendEntriesRPC: {
			rf.timerCh <- TQuit
			rf.fallInto(Follower)
		}
		case ElectionSuccess: {
			logD("[candidate] First quit timer")
			rf.timerCh <- TQuit
			logD("[candidate] then for into leader")
			rf.fallInto(Leader)
		}
		case TimeOut: {
			rf.timerCh <- TQuit
			rf.fallInto(Candidate)
		}
	}
}

func (rf *Raft) launchElection() {
	go func() {
		rf.voteGot = 1

		logLen := len(rf.logEntries)
		var lastIndex,lastTerm int
		if logLen <= 0 {
			lastIndex = -1
			lastTerm = -1
		} else {
			lastLog := rf.logEntries[logLen - 1]
			lastIndex = lastLog.Index
			lastTerm = lastLog.Term
		}

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}
		reply := &RequestVoteReply{}

		for i := 0; i < rf.serverNum; i ++ {
			if i == rf.me {
				continue
			}
			rf.sendRequestVote(i, args, reply)
		}
	}()
}

func (rf *Raft) timer(role Role) {
	go func() {
		for {
			to := random(TimeOutMin, TimeOutMax)
			select {
			case tch := <- rf.timerCh: {
				if tch == Reset {
					logD("[timer] [Raft_%d] reset timer!", rf.me)
				} else if tch == TQuit {
					return
				}
			}
			case <- time.After(time.Duration(to) * time.Millisecond): {
				goto timeout
			}
			}
		}

	timeout :
		if role == rf.role {
			rf.eventCh <- TimeOut
		}
	}()
}

func (rf *Raft) heartbeat() {
	go func() {
		for {
			select {
			case hb := <- rf.heartBeatCh:
				if hb == HQuit {
					return
				}
			case <- time.After(HeartBeatDuration * time.Millisecond):
				rf.sendMsg(true)
			}
		}
	}()
}

func (rf *Raft) sendMsg(heartbeat bool) {

	preIndex := rf.commitIndex - 1;// TODO
	var preTerm int
	if preIndex < 0 {
		preTerm = -1
	} else {
		preTerm = rf.logEntries[preIndex].Term
	}

	var args *AppendEntriesArgs
	if heartbeat {
		args = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  preIndex,
			PrevLogTerm:  preTerm,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
	} else {
		args = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  preIndex,
			PrevLogTerm:  preTerm,
			Entries:      rf.logEntries,
			LeaderCommit: rf.commitIndex,
		}
	}

	reply := &AppendEntriesReply{}

	for i := 0; i < rf.serverNum; i ++ {
		if i == rf.me {
			continue
		}
		rf.sendAppendEntries(i, args, reply)
	}
}

func random(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
}