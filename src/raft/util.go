package raft

import "log"

const multi = 1

const TimeOutMin = 1000 * multi
const TimeOutMax = 2000 * multi
const HeartBeatDuration = 50

// Debugging
const Debug = 1


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func logE(format string, a ...interface{}) (n int, err error) {
	log.Fatalf(format, a...)
	return
}

func logD(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Role int
const (
	Leader Role=iota
	Follower
	Candidate
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return "???"
	}
}

type Event int
const (
	AppendEntriesRPC Event =iota
	WinElection
	TermOverdue
	GrantVote
	TimeOut
)

func (e Event)String() string {
	switch e {
	case AppendEntriesRPC :
		return "AppendEntriesRPC"
	case TimeOut:
		return "TimeOut"
	case WinElection:
		return "WinElection"
	case TermOverdue:
		return "TermOverdue"
	case GrantVote:
		return "GrantVote"
	default:
		return "???"
	}
}

type Timer int
const (
	Reset Timer=iota
	TQuit
)

func (t Timer) String() string {
	switch t {
	case Reset:
		return "Reset"
	case TQuit:
		return "TQuit"
	default:
		return "???"
	}
}

type HeartBeat int
const (
	HQuit HeartBeat=iota
)

func (h HeartBeat) String() string {
	switch h {
	case HQuit:
		return "HQuit"
	default:
		return "???"
	}
}
