package raft

import "log"

const TimeOutMin = 1000
const TimeOutMax = 2000
const HeartBeatDuration = 500

// Debugging
const Debug = 0


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
	GrantVote
	ElectionSuccess
	TimeOut
)

func (e Event)String() string {
	switch e {
	case AppendEntriesRPC:
		return "AppendEntriesRPC"
	case GrantVote:
		return "GrantVote"
	case ElectionSuccess:
		return "ElectionSuccess"
	case TimeOut:
		return "TimeOut"
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
