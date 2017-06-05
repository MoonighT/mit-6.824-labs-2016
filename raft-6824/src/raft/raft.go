package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"

	"github.com/MoonighT/mit6824/raft-6824/src/labrpc"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//

type RaftRole int

const (
	RAFT_UNKNOWN   RaftRole = 0
	RAFT_LEADER    RaftRole = 1
	RAFT_FOLLOWER  RaftRole = 2
	RAFT_CANDIDATE RaftRole = 3
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        RaftRole
	currentTerm int
	votedFor    int

	// heartBeat get chan to clear election timeout
	heartbeatChan chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.role == RAFT_LEADER)
	return term, isleader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
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
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) election() {
	rf.currentTerm += 1
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	lock := sync.Mutex{}
	voted := 1 // need to include itself
	for i, _ := range rf.peers {
		go func(index int) {
			if index == rf.me {
				return
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(index, args, reply)
			if !ok {
				DPrintf("rpc error")
				return
			}
			DPrintf("[election] %d role %d sendRequestVote args %v, reply %v, ok %v",
				rf.me, rf.role, args, reply, ok)
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				lock.Lock()
				voted += 1
				if voted > len(rf.peers)/2 {
					//success
					DPrintf("[election] one server win %d", rf.me)
					rf.role = RAFT_LEADER
				}
				lock.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) electionCheck() {
	for {
		random := rand.Int()%500 + 500 // millisecond
		timeout := time.Duration(int64(random)) * time.Millisecond
		select {
		case <-rf.heartbeatChan:
			break
		case <-time.After(timeout):
			rf.mu.Lock()
			switch rf.role {
			case RAFT_FOLLOWER:
				// election vote for rf self
				rf.role = RAFT_CANDIDATE
				DPrintf("follower start election %d role %d", rf.me, rf.role)
				rf.election()
			case RAFT_CANDIDATE:
				DPrintf("candidate start election %d role %d", rf.me, rf.role)
				rf.election()
			default:
				// no action
			}
			rf.mu.Unlock()
		}
	}

}

func (rf *Raft) heartBeat() {
	num := 1
	lock := sync.Mutex{}
	for i, _ := range rf.peers {
		go func(index int) {
			if index == rf.me {
				return
			}
			args := &AppendEntriesArgs{
				Term: rf.currentTerm,
			}
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(index, args, reply)
			DPrintf("[heartbeat] %d, role %d sendAppendEntries args %v, reply %v, ok %v",
				rf.me, rf.role, args, reply, ok)
			if ok {
				lock.Lock()
				num += 1
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = RAFT_FOLLOWER
				}
				lock.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) heartBeatCheck(timeout time.Duration) {
	for {
		select {
		case <-time.After(timeout):
			rf.mu.Lock()
			switch rf.role {
			case RAFT_LEADER:
				DPrintf("heartbeat check %d role %d", rf.me, rf.role)
				rf.heartBeat()
			default:
			}
			rf.mu.Unlock()
		}
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.role = RAFT_FOLLOWER
	rf.heartbeatChan = make(chan struct{})
	go rf.electionCheck()
	go rf.heartBeatCheck(time.Duration(200) * time.Millisecond)
	// two ticker  one is for election  none leader
	// only leader one is heartbeat, if recieve clear election ticker

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
