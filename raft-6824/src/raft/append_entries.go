package raft

import (
	"encoding/json"
)

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// AppendEntries rpc call, append entries from leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	// args term >= rf.currentTerm
	// first append when rf log is empty
	if args.PrevLogIndex < 0 {
		// append all logs
		rf.appendEntryToLog(args)
	} else {
		// check prevlogindex and prevlogterm exist in rf logs
		if len(rf.logs)-1 < args.PrevLogIndex {
			return
		}
		if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			return
		}
		//If an existing entry conflicts with a new one (same index
		//but different terms), delete the existing entry and all that
		//follow it
		rf.appendEntryToLog(args)
	}
	reply.Success = true
	// update commit index
	rf.persist()
	rf.updateCommitIndex(args)
	rf.currentTerm = args.Term
	if rf.role != RAFT_FOLLOWER {
		DPrintf("update current term to %d, change %d role to follower", args.Term, rf.me)
	}
	rf.role = RAFT_FOLLOWER
	rf.votedFor = -1
	rf.heartbeatChan <- struct{}{}
	return
}

func (rf *Raft) appendEntryToLog(args *AppendEntriesArgs) {
	if len(args.Entries) <= 0 {
		return
	}
	baseIndex := args.PrevLogIndex + 1
	for i := range args.Entries {
		if baseIndex+i < len(rf.logs) {
			if args.Entries[i].Term != rf.logs[baseIndex+i].Term {
				//conflict
				rf.logs = append(rf.logs[:baseIndex+i], args.Entries[i:]...)
				break
			}
		} else {
			// all remain not exist
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}
	}
	logsStr, _ := json.Marshal(rf.logs)
	DPrintf("append logs entry %d logs %s", rf.me, string(logsStr))
	return
}

func (rf *Raft) updateCommitIndex(args *AppendEntriesArgs) {
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		}
		DPrintf("%d update commit index %d", rf.me, rf.commitIndex)
	}
}

// send Append entries request
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
