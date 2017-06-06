package raft

// Append Entries args
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

// Append Entries reply
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	// first append when rf log is empty
	if args.PrevLogIndex < 0 {
		// append all logs and update commit index
	}
	// args term >= rf.currentTerm
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

	rf.currentTerm = args.Term
	rf.role = RAFT_FOLLOWER
	rf.votedFor = -1
	DPrintf("update current term to %d, change %d role to follower", args.Term, rf.me)
	rf.heartbeatChan <- struct{}{}
	return
}

func (rf *Raft) appendEntryToLog(args *AppendEntriesArgs) {
	if len(args.Entries) <= 0 {
		return
	}
	baseIndex := args.PrevLogIndex + 1
	for i, _ := range args.Entries {
		if baseIndex+i < len(rf.logs) {
			if args.Entries[baseIndex+i].Term != rf.logs[baseIndex+i].Term {
				//conflict
				rf.logs = append(rf.logs[:baseIndex+i], args.Entries[i:]...)
				break
			} else {
				rf.logs = append(rf.logs, args.Entries[i])
			}
		} else {
			// all remain not exist
			rf.logs = append(rf.logs, args.Entries[i:])
			break
		}
	}
	return
}

func (rf *Raft) updateCommitIndex(args *AppendEntriesArgs) {
	if args.LeaderCommit > rf.commitIndex {
		//TODO check this minus 1
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}
}

// send Append entries request
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
