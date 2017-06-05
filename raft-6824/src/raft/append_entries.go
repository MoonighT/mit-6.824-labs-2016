package raft

// Append Entries args
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

// Append Entries reply
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = RAFT_FOLLOWER
		rf.votedFor = -1
	}
	DPrintf("update current term to %d, change %d role to follower", args.Term, rf.me)
	rf.heartbeatChan <- struct{}{}
	return
}

// send Append entries request
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
