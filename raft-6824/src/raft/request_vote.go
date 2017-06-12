package raft

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

func (rf *Raft) checkUpdateToDate(args *RequestVoteArgs) bool {
	// first time  len(logs) == 0  lastindex = -1 lastterm = 0
	if args.LastLogIndex < 0 {
		return true
	}
	lastLogTerm := 0
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	if args.LastLogTerm > lastLogTerm {
		return true
	} else if args.LastLogTerm == lastLogTerm {
		//candidate’s log is at least as up-to-date
		if args.LastLogIndex >= len(rf.logs)-1 {
			return true
		}
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	// new term
	if args.Term > rf.currentTerm {
		DPrintf("[request vote] new term %d vote for %d", args.Term, args.CandidateId)
		//  revert to follower vote for candidate
		rf.role = RAFT_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		if rf.checkUpdateToDate(args) {
			DPrintf("logs of %d is more than %d", args.CandidateId,
				rf.me)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		rf.persist()
		return
	}

	// same term
	switch rf.role {
	case RAFT_FOLLOWER:
		DPrintf("[request vote] same term %d vote for %d", args.Term, args.CandidateId)
		if (rf.votedFor == -1 || rf.votedFor == rf.me) && rf.checkUpdateToDate(args) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.persist()
			return
		}
	case RAFT_CANDIDATE:
	default:
		// no action
	}
	return
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
	return ok
}
