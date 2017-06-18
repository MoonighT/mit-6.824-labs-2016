package raft

type InstallSnapshotArgs struct {
	Term             int
	Leaderid         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Offset           int
	Data             []byte
	Done             bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.persister.SaveSnapshot(args.Data)
	// if last include in the logs, retain the logs after it
	DPrintf("%d [install snapshot] lastinclude %d, len(logs) %d logs %v",
		rf.me, args.LastIncludeIndex, len(rf.logs), rf.logs)
	if args.LastIncludeIndex < rf.indexph2l(len(rf.logs)) {
		DPrintf("logs index = %d, last include = %d",
			rf.indexl2ph(args.LastIncludeIndex), rf.lastIncludeIndex)
		if rf.indexl2ph(args.LastIncludeIndex) < 0 {
			return
		}
		if args.LastIncludeTerm == rf.logs[rf.indexl2ph(args.LastIncludeIndex)].Term {
			rf.discardLogs(args.LastIncludeIndex, args.LastIncludeTerm)
			return
		}
	}
	rf.discardLogs(rf.indexph2l(len(rf.logs)-1), rf.logs[len(rf.logs)-1].Term)
	rf.applyChan <- ApplyMsg{
		UseSnapshot: true,
		Snapshot:    args.Data,
	}
}

// install snapshot request
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
