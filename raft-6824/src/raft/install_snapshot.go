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
	rf.persister.SaveSnapshot(args.data)
	// if last include in the logs, retain the logs after it
	if args.LastIncludeIndex < rf.indexph2l(len(rf.logs)) {
		if args.LastIncludeTerm == rf.logs[rf.indexl2ph(args.LastIncludeIndex)] {
			rf.discard(args.LastIncludeIndex, args.LastIncludeTerm)
			return
		}
	}
	rf.discardLogs(rf.indexph2l(len(rf.logs)-1), rf.logs[len(rf.logs)-1])
	rf.applyChan <- ApplyMsg{
		UseSnapshot: true,
		Snapshot:    args.data,
	}
}

// install snapshot request
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
