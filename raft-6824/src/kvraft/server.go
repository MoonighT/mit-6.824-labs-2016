package raftkv

import (
	"encoding/gob"
	"log"
	"sync"

	"github.com/MoonighT/mit6824/raft-6824/src/labrpc"
	"github.com/MoonighT/mit6824/raft-6824/src/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string
	Id       int
	Clientid int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientopid map[int]int       // map of clientid -> opid
	dataStore  map[string]string // map of key -> value
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := &Op{
		Key:      args.Key,
		Value:    args.Value,
		Op:       args.Op,
		Id:       args.Opid,
		Clientid: args.Clientid,
	}
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
		return
	}
	for {
		currentTerm, currentLeader := kv.rf.GetState()
		if currentTerm != term || !currentLeader {
			// term change or leader change
			reply.WrongLeader = true
			reply.Err = ErrNoKey
			return
		}
		kv.mu.Lock()
		// raft apply opid log,
		if val, ok := kv.clientopid[args.Clientid]; ok {
			//client process to this command
			if val == args.Opid {
				reply.Err = OK
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()
		// not process to this command
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) doOperation(op *Op) {
	//within mu lock
	switch op.Op {
	case "Put":
		kv.dataStore[op.Key] = op.Value
	case "Append":
		if _, ok := kv.dataStore[op.Key]; ok {
			kv.dataStore[op.Key] = kv.dataStore[op.Key] + op.Value
		} else {
			kv.dataStore[op.Key] = op.Value
		}
	}
}

func (kv *RaftKV) applyMessage() {
	for msg := range kv.applyCh {
		cmd := msg.Command
		op := cmd.(*Op)
		kv.mu.Lock()
		if val, ok := kv.clientopid[op.Clientid]; ok {
			if val < op.Id {
				// new command
				kv.clientopid[op.Clientid] = op.Id
				kv.doOperation(op)
			}
		} else {
			kv.clientopid[op.Clientid] = op.Id
			kv.doOperation(op)
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
