package raftkv

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/MoonighT/mit6824/raft-6824/src/labrpc"
	"github.com/MoonighT/mit6824/raft-6824/src/raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
		fmt.Printf("\n")
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
	Id    int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	processid int
	nextIndex int
	dataStore map[string]string // map of key -> value
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[raftkv server] get")
	// Your code here.
	op := Op{
		Key: args.Key,
	}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()
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
		//client process to this command
		DPrintf("%d kv get waiting for index %d processid %d nextid %d",
			kv.me, index, kv.processid, kv.nextIndex)
		if kv.processid >= index {
			reply.Err = OK
			reply.WrongLeader = false
			reply.Value = kv.dataStore[args.Key]
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		// not process to this command
		time.Sleep(time.Millisecond * 10)
	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[raftkv server] put")
	// Your code here.
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Op:    args.Op,
	}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
		return
	}
	DPrintf("%d finish start index %d", kv.me, index)
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
		//client process to this command
		DPrintf("%d kv put waiting for index %d processid %d nextid %d",
			kv.me, index, kv.processid, kv.nextIndex)
		if kv.processid >= index {
			//kv.nextIndex++
			reply.Err = OK
			reply.WrongLeader = false
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		// not process to this command
		time.Sleep(time.Millisecond * 10)
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

func (kv *RaftKV) doOperation(op Op) {
	//within mu lock
	DPrintf("[doOperation] me %d op %s, key %s, value %s",
		kv.me, op.Op, op.Key, op.Value)
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
		op := msg.Command.(Op)
		kv.mu.Lock()
		if kv.processid < msg.Index {
			kv.processid = msg.Index
		}
		kv.doOperation(op)
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
	kv.processid = 0
	kv.nextIndex = 1
	kv.dataStore = make(map[string]string)
	go kv.applyMessage()
	return kv
}
