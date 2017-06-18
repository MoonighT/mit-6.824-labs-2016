package raftkv

import (
	"bytes"
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
	Key      string
	Value    string
	Op       string
	Clientid int
	Msgid    int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	processid        map[int]int
	dataStore        map[string]string // map of key -> value
	persister        *raft.Persister
	lastIncludeIndex int
	lastIncludeTerm  int
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[raftkv server] get")
	// Your code here.
	op := Op{
		Key:      args.Key,
		Clientid: args.Clientid,
		Msgid:    args.Msgid,
	}
	kv.mu.Lock()
	_, term, isLeader := kv.rf.Start(op)
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
		// raft apply opid log,
		//client process to this command
		kv.mu.Lock()
		if _, ok := kv.processid[args.Clientid]; !ok {
			kv.mu.Unlock()
			continue
		}
		DPrintf("%d kv get waiting for id %d processid %d",
			kv.me, args.Msgid, kv.processid[args.Clientid])
		if kv.processid[args.Clientid] >= args.Msgid {
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
	DPrintf("[raftkv server] %d put key %s val %s", kv.me, args.Key, args.Value)
	// Your code here.
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		Op:       args.Op,
		Clientid: args.Clientid,
		Msgid:    args.Msgid,
	}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNoKey
		DPrintf("%d finish start is not leader %d", kv.me, index)
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
		if _, ok := kv.processid[args.Clientid]; !ok {
			kv.mu.Unlock()
			continue
		}

		// raft apply opid log,
		//client process to this command
		DPrintf("%d kv get waiting for id %d processid %d",
			kv.me, args.Msgid, kv.processid[args.Clientid])
		if kv.processid[args.Clientid] >= args.Msgid {
			//kv.nextIndex++
			reply.Err = OK
			reply.WrongLeader = false
			// after put, need to check log size
			if kv.persister.RaftStateSize() >= kv.maxraftstate &&
				kv.maxraftstate > 0 {
				// kv build snapshot
				DPrintf("start building snapshot size = %d", kv.persister.RaftStateSize())
				kv.buildSnapshot(index, term)
				// tell raft to discard old log, since kv start from 1
				// while raft log start from 0
				kv.rf.TakeSnapshot(kv.lastIncludeIndex-1, kv.lastIncludeTerm)
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		// not process to this command
		time.Sleep(time.Millisecond * 10)
	}
}

// with kv mutex
func (kv *RaftKV) buildSnapshot(index, term int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(index)
	e.Encode(term)
	e.Encode(kv.dataStore)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
	kv.lastIncludeIndex = index
	kv.lastIncludeTerm = term
}

func (kv *RaftKV) restoreSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	e := gob.NewDecoder(r)
	e.Decode(&kv.lastIncludeIndex)
	e.Decode(&kv.lastIncludeTerm)
	e.Decode(&kv.dataStore)
	return
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
		if msg.UseSnapshot == false {
			op := msg.Command.(Op)
			kv.mu.Lock()
			if _, ok := kv.processid[op.Clientid]; !ok {
				kv.processid[op.Clientid] = op.Msgid
				kv.doOperation(op)
			} else if kv.processid[op.Clientid] < op.Msgid {
				kv.processid[op.Clientid] = op.Msgid
				kv.doOperation(op)
			}
			kv.mu.Unlock()
		} else {
			kv.mu.Lock()
			kv.restoreSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
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
	kv.persister = persister
	// You may need initialization code here.
	kv.processid = make(map[int]int)
	kv.dataStore = make(map[string]string)
	kv.restoreSnapshot(kv.persister.ReadSnapshot())
	go kv.applyMessage()
	return kv
}
