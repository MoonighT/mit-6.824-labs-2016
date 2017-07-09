package shardmaster

import (
	"encoding/gob"
	"sync"
	"time"

	"github.com/MoonighT/mit6824/raft-6824/src/labrpc"
	"github.com/MoonighT/mit6824/raft-6824/src/raft"
)

const JOIN = 0
const LEAVE = 1
const MOVE = 2
const QUERY = 3

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	processid map[int]int
	configs   []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Option  int
	Payload interface{}
}

func (sm *ShardMaster) submit(op Op) (wrongLeader bool, err Err) {
	sm.mu.Lock()
	_, term, isLeader := sm.rf.Start(op)
	sm.mu.Unlock()
	if !isLeader {
		wrongLeader = true
		return
	}
	for {
		currentTerm, currentLeader := sm.rf.GetState()
		if currentTerm != term || !currentLeader {
			wrongLeader = true
			return
		}
		sm.mu.Lock()
		if _, ok := kv.processid[args.Header.Clientid]; !ok {
			kv.mu.Unlock()
			continue
		}
		DPrintf("%d join waiting for id %d processid %d", sm.me,
			args.Header.Msgid, sm.processid[args.Header.Clientid])
		if sm.processid[args.Header.Clientid] >= args.Header.Msgid {
			reply.Err = OK
			reply.WrongLeader = false
			sm.mu.Unlock()
			return
		}
		sm.mu.Unlock()
		// not process to this command
		time.Sleep(time.Millisecond * 10)
	}
	return
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Option:  JOIN,
		Payload: args,
	}
	wrongLeader, err := sm.submit(op)
	reply.WrongLeader = wrongleader
	reply.Err = err
	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Option:  LEAVE,
		Payload: args,
	}
	wrongLeader, err := sm.submit(op)
	reply.WrongLeader = wrongleader
	reply.Err = err
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Option:  MOVE,
		Payload: args,
	}
	wrongLeader, err := sm.submit(op)
	reply.WrongLeader = wrongleader
	reply.Err = err
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Option:  QUERY,
		Payload: args,
	}
	wrongLeader, err := sm.submit(op)
	reply.WrongLeader = wrongleader
	reply.Err = err
	// need to return config also
	return
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (kv *RaftKV) doOperation(op Op) {
	//within mu lock
	DPrintf("[doOperation] me %d op %s, key %s, value %s",
		kv.me, op.Op, op.Key, op.Value)
	// actual logic
	var c Config
	for gid, servers := range args.Servers {

	}
	sm.configs = Append(sm.configs, c)

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

func (sm *ShardMaster) applyMessage() {
	for msg := range sm.applyCh {
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
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.processid = make(map[int]int)
	go sm.applyMessage()
	return sm
}
