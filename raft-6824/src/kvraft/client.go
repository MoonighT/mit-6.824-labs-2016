package raftkv

import (
	"crypto/rand"
	"encoding/json"
	"math/big"
	"time"

	"github.com/MoonighT/mit6824/raft-6824/src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf("[Get] key %s", key)
	args := &GetArgs{
		Key: key,
	}
	for {
		if ck.lastLeader >= 0 {
			reply := &GetReply{}
			ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", args, reply)
			if ok {
				argStr, _ := json.Marshal(args)
				replyStr, _ := json.Marshal(reply)
				DPrintf("[Get] get from %d arg %s reply %s", ck.lastLeader,
					argStr, replyStr)
				if reply.WrongLeader == false {
					if reply.Err == ErrNoKey {
						return ""
					} else {
						return reply.Value
					}
				}
			}
		}
		for i := range ck.servers {
			reply := &GetReply{}
			ok := ck.servers[i].Call("RaftKV.Get", args, reply)
			if ok {
				argStr, _ := json.Marshal(args)
				replyStr, _ := json.Marshal(reply)
				DPrintf("[Get] get from %d arg %s reply %s", i,
					argStr, replyStr)

				if reply.WrongLeader == false {
					ck.lastLeader = i
					if reply.Err == ErrNoKey {
						return ""
					} else {
						return reply.Value
					}
				}
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("[Set] key %s value %s", key, value)
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	for {
		if ck.lastLeader >= 0 {
			reply := &PutAppendReply{}
			ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", args, reply)
			if ok {
				argStr, _ := json.Marshal(args)
				replyStr, _ := json.Marshal(reply)
				DPrintf("[Set] get from %d arg %s reply %s", ck.lastLeader,
					argStr, replyStr)
				if reply.WrongLeader == false {
					return
				}
			}
		}
		for i := range ck.servers {
			reply := &PutAppendReply{}
			ok := ck.servers[i].Call("RaftKV.PutAppend", args, reply)
			if ok {
				argStr, _ := json.Marshal(args)
				replyStr, _ := json.Marshal(reply)
				DPrintf("[Set] get from %d arg %s reply %s", i,
					argStr, replyStr)
				if reply.WrongLeader == false {
					ck.lastLeader = i
					return
				}
			}
		}
		time.Sleep(time.Millisecond * 10)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
