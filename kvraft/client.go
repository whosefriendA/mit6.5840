package kvraft

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Leader   int //集群的leader
	ClientId int64
	OptionId int
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
	ck.Leader = 0
	ck.ClientId = nrand()
	ck.OptionId = 0

	return ck
}

// fetch the current Value for a Key.
// returns "" if the Key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf("触发Get\n")
	args := &GetArgs{Key: key, ClientID: ck.ClientId, OPID: ck.OptionId}
	reply := &GetReply{}
	for {
		ok := ck.servers[ck.Leader].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == Errtimeout {
			ck.Leader = (ck.Leader + 1) % len(ck.servers)
			reply.Err = ""
		} else {
			ck.OptionId++
			break
		}
	}
	if reply.Err == ErrNoKey {
		return ""
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.ClientId, OptionId: ck.OptionId}
	reply := &PutAppendReply{}
	for {
		ok := ck.servers[ck.Leader].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == Errtimeout {
			ck.Leader = (ck.Leader + 1) % len(ck.servers)
			reply.Err = ""
		} else {

			ck.OptionId++
			break
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
