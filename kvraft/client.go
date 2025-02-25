package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	return ck
}

// 获取键的当前值。
// 如果键不存在则返回“”。
// 面对所有其他错误，不断尝试。
//
// 您可以使用如下代码发送 RPC：
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// args和reply的类型（包括是否是指针）
// 必须与 RPC 处理函数的声明类型匹配
// 参数。并且回复必须作为指针传递。
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key: key,
	}
	reply := &GetReply{}

	for !ck.servers[].Call("KVServer.Get", args, reply) {
	} // keep trying forever
	return reply.Value
	// You will have to modify this function.
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
	MessageID := nrand()
	arg := &PutAppendArgs{
		Key:         key,
		Value:       value,
		MessageID:   MessageID,
		MessageType: Modify,
	}
	reply := &PutAppendReply{}
	for !ck.server.Call("KVServer."+op, arg, reply) {
	}
	arg = &PutAppendArgs{
		MessageType: Report,
		MessageID:   MessageID,
	}
	for !ck.server.Call("KVServer."+op, arg, reply) {
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
