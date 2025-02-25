package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// 当 KVServer 实例不调用 Kill() 时，测试人员调用 Kill()
// 再次需要。为了您的方便，我们提供
// 设置 rf.dead 的代码（不需要锁），
// 和一个 Killed() 方法来测试 rf.dead
// 长时间运行的循环。您也可以添加自己的
// Kill() 的代码。你不需要做任何事
// 关于这个，但可能很方便（例如）
// 抑制 Kill()ed 实例的调试输出。
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] 包含一组端口
// 将通过 Raft 进行协作的服务器
// 形成容错键/值服务。
// me 是当前服务器在servers[]中的索引。
// k/v服务器应该通过底层Raft存储快照
// 实现，应该调用 persister.SaveStateAndSnapshot() 来
// 以原子方式保存 Raft 状态和快照。
// 当 Raft 保存的状态超过 maxraftstate 字节时，k/v 服务器应该快照，
// 为了允许 Raft 对其日志进行垃圾收集。如果 maxraftstate 为 -1，
// 你不需要快照。
// StartKVServer() 必须快速返回，因此它应该启动 goroutine
// 对于任何长时间运行的工作。
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
