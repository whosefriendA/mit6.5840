package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Option   string
	Key      string
	Value    string
	ClientId int64
	OPID     int
	// 这里是你的定义。
	// 字段名称必须以大写字母开头，
	// 否则 RPC 将中断。
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	data             map[string]string
	lastOpId         map[int64]int
	applyChan        map[int]chan Op
	lastincludeindex int
	maxraftstate     int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Option:   "Get",
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientID,
		OPID:     args.OPID,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.Getchan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		if result.OPID != args.OPID || result.ClientId != args.ClientID {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
		//DPrintf("PutAppend kvMap = %v,replyErr = %v\n", kv.kvMap, reply.Err)
	case <-time.After(100 * time.Millisecond):
		DPrintf("PutAppend Timeout\n")
		reply.Err = Errtimeout
	}
	go func() {
		kv.mu.Lock()
		DPrintf("%d delet chan: %d\n", kv.me, index)
		delete(kv.applyChan, index)
		kv.mu.Unlock()
	}()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Key: args.Key, Option: args.Op, ClientId: args.ClientId, OPID: args.OptionId, Value: args.Value}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.Getchan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		if result.OPID != args.OptionId || result.ClientId != args.ClientId {
			DPrintf("ErrWrongLeader: result.OPID =%d args.OPID=%d result.ClientId=%d  args.ClientId=%d\n", result.OPID, args.OptionId, result.ClientId, args.ClientId)
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK

		}
		//DPrintf("PutAppend kvMap = %v,replyErr = %v\n", kv.kvMap, reply.Err)

	case <-time.After(100 * time.Millisecond):
		DPrintf("PutAppend Timeout\n")
		reply.Err = Errtimeout
	}
	go func() {
		kv.mu.Lock()
		DPrintf("%d delet chan: %d\n", kv.me, index)
		delete(kv.applyChan, index)
		kv.mu.Unlock()
	}()

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

func (kv *KVServer) Getchan(index int) chan Op {
	ch, ok := kv.applyChan[index]
	if !ok {
		kv.applyChan[index] = make(chan Op, 1)
		ch = kv.applyChan[index]
	}
	log.Println("a new chan :", index)
	return ch
}

func (kv *KVServer) IsDuplicateRequest(clientId int64, OptionId int) bool {

	_, ok := kv.lastOpId[clientId]
	if ok {
		return OptionId <= kv.lastOpId[clientId]
	}
	return ok
}

func (kv *KVServer) PersistSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.lastOpId)
	SnapshotBytes := w.Bytes()
	return SnapshotBytes
}

func (kv *KVServer) ReadSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvMap map[string]string
	var lastOptionId map[int64]int
	if d.Decode(&kvMap) != nil || d.Decode(&lastOptionId) != nil {
		fmt.Println("read persist err")
	} else {
		kv.data = kvMap
		kv.lastOpId = lastOptionId
	}
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

	//You may need initialization code here.
	kv.data = make(map[string]string)
	kv.applyChan = make(map[int]chan Op)
	kv.lastOpId = make(map[int64]int)
	kv.lastincludeindex = -1
	snapshot := persister.ReadSnapshot()
	kv.mu.Lock()
	kv.ReadSnapshot(snapshot)
	kv.mu.Unlock()
	go kv.applier()
	return kv
}
func (kv *KVServer) applier() {

}
