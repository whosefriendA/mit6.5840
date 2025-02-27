package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	data   map[string]string
	record sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	if args.MessageType == Report {
		kv.record.Delete(args.MessageID)
		return
	}
	res, ok := kv.record.Load(args.MessageID)
	if ok {
		reply.Value = res.(string)
		return
	}
	kv.mu.Lock()
	old := kv.data[args.Key]
	kv.data[args.Key] = args.Value
	reply.Value = old
	kv.mu.Unlock()

	kv.record.Store(args.MessageID, old)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	if args.MessageType == Report {
		kv.record.Delete(args.MessageID)
		return
	}
	res, ok := kv.record.Load(args.MessageID)
	if ok {
		reply.Value = res.(string)
		return
	}
	kv.mu.Lock()
	old := kv.data[args.Key]
	kv.data[args.Key] = old + args.Value
	reply.Value = old
	kv.mu.Unlock()

	kv.record.Store(args.MessageID, old) // 记录请求
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.data = make(map[string]string)
	return kv
}

//func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
//	// call labgob.Register on structures you want
//	// Go's RPC library to marshall/unmarshall.
//	labgob.Register(Op{})
//
//	kv := new(KVServer)
//	kv.me = me
//	kv.maxraftstate = maxraftstate
//
//	// You may need initialization code here.
//
//	kv.applyCh = make(chan raft.ApplyMsg)
//	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
//
//	// You may need initialization code here.
//	kv.kvMap = make(map[string]string)
//	kv.executeChan = make(map[int]chan Op)
//	kv.lastOptionId = make(map[int64]int)
//	kv.lastIncludeIndex = -1
//	snapshot := persister.ReadSnapshot()
//	kv.mu.Lock()
//	kv.ReadSnapshot(snapshot)
//	kv.mu.Unlock()
//	go kv.applier()
//
//	return kv
//}
//
//func (kv *KVServer) applier() {
//	for !kv.killed() {
//		select {
//		case msg := <-kv.applyCh:
//			if msg.CommandValid {
//				kv.mu.Lock()
//				if msg.CommandIndex <= kv.lastIncludeIndex {
//					kv.mu.Unlock()
//					continue
//				}
//				kv.lastIncludeIndex = msg.CommandIndex
//				command := msg.Command.(Op)
//				DPrintf("%d server applyCh msg = %v\n", kv.me, msg)
//				if command.Option != "Get" && !kv.IsDuplicateRequest(command.ClientId, command.OptionId) {
//
//					switch command.Option {
//
//					case "Append":
//						kv.kvMap[command.Key] += command.Value
//
//					case "Put":
//						kv.kvMap[command.Key] = command.Value
//					}
//
//					kv.lastOptionId[command.ClientId] = command.OptionId
//				}
//				if command.Option == "Get" {
//					command.Value = kv.kvMap[command.Key]
//					DPrintf("%d server applyCh msg = %v, value = %v\n", kv.me, msg, command.Value)
//				}
//
//				if _, isLeader := kv.rf.GetState(); isLeader {
//					kv.GetChan(msg.CommandIndex) <- command
//				}
//				if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
//					DPrintf("%d server PersistSnapshot, index = %v\n", kv.me, msg.CommandIndex)
//					kv.rf.Snapshot(msg.CommandIndex, kv.PersistSnapshot())
//				}
//				kv.mu.Unlock()
//			}
//
//			if msg.SnapshotValid {
//				kv.mu.Lock()
//				DPrintf("%d SnapshotValid: kv.lastIncludeIndex =%d, msg.SnapshotIndex=%d\n", kv.me, kv.lastIncludeIndex, msg.SnapshotIndex)
//				kv.ReadSnapshot(msg.Snapshot)
//				kv.lastIncludeIndex = msg.SnapshotIndex
//
//				kv.mu.Unlock()
//			}
//
//		}
//
//	}
//}
