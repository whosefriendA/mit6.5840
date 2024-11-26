package raft

// 这是 Raft 向服务（或测试器）暴露的 API 的大纲。
// 具体每个函数的详细信息见下方的注释说明。

// rf = Make(...)
//   创建一个新的 Raft 服务器。
// rf.Start(command interface{}) (index, term, isLeader)
//   开始对一个新的日志条目达成一致
// rf.GetState() (term, isLeader)
//   查询 Raft 当前的任期，并判断它是否认为自己是 Leader
// ApplyMsg
//   每当一个新条目提交到日志时，
//   每个 Raft 节点都应在相同的服务器中将 ApplyMsg 发送给服务（或测试器）。

import (
	"fmt"
	"log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// 当每个 Raft 节点意识到连续的日志条目已被提交时，
// 该节点应通过传递给 Make() 的 applyCh 向同一服务器上的服务（或测试器）发送一个 ApplyMsg。
// 将 ApplyMsg 的 CommandValid 设置为 true，以表明其中包含一个新提交的日志条目。

// 在第 3D 部分，你可能需要通过 applyCh 发送其他类型的消息（例如：快照），
// 但对于这些用途，应将 CommandValid 设置为 false。

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Term         int
	Command      interface{}
	CommandIndex int
}
type state int

const (
	Leader state = iota
	Follower
	Candidate
)

// 一个实现单个 Raft 节点的 Go 对象。
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	currentTerm   int                 //服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	votedFor      int                 //当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	log           []LogEntry          //日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）
	commitIndex   int                 //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied   int                 //已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	nextIndex     []int               //对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex    []int               //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	rfstate       state               //本状态机类型
	lastheartbeat time.Time           //选举定时器
	applyChan     chan ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	if rf.rfstate == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// 将 Raft 的持久化状态保存到稳定存储中，
// 这样在崩溃和重启后可以恢复状态。
// 关于哪些状态需要持久化，请参阅论文的图 2 描述。
// 在实现快照之前，应将 nil 作为第二个参数传递给 persister.Save()。
// 实现快照后，应传递当前的快照（如果尚未创建快照，则传递 nil）。
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// 服务表示它已经创建了一个快照，包含了直到并包括索引的所有信息。
// 这意味着服务不再需要该索引之前的日志。Raft 应该尽可能地修剪其日志。

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) broadcastRequestVoteEntries() {
	if rf.killed() {
		return
	}
	log.Printf("vote begin")
	rf.mu.Lock()
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	if len(rf.log) > 0 {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	} else {
		args.LastLogIndex = -1
		args.LastLogTerm = -1
	}
	votemap := make(map[int]bool)
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if rf.rfstate != Candidate {
			return
		}
		if i != rf.me {
			go func(server int, args RequestVoteArgs) {
				var reply RequestVoteReply
				if rf.sendRequestVote(server, &args, &reply) {
					rf.handleRequestVoteReply(server, votemap, &args, &reply)
				}
			}(i, args)
		}
	}
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.LastLogIndex >= 0 && args.LastLogTerm < rf.log[args.LastLogIndex].Term && args.LastLogIndex < len(rf.log)-1 {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
	}
}
func (rf *Raft) handleRequestVoteReply(server int, votemap map[int]bool, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term >= rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.rfstate = Follower
		return
	}
	if reply.VoteGranted {
		votemap[server] = true
	}
	num := 1
	for _, n := range votemap {
		if n {
			num++
		}
	}
	//log.Printf("len of peers %d num %d", len(rf.peers), num)
	if num >= len(rf.peers)/2 && rf.rfstate == Candidate {
		log.Printf("new leader was voted")
		rf.rfstate = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.nextIndex[i] = len(rf.log)
			}
		}
		go rf.broadcastAppendEntries()
	}
}

// 向服务器发送 RequestVote RPC 的示例代码。
// server 是 rf.peers[] 中目标服务器的索引。
// 需要将 RPC 参数存储在 args 中。
// *reply 会被填充为 RPC 的回复结果，因此调用者应传递 &reply。
// 传递给 Call() 的 args 和 reply 类型必须与处理函数声明的参数类型相同（包括是否为指针）。

// labrpc 包模拟了一个存在丢包的网络，服务器可能无法访问，且请求和回复可能会丢失。
// Call() 发送请求并等待回复。如果在超时时间内收到回复，Call() 返回 true；
// 否则返回 false。因此，Call() 有时可能会等待一段时间。
// 返回 false 的原因可能是服务器宕机、服务器不可达、请求丢失或回复丢失。

// Call() 保证会返回（可能会有延迟），*除非*服务器端的处理函数未返回。
// 因此，调用者不需要为 Call() 实现额外的超时控制。

// 更多详细信息，请参阅 ../labrpc/labrpc.go 中的注释。

// 如果 RPC 有问题，请检查传递的结构体字段名是否首字母大写，
// 并确保调用者使用 & 传递回复结构体的地址，而不是直接传递结构体。
func (rf *Raft) broadcastAppendEntries() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		//log.Printf("heart beat")
		if rf.rfstate != Leader {
			return
		}
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = 0
		args.PrevLogTerm = 0
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				if len(rf.log) > 0 {
					rf.mu.Lock()
					//log.Printf("broad nextindex %d", rf.nextIndex[i])
					nextIndex := rf.nextIndex[i]
					if nextIndex >= 0 {
						args.PrevLogIndex = nextIndex - 1
						//log.Printf("broad Prevlogindex %d", args.PrevLogIndex)
						if args.PrevLogIndex > 0 {
							args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
						}
					}
					if nextIndex < len(rf.log) {
						//log.Printf("broad nextindex %d", nextIndex)
						args.Entries = rf.log[nextIndex:]
					} else {
						args.Entries = nil
					}
					rf.mu.Unlock()
				}
				go func(server int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					if rf.sendAppendEntries(server, &args, &reply) {
						rf.handleAppendEntriesReply(server, &args, &reply)
					}
				}(i, args)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//领导人变为跟随者
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.rfstate = Follower
		return
	}
	if reply.Success && args.PrevLogIndex >= -1 {
		//log.Printf("handle nextindex  %d    %d", server, rf.nextIndex[server])
		//log.Printf("len %d", len(args.Entries))
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// 尝试更新Leader的commitIndex
		log.Printf("commitindex %d", rf.commitIndex)
		rf.updateCommitIndex()
		log.Printf("commitindex %d", rf.commitIndex)
	} else if !reply.Success && args.PrevLogIndex >= 0 {
		if rf.nextIndex[server] >= 1 {
			rf.nextIndex[server] = rf.nextIndex[server] - 1
		} else {
			rf.nextIndex[server] = 1
		}
	}
}
func (rf *Raft) updateCommitIndex() {
	n := len(rf.log)
	for i := rf.commitIndex + 1; i < n; i++ {
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.lastheartbeat = time.Now()
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.rfstate = Follower
	}
	if args.PrevLogIndex >= len(rf.log) || args.PrevLogIndex < 0 || (args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if len(args.Entries) > 0 {
		conflictIndex := args.PrevLogIndex + 1
		if conflictIndex < len(rf.log) {
			rf.log = rf.log[:conflictIndex]
		}
		rf.log = append(rf.log, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.applyChan <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	for i, v := range rf.log {
		fmt.Printf("log  %d   is   %d", i, v)
	}
}

// 使用 Raft 的服务（例如一个键值对服务器）希望开始对下一个要追加到 Raft 日志的命令达成一致。
// 如果当前服务器不是 Leader，则返回 false。否则，开始达成一致并立即返回。
// 该命令是否会提交到 Raft 日志中并无保证，因为 Leader 可能会失败或在选举中失去领导地位。
// 即使当前的 Raft 实例被终止，这个函数也应当平稳地返回。

// 第一个返回值是该命令若被提交时将在日志中的索引。
// 第二个返回值是当前的任期。
// 第三个返回值为 true 表示该服务器认为自己是 Leader。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.rfstate == Leader {
		isLeader = true
	}
	if isLeader {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.nextIndex[i] = len(rf.log)
			}
		}
		newlog := LogEntry{
			Term:         rf.currentTerm,
			Command:      command,
			CommandIndex: len(rf.log),
		}
		rf.log = append(rf.log, newlog)
		term = rf.currentTerm
		index = len(rf.log) - 1
		log.Printf("index %d", index)
	}
	// Your code here (3B).
	return index, term, isLeader
}

// 测试者在每个测试后不会终止 Raft 创建的 goroutine，
// 但会调用 Kill() 方法。你的代码可以使用 killed() 来检查
// Kill() 是否已被调用。使用 atomic 避免了对锁的需求。

// 问题在于长期运行的 goroutine 会占用内存，并可能消耗
// CPU 时间，导致后续测试失败并产生令人困惑的调试输出。
// 任何长期运行的 goroutine 都应该调用 killed() 来检查
// 是否应该停止。

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		//log.Printf("current term: %d rfstate : %d", rf.currentTerm, rf.rfstate)
		ms := 50 + (rand.Int63() % 300)
		timeout := (time.Duration(ms) + 2000) * time.Millisecond
		if time.Since(rf.lastheartbeat) > timeout {
			rf.mu.Lock()
			if rf.rfstate == Follower {
				rf.rfstate = Candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				go rf.broadcastRequestVoteEntries()
			}
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.

		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 服务或测试者希望创建一个 Raft 服务器。所有 Raft 服务器的端口（包括当前服务器）都在 peers[] 中。
// 当前服务器的端口是 peers[me]。所有服务器的 peers[] 数组顺序一致。
// persister 是一个用于保存当前服务器持久化状态的地方，并且最初保存了最近的已保存状态（如果有的话）。
// applyCh 是一个通道，测试者或服务期望 Raft 通过该通道发送 ApplyMsg 消息。
// Make() 必须迅速返回，因此它应该为任何长期运行的工作启动 goroutine。

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyChan = applyCh
	rf.rfstate = Follower
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.lastApplied = 0
	rf.lastheartbeat = time.Now()
	// 从崩溃前持久化的状态初始化
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
