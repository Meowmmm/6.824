package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should sendMsg an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"../labrpc"
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

const LEADER = 1
const FOLLOWER = 2
const CANDIDATE = 3
const LEADER_HEARTBEAT = math.MinInt32
const LEADER_BLANK_ENTRY = math.MinInt32 + 1

const HEARTBEAT_INTERVAL = 50
const VORE_BASE = 400
const VOTE_INTERVAL = 200
const NET_TIMEOUT = 50

const MsgRingSize = 1000
const BROADCAST = -1
const NO_MSG_ID = -1

const MSG_ASK_VOTE = 0
const MSG_HERATBEAT = 1
const MSG_BROADCAST_APPLY_ENTRY = 2
const MSG_APPLY_ENTRY = 3
const MSG_STOP = 4

var msgId = 0
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should sendMsg an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to sendMsg other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Log  interface{}
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	rwLock    sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// TODO Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// need persist
	currentTerm  int
	votedTerm    int
	voteEachTerm []int
	logs         []LogEntry

	/*
	 * log[0]默认已经提交了
	 * 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	 */
	commitIndex int
	/*
	 * 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	 */
	lastApplied int

	/*
	 *  领导人针对每一个跟随者维护了一个nextIndex
	 *  这表示下一个需要发送给跟随者的日志条目的索引地址
	 *  当一个领导人刚获得权力的时候，他初始化所有的 nextIndex 值为自己的最后一条日志的 index 加 1。
	 *  如果一个跟随者的日志和领导人不一致，那么在下一次的附加日志 RPC 时的一致性检查就会失败。
	 *  在被跟随者拒绝之后，领导人就会减小 nextIndex 值并进行重试。
	 *  最终 nextIndex 会在某个位置使得领导人和跟随者的日志达成一致。
	 *  当这种情况发生，附加日志 RPC 就会成功，这时就会把跟随者冲突的日志条目全部删除并且加上领导人的日志。
	 */
	nextIndex  []int
	/*
	 * 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	 */
	matchIndex []int

	// my add
	status int

	heartBeatChan chan ApplyMsg
	applyCh chan ApplyMsg
	stopChan chan bool

	appendHistoryFlag []bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// TODO Your code here (2A).
	rf.rwLock.RLock()
	defer rf.rwLock.RUnlock()
	return rf.currentTerm, rf.status == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.rwLock.Lock()
	defer rf.rwLock.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.lastApplied)
	e.Encode(rf.commitIndex)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.rwLock.Lock()
	defer rf.rwLock.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var commitIndex int
	var term int
	var logs []LogEntry
	if  d.Decode(&lastApplied) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&logs) != nil {
	  fmt.Printf("raft %d into readPersist but decode err\n", rf.me)
	} else {
	  rf.logs = logs
	  rf.lastApplied = lastApplied
	  rf.commitIndex = commitIndex
	  rf.currentTerm = term
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestAppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Log          interface{}
 	LeaderCommit int
	LogId        int
	LogTerm 	int
}

type RequestAppendEntryReply struct {
	Term        int
	Success     bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// fmt.Printf("[RequestVote] %d ask %d for vote in term %d, args: %+v, rf.Term: %d, rf.commitIndex %d, rf.lastLog: %+v\n",
	// 	args.CandidateId, rf.me, args.Term, args, rf.currentTerm, rf.commitIndex, rf.logs[rf.commitIndex])

	// TODO Your code here (2A, 2B).
	// 收到了一个过去任期的投票请求，直接丢掉
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		// fmt.Printf("[RequestVote] %d ask %d for vote in term %d failed, rf.Term: %d\n", args.CandidateId, rf.me, args.Term, rf.currentTerm)
		return
	}

	// 如果接收到的 RPC 请求或响应中，任期号`T > currentTerm`，
	// 那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
	if args.Term > rf.currentTerm {
		rf.rwLock.Lock()
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.rwLock.Unlock()
	}

	/*
	 * Raft保证被选为新leader的节点拥有所有已提交的log entry
	 * candidate在发送RequestVoteRPC时，会带上自己的最后一条日志记录的term_id和index，
	 * 其他节点收到消息时，如果发现自己的日志比RPC请求中携带的更新，拒绝投票。
	 * 日志比较的原则是，如果本地的最后一条log entry的term id更大，则更新，如果term id一样大，则日志更多的更大(index更大)。
	 */
	if rf.logs[rf.commitIndex].Term > args.LastLogTerm ||
		(rf.logs[rf.commitIndex].Term == args.LastLogTerm && rf.commitIndex > args.LastLogIndex){
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		// fmt.Printf("[RequestVote] %d ask %d for vote in term %d failed, rf.Term: %d, args: %+v\n", args.CandidateId, rf.me, args.Term, rf.currentTerm, args)
		return
	}

	// 准备投票，每个term，只会给一个candidate投票
	pos := args.Term % 100
	if ((rf.votedTerm == -1 || rf.voteEachTerm[pos] == args.CandidateId) && rf.currentTerm == args.Term) ||
		(rf.votedTerm < args.Term) {
		rf.votedTerm = args.Term
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.voteEachTerm[pos] = args.CandidateId

		rf.rwLock.Lock()
		rf.currentTerm = args.Term
		if args.CandidateId != rf.me {
			rf.status = FOLLOWER
		}
		rf.rwLock.Unlock()

		// fmt.Printf("[RequestVote] [%d] vote for candidate %d in term %d\n", rf.me, args.CandidateId, args.Term)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// fmt.Printf("[RequestVote] [%d] reject for candidate %d in term %d\n", rf.me, args.CandidateId, args.Term)
	}
}

func (rf *Raft) RequestAppendEntry(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	// fmt.Printf("leader %d ask %d to append Entry with args: %+v\n", args.LeaderId, rf.me, args)
	/*
	1. 返回假
		如果领导者的任期 小于 接收者的当前任期（译者注：这里的接收者是指跟随者或者候选者）（5.1 节）
	2. 返回假
		如果接收者日志中没有包含这样一个条目 即该条目的任期在preLogIndex上能和prevLogTerm匹配上
	   （译者注：在接收者日志中 如果能找到一个和preLogIndex以及prevLogTerm一样的索引和任期的日志条目 则返回真 否则返回假）（5.3 节）
	3. 如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
	4. 追加日志中尚未存在的任何新条目
	5. 如果领导者的已知已经提交的最高的日志条目的索引 大于 接收者的已知已经提交的最高的日志条目的索引
	   则把 接收者的已知已经提交的最高的日志条目的索引 重置为 领导者的已知已经提交的最高的日志条目的索引 或者是 上一个新条目的索引 取两者的最小值
	 */

	// 收到了一个过期任期的请求，直接拒绝
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		fmt.Printf("[RequestAppendEntry-A] from %d[%d] to %d[%d], arg: %+v, reply: %v, rf.Term: %d\n",
		 	args.LeaderId, args.LogId, rf.me, rf.commitIndex, args, reply, rf.currentTerm)
		return
	}

	// 如果接收到的 RPC 请求或响应中，任期号`T > currentTerm`，
	// 那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		fmt.Printf("[RequestAppendEntry-past] from %d[%d] to %d[%d], arg: %+v, reply: %v, change to follower rf.Term: %d\n",
			args.LeaderId, args.LogId, rf.me, rf.commitIndex, args, reply, rf.currentTerm)
	}

	if args.Log == LEADER_HEARTBEAT {
		// 接收到的是leader的心跳，直接转到心跳channel进行处理
		rf.heartBeatChan <- ApplyMsg{
			CommandValid: true,
			Command:      args.LeaderCommit,
			CommandIndex: args.LogId,
		}
	} else if args.Log == LEADER_BLANK_ENTRY {
		// 5. 如果领导者的已知已经提交的最高的日志条目的索引 大于 接收者的已知已经提交的最高的日志条目的索引
		// 则把 接收者的已知已经提交的最高的日志条目的索引 重置为
		// 领导者的已知已经提交的最高的日志条目的索引 或者是 上一个新条目的索引 取两者的最小值
		if rf.logs[rf.commitIndex].Term != args.PrevLogTerm {
			return
		}
		if args.LeaderCommit > rf.lastApplied {
			newLastApply := 0
			if args.LeaderCommit < rf.commitIndex {
				newLastApply = args.LeaderCommit
			} else {
				newLastApply = rf.commitIndex
			}
			for i := rf.lastApplied + 1; i <= newLastApply; i += 1 {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Log,
					CommandIndex: i,
				}
			}
		}
		rf.persist()
	} else {
		if rf.logs[args.LogId - 1].Term == args.PrevLogTerm {
			if args.LogId == rf.commitIndex + 1 {
				// logId，正好是下一个要append entry的位置
				index := rf.commitIndex + 1
				rf.logs[index].Log = args.Log
				rf.logs[index].Term = args.LogTerm
				rf.commitIndex = index
				fmt.Printf("[RequestAppendEntry-B], append new entry: rf.logs[%d][%d]: %+v, now commit: %d, apply: %d\n",
					rf.me, index, rf.logs[index], rf.commitIndex, rf.lastApplied)
			} else if rf.commitIndex >= args.LogId {
				// logId位置已经添加过日志
				if rf.logs[args.LogId].Log == args.Log && rf.logs[args.LogId].Term == args.Term  {
					// 日志相同，返回true(可能是网络延迟收到的旧包）
					reply.Success = true
					reply.Term = rf.currentTerm
					fmt.Printf("[RequestAppendEntry-C], %d received a past log %+v and return success: %+v\n", rf.me, args, reply)
				} else {
					// 日志相悖，删除这个已经存在的条目以及它之后的所有条目
					rf.commitIndex = args.LogId
					// 追加新条目
					rf.logs[rf.commitIndex].Log = args.Log
					rf.logs[rf.commitIndex].Term = args.LogTerm
					fmt.Printf("[RequestAppendEntry-D], append exist entry: rf[%d].logs[%d]: %+v\n", rf.me, rf.commitIndex, rf.logs[rf.commitIndex])
				}
			} else {
				reply.Success = false
				reply.Term = rf.currentTerm
				fmt.Printf("[RequestAppendEntry-E], append exist entry: rf[%d].logs[%d]: %+v\n", rf.me, rf.commitIndex, rf.logs[rf.commitIndex])

			}
			// 5. 如果领导者的已知已经提交的最高的日志条目的索引 大于 接收者的已知已经提交的最高的日志条目的索引
			// 则把 接收者的已知已经提交的最高的日志条目的索引 重置为
			// 领导者的已知已经提交的最高的日志条目的索引 或者是 上一个新条目的索引 取两者的最小值
			if args.LeaderCommit > rf.lastApplied {
				newLastApply := 0
				if args.LeaderCommit < rf.commitIndex {
					newLastApply = args.LeaderCommit
				} else {
					newLastApply = rf.commitIndex
				}
				for i := rf.lastApplied + 1; i <= newLastApply; i += 1 {
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[i].Log,
						CommandIndex: i,
					}
				}
			}
			reply.Success = true
			reply.Term = rf.currentTerm
			fmt.Printf("[A] rf.logs[%d][%d] %+v, arg: %+v, rep: %+v\n", rf.me, args.LogId, rf.logs[rf.commitIndex].Log, args, reply)
		} else {
			reply.Success = false
			reply.Term = rf.currentTerm
			fmt.Printf("[B] from [%d][%d] to [%d][%d], arg: %+v, reply: %v, rf.logs[%d].Term: %d\n", args.LeaderId, args.LogId, rf.me, rf.commitIndex, args, reply, args.LogId - 1, rf.logs[args.LogId - 1].Term)
		}
		rf.persist()
	}
}

//
// example code to sendMsg a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	done := make(chan bool)

	go func() {
		rf.peers[server].Call("Raft.RequestVote", args, reply)
		done <- true

	}()
	select {
	case <- done:
		// 如果接收到的 RPC 请求或响应中，任期号`T > currentTerm`，那么就令 currentTerm 等于 T，并切换状态为跟随者
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			if rf.status == LEADER {
				fmt.Printf("leader %d change to follower\n", rf.me)
			}
			rf.status = FOLLOWER
		}
		return true
	case <- time.After(time.Millisecond * time.Duration(NET_TIMEOUT)):
		reply.VoteGranted = false
		reply.Term = -1
		return false
	}
}

func (rf *Raft) sendRequestAppendEntry(server int, args interface{}, reply *RequestAppendEntryReply) bool {
	done := make(chan bool)

	go func() {
		rf.peers[server].Call("Raft.RequestAppendEntry", args, reply)
		done <- true
	}()

	select {
	case <- done:
		// 如果接收到的 RPC 请求或响应中，任期号`T > currentTerm`，那么就令 currentTerm 等于 T，并切换状态为跟随者
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			if rf.status == LEADER {
				fmt.Printf("leader %d change to follower\n", rf.me)
			}
			rf.status = FOLLOWER
		}
		return true
	case <- time.After(time.Millisecond * time.Duration(NET_TIMEOUT)):
		reply.Success = false
		return false
	}
}


func (rf *Raft) sendHistoryMsgs(term int, who int) {
	msgId := rf.nextIndex[who]
	for ; msgId >= 1 && rf.status == LEADER; msgId -= 1 {
		rf.rwLock.RLock()
		request := RequestAppendEntryArgs{
			Term:         term,
			LeaderId:     rf.me,
			Log:          rf.logs[msgId].Log,
			LogId:        msgId,
			PrevLogIndex: msgId - 1,
			PrevLogTerm:  rf.logs[msgId - 1].Term,
			LogTerm:      rf.logs[msgId].Term,
			LeaderCommit: rf.lastApplied,
		}
		rf.rwLock.RUnlock()

		replies := rf.sendMsg(MSG_APPLY_ENTRY, &request, msgId, who)
		rpy := replies[0].(*RequestAppendEntryReply)
		fmt.Printf("%d sendHistoryMsgs to %d msgId: %d, rep: %+v\n", rf.me, who, msgId, rpy)
		if rpy.Success == false {
			continue
		} else {
			rf.matchIndex[who] = msgId
			break
		}
	}
	// fmt.Printf("start sync at port: %d\n", msgId)
	for syncPot := msgId + 1; syncPot <= rf.commitIndex && rf.status == LEADER; syncPot += 1 {
		rf.rwLock.RLock()
		request := RequestAppendEntryArgs{
			Term:         term,
			LeaderId:     rf.me,
			Log:          rf.logs[syncPot].Log,
			LogId:        syncPot,
			PrevLogIndex: syncPot - 1,
			PrevLogTerm:  rf.logs[syncPot-1].Term,
			LogTerm:      rf.logs[syncPot].Term,
			LeaderCommit: rf.lastApplied,
		}
		rf.rwLock.RUnlock()

		replies := rf.sendMsg(MSG_APPLY_ENTRY, &request, msgId, who)
		rpy := replies[0].(*RequestAppendEntryReply)
		// fmt.Printf("%d sendHistoryMsgs to %d msgId: %d, rep: %+v\n", rf.me, who, msgId, rpy)
		if rpy.Success == false {
			break
		} else {
			rf.rwLock.Lock()
			rf.nextIndex[who] = syncPot + 1
			rf.matchIndex[who] = syncPot
			rf.rwLock.Unlock()
		}
	}
	rf.appendHistoryFlag[who] = false
	fmt.Printf("[appendLog] leader: %d(commitIndex: %d) finish sync to follower: %d(nextIndex: %d, matchIndex: %d)\n",
		rf.me, rf.commitIndex, who, rf.nextIndex[who], rf.matchIndex[who])
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	/*
	 *  客户端的每一个请求都包含一条被复制状态机执行的指令。
	 */
	fmt.Printf("start command: %d, rf[%d] commitIndex: %d\n", command, rf.me, rf.commitIndex)
	// 当前log应该append的position
	rf.rwLock.Lock()
	defer rf.rwLock.Unlock()

	index := rf.commitIndex + 1
	term := rf.currentTerm
	isLeader := rf.status == LEADER
	request := &RequestAppendEntryArgs{
		Term:         term,
		LeaderId:     rf.me,
		Log:          command,
		LogId:        index,
		PrevLogIndex: index - 1,
		PrevLogTerm:  rf.logs[index - 1].Term,
		LogTerm:      term,
		LeaderCommit: rf.lastApplied,
	}

	// Your code here (2B).
	if rf.status == LEADER {
		// 领导人把这条指令作为一条新的日志条目附加到日志中去
		rf.logs[index].Log = command
		rf.logs[index].Term = rf.currentTerm
		rf.commitIndex = index
		fmt.Printf("[Start-A], append new entry: rf.logs[%d][%d]: %+v\n", rf.me, rf.commitIndex, rf.logs[rf.commitIndex])

		// 然后并行的发起附加条目 RPCs 给其他的服务器，让他们复制这条日志条目。
		successNum := 0
		for i := 0; i < len(rf.peers) && rf.status == LEADER; i += 1 {
			if i == rf.me {
				continue
			}

			// ask follower to append
			replies := rf.sendMsg(MSG_APPLY_ENTRY, request, index, i)
			rep := replies[0].(*RequestAppendEntryReply)
			if rep.Success {
				rf.nextIndex[i] = index
				successNum += 1
			}
		}

		if rf.status == LEADER {
			// leader先自己append log
			// logId位置还没添加新的日志条目，则直接追加
			// 可能在发消息的时候，状态变了

			if successNum + 1 > len(rf.peers)/2 {
				// 当这条日志条目被安全的复制（下面会介绍）
				// 领导人会应用这条日志条目到它的状态机中然后把执行的结果返回给客户端。
				fmt.Printf("apply success, commitIndex: %d, leader %d apply %d now\n", rf.commitIndex, rf.me, command)

				// 这里只有当前面的都apply了，才能立马apply当前的command
				if rf.commitIndex == rf.lastApplied + 1 {
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      command,
						CommandIndex: index,
					}
					rf.lastApplied = rf.commitIndex
				}


			} else {
				fmt.Printf("apply failed and retry, command: %d, index: %d, successNum: %d\n", command.(int), index, successNum)
			}
		} else {
			return index, term, false
		}
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	fmt.Printf("kill rf %d\n", rf.me)
	rf.persist()
	rf.sendMsg(MSG_STOP, nil, NO_MSG_ID, rf.me)
}

func (rf *Raft)initLeader()  {
	for i := 0; i < len(rf.peers); i += 1 {
		rf.nextIndex[i] = rf.commitIndex + 1
		rf.matchIndex[i] = 0
		// 先发个心跳
		rf.sendMsg(MSG_HERATBEAT, nil, NO_MSG_ID, i)
		/*
		// 发送空的entry，把前面的entry都直接apply
		rf.sendMsg(MSG_APPLY_ENTRY,&RequestAppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Log:          LEADER_BLANK_ENTRY,
			LogId:        rf.commitIndex + 1,
			PrevLogIndex: rf.commitIndex ,
			PrevLogTerm:  rf.logs[rf.commitIndex].Term,
			LogTerm:      rf.currentTerm,
			LeaderCommit: rf.lastApplied,
		} , rf.commitIndex + 1, i)
		 */
	}
	/*
	for i := rf.commitIndex; i < rf.lastApplied; i += 1 {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Log,
			CommandIndex: i,
		}
	}
	rf.lastApplied = rf.commitIndex
	 */
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to sendMsg ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// TODO Your initialization code here (2A, 2B, 2C).
	rf.votedTerm = -1
	rf.currentTerm = 0
	rf.status = FOLLOWER


	rf.voteEachTerm = make([]int, MsgRingSize)
	rf.logs = make([]LogEntry, MsgRingSize)
	rf.commitIndex = 0
	rf.lastApplied = 0

	// heartbeat channel
	rf.heartBeatChan = make(chan ApplyMsg)
	// stop channel
	rf.stopChan = make(chan bool)
	// logs channel
	rf.applyCh = applyCh

	// for leader
	rf.nextIndex = make([]int, MsgRingSize)
	rf.matchIndex = make([]int, MsgRingSize)
	rf.appendHistoryFlag = make([]bool, MsgRingSize)


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// run
	go rf.process()
	return rf
}

func (rf *Raft) appendLog()  {
	serverNum := len(rf.peers)
	/*
	 * 如果对于一个跟随者，最后日志条目的索引值大于等于 nextIndex，那么：发送从 nextIndex 开始的所有日志条目：
	 * 如果成功：更新相应跟随者的 nextIndex 和 matchIndex
	 * 如果因为日志不一致而失败，减少 nextIndex 重试
	 */
	term := rf.currentTerm
	for i := 0; i < serverNum; i += 1 {
		if i == rf.me {
			continue
		}
		//fmt.Printf("[appendLog] leader: %d(commitIndex: %d, apply: %d) to follower: %d(nextIndex: %d)\n",
		// 	rf.me, rf.commitIndex, rf.lastApplied, i, rf.nextIndex[i])
		if rf.commitIndex > 0 && rf.commitIndex > rf.matchIndex[i] {
			if rf.appendHistoryFlag[i] == false {
				rf.appendHistoryFlag[i] = true
				fmt.Printf("[appendLog] leader: %d(commitIndex: %d) start sync to follower: %d(nextIndex: %d)\n",
					rf.me, rf.commitIndex, i, rf.nextIndex[i])
				go rf.sendHistoryMsgs(term, i)
			}
		}
	}

	// 如果start的时候apply失败了，在这里需要重新检测follower日志是否已经追上了，如果大部分的追上了，则leader apply这个log
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i += 1 {
		applied := 0
		for j := 0; j < serverNum; j += 1 {
			if rf.matchIndex[j] >= i {
				applied += 1
			}
		}
		// 加上自己
		applied += 1
		// fmt.Printf("[process] %d check log: %+v, applied now: %d\n", rf.me, rf.logs[i], applied)
		if applied > serverNum / 2 {
			rf.lastApplied = i
			rf.applyCh <- ApplyMsg {
				CommandValid: true,
				Command:  rf.logs[i].Log,
				CommandIndex: i,
			}
		}
	}
}

func (rf *Raft) process() {
	for true {
		if rf.status == LEADER {
			rand.Seed(time.Now().UnixNano())
			waitTime := rand.Intn(VOTE_INTERVAL) + VORE_BASE
			heartbeatTicker := time.Tick(HEARTBEAT_INTERVAL * time.Millisecond)
			select {
			case <- rf.stopChan:
				// rf.persist()
				return
			case <- time.After(time.Millisecond * time.Duration(waitTime)):
				rf.status = CANDIDATE
			case <- heartbeatTicker:
				rf.sendMsg(MSG_HERATBEAT, nil, NO_MSG_ID, BROADCAST)
			}
			rf.appendLog()
		} else if rf.status == FOLLOWER {
			rand.Seed(time.Now().UnixNano())
			waitTime := rand.Intn(VOTE_INTERVAL) + VORE_BASE
			select {
			case <- rf.stopChan:
				// rf.persist()
				return
			case <- time.After(time.Millisecond * time.Duration(waitTime)):
				rf.status = CANDIDATE
			case msg := <-rf.heartBeatChan:
				// leader已经应用了position在applyEnd的log
				applyEnd := msg.Command.(int)
				rf.rwLock.Lock()
				fmt.Printf("[HeartBeat-follower] %d now lastApplied: %d, commitIndex: %d receive lastApplied: %d\n",
					rf.me, rf.lastApplied, rf.commitIndex, applyEnd)
				for i := rf.lastApplied + 1; i <= applyEnd && i <= rf.commitIndex; i += 1 {
					varCommand := rf.logs[i].Log
					rf.lastApplied = i
					rf.applyCh <- ApplyMsg {
						CommandValid: true,
						Command:  varCommand,
						CommandIndex: i,
					}
					fmt.Printf("[HeartBeat-follower] %d now apply: %+v, lastApplied: %d, commitIndex: %d\n",
						rf.me, rf.logs[i].Log, rf.lastApplied, rf.commitIndex)
				}
				rf.rwLock.Unlock()
				continue
			}
		} else if rf.status == CANDIDATE {
			rand.Seed(time.Now().UnixNano())
			waitTime := rand.Intn(VOTE_INTERVAL) + VORE_BASE

			// 开始请求投票
			rf.rwLock.RLock()
			severNum := len(rf.peers)
			rf.currentTerm += 1
			req := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.commitIndex,
				LastLogTerm:  rf.logs[rf.commitIndex].Term,
			}
			rf.rwLock.RUnlock()

			voteCount := 0
			rsp := rf.sendMsg(MSG_ASK_VOTE, &req, NO_MSG_ID, BROADCAST)
			for i := 0; i < severNum; i += 1 {
				if rsp[i].(*RequestVoteReply).VoteGranted == true {
					voteCount += 1
				}
				if rsp[i].(*RequestVoteReply).Term > rf.currentTerm {
					rf.currentTerm = rsp[i].(*RequestVoteReply).Term
				}
			}

			// 处理投票结果
			if voteCount > severNum / 2 && rf.status !=FOLLOWER{
				fmt.Printf("-------\n%d ask for vote and success with term %d\n", rf.me, rf.currentTerm)
				rf.rwLock.Lock()
				rf.status = LEADER
				rf.initLeader()
				rf.rwLock.Unlock()
				continue
			}

			select {
			case <- rf.stopChan:
				return
			case <- time.After(time.Millisecond * time.Duration(waitTime)):
				fmt.Printf("%d timeout %d and will ask for vote\n", rf.me, waitTime)
				rf.status = CANDIDATE
			case msg := <-rf.heartBeatChan:
				//
				rf.status = FOLLOWER
				// leader已经应用了position在applyEnd的log
				applyEnd := msg.Command.(int)
				rf.rwLock.Lock()
				fmt.Printf("[HeartBeat-follower] %d now lastApplied: %d, commitIndex: %d receive lastApplied: %d\n",
					rf.me, rf.lastApplied, rf.commitIndex, applyEnd)
				for i := rf.lastApplied + 1; i <= applyEnd && i <= rf.commitIndex; i += 1 {
					varCommand := rf.logs[i].Log
					rf.lastApplied = i
					rf.applyCh <- ApplyMsg {
						CommandValid: true,
						Command:  varCommand,
						CommandIndex: i,
					}
					fmt.Printf("[HeartBeat-follower] %d now apply: %+v, lastApplied: %d, commitIndex: %d\n",
						rf.me, rf.logs[i].Log, rf.lastApplied, rf.commitIndex)
				}
				rf.rwLock.Unlock()
			}
		}
	}
}

func (rf *Raft) sendMsg(msgTye int, msg interface{}, msgId int, who int) []interface{} {
	severNum := len(rf.peers)
	switch msgTye {
	case MSG_ASK_VOTE:
		if who == BROADCAST {
			rsp := make([]interface{}, severNum)
			for i := 0; i < severNum; i += 1 {
				rsp[i] = new(RequestVoteReply)
				rf.sendRequestVote(i, msg.(*RequestVoteArgs), rsp[i].(*RequestVoteReply))
			}
			return rsp
		} else {
			rsp := make([]interface{}, 1)
			rsp[0] = new(RequestVoteReply)
			rf.sendRequestVote(who, msg.(*RequestVoteArgs), rsp[0].(*RequestVoteReply))
			return rsp
		}
	case MSG_HERATBEAT:
		// 心跳，不需要reply
		for i := 0; i < severNum; i += 1 {
			req := &RequestAppendEntryArgs {
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Log:          LEADER_HEARTBEAT,
				LeaderCommit: int(math.Min(float64(rf.lastApplied), float64(rf.matchIndex[i]))),
			}
			rf.sendRequestAppendEntry(i, req, &RequestAppendEntryReply{})
		}
		return nil
	case MSG_BROADCAST_APPLY_ENTRY:
		req := RequestAppendEntryArgs {
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			LogId:    msgId,
			Log:      msg,
			PrevLogIndex: msgId - 1,
			PrevLogTerm: rf.logs[msgId - 1].Term,
			LogTerm: rf.currentTerm,
			LeaderCommit: rf.lastApplied,
		}
		rsp := make([]interface{}, severNum)
		for i := 0; i < severNum; i += 1 {
			rsp[i] = new(RequestAppendEntryReply)
			rf.sendRequestAppendEntry(i, &req, rsp[i].(*RequestAppendEntryReply))
		}
		return rsp
	case MSG_APPLY_ENTRY:
		rsp := make([]interface{}, 1)
		realRsp := new(RequestAppendEntryReply)
		rsp[0] = realRsp
		rf.sendRequestAppendEntry(who, msg, rsp[0].(*RequestAppendEntryReply))
		return rsp
	case MSG_STOP:
		rf.stopChan <- true
		break
	}
	return nil
}