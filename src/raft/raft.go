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
	"../labrpc"
	"math"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

const LEADER = 1
const FOLLOWER = 2
const CANDIDATE = 3
const LEADER_HEARTBEAT = math.MinInt32

const HEARTBEAT_INTERVAL = 50
const VOTE_INTERVAL = 100
const NET_TIMEOUT = 50

const MsgRingSize = 100
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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
	msg          []interface{}

	/*
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
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// TODO Your code here (2A).
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	Msg 		interface{}
 	LeaderCommit int
	MsgId 		int
}

type RequestAppendEntryReply struct {
	Term        int
	Success     bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO Your code here (2A, 2B).
	// fmt.Printf("[%d] RequestVote for candidate %d\n", rf.me, args.CandidateId)
	pos := args.Term % 100
	if ((rf.votedTerm == -1 || rf.voteEachTerm[pos] == args.CandidateId) && rf.currentTerm == args.Term) ||
		(rf.votedTerm < args.Term) {
		rf.votedTerm = args.Term
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.voteEachTerm[pos] = args.CandidateId
		//fmt.Printf("[%d] vote for candidate %d in term %d\n", rf.me, args.CandidateId, args.Term)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//fmt.Printf("[%d] reject for candidate %d in term %d\n", rf.me, args.CandidateId, args.Term)
	}
}

func (rf *Raft) RequestAppendEntry(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	if rf.me != args.LeaderId {
		rf.status = FOLLOWER
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}

	if args.Msg == LEADER_HEARTBEAT {
		rf.heartBeatChan <- ApplyMsg{
			CommandValid: true,
			Command:      args,
			CommandIndex: args.MsgId,
		}
		return
	} else {
		// fmt.Printf("from %d pos %d to %d pos %d, arg: %+v\n", args.LeaderId, args.MsgId, rf.me, rf.commitIndex, args)
		if rf.commitIndex == args.MsgId {

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      args.Msg,
				CommandIndex: args.MsgId,
			}
			rf.msg[rf.commitIndex] = args.Msg
			rf.commitIndex += 1
			reply.Success = true
			reply.Term = rf.currentTerm
			// fmt.Printf("applyCh[%d][%d] %d, rep: %+v\n", rf.me, args.MsgId, args.Msg, reply)
		} else {
			reply.Success = false
			reply.Term = rf.currentTerm
		}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestAppendEntry(server int, args interface{}, reply *RequestAppendEntryReply) bool {
	done := make(chan bool)

	go func() {
		rf.peers[server].Call("Raft.RequestAppendEntry", args, reply)
		done <- true
	}()

	select {
	case <- done:
		return true
	case <- time.After(time.Millisecond * time.Duration(NET_TIMEOUT)):
		return false
	}
}

func (rf *Raft) sendHistoryMsgs(who int, begin int) {
	//fmt.Printf("%d sendHistoryMsgs to %d, endEntry: %d\n", rf.me, who, begin)
	for i := begin; i >= 0; i -= 1 {
		replies := rf.sendMsg(MSG_APPLY_ENTRY, &RequestAppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Msg:          rf.msg[i],
			MsgId:        i,
		}, i, who)
		rpy := replies[0].(*RequestAppendEntryReply)
		if !rpy.Success {
			continue
		}
		// fmt.Printf("start sync at port: %d\n", i + 1)
		for syncPot := i + 1; syncPot <= begin; syncPot += 1 {
			rf.sendMsg(MSG_APPLY_ENTRY, &RequestAppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Msg:          rf.msg[syncPot],
				MsgId:        syncPot,
			}, i, who)
		}
		return
	}
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
	index := rf.commitIndex
	term := rf.currentTerm
	isLeader := rf.status == LEADER

	// Your code here (2B).
	if rf.status == LEADER {
		fmt.Printf("send command %+v\n", command)
		successNum := 0
		replies := rf.sendMsg(MSG_BROADCAST_APPLY_ENTRY, command, index, BROADCAST)
		for i := 0; i < len(rf.peers); i += 1 {
			rep := replies[i].(*RequestAppendEntryReply)
			// fmt.Printf("start %d -> %d rep %+v\n", rf.me, i, rep)
			if !rep.Success {
				go rf.sendHistoryMsgs(i, index)
			} else {
				successNum += 1
			}
		}

		if successNum > len(rf.peers) / 2 {
			rf.lastApplied = rf.commitIndex
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
	rf.sendMsg(MSG_STOP, nil, NO_MSG_ID, BROADCAST)
}

func (rf *Raft)initLeader()  {
	for i := 0; i < len(rf.peers); i += 1 {
		rf.nextIndex[i] = rf.commitIndex + 1
		rf.matchIndex[i] = 0
	}
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
	rf.msg = make([]interface{}, MsgRingSize)
	rf.commitIndex = 1

	// heartbeat channel
	rf.heartBeatChan = make(chan ApplyMsg)

	// stop channel
	rf.stopChan = make(chan bool)

	// msg channel
	rf.applyCh = applyCh

	// for leader
	rf.nextIndex = make([]int, MsgRingSize)
	rf.matchIndex = make([]int, MsgRingSize)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// run
	go rf.process()
	return rf
}

func (rf *Raft) process() {
	for true {
		if rf.status == LEADER {
			rf.initLeader()
			rf.sendMsg(MSG_HERATBEAT, nil, NO_MSG_ID, BROADCAST)

			rand.Seed(time.Now().UnixNano())
			waitTime := rand.Intn(VOTE_INTERVAL) + VOTE_INTERVAL
			select {
			case <- rf.stopChan:
				return
			case <- time.After(time.Millisecond * time.Duration(waitTime)):
				// fmt.Printf("%d timeout and will ask for vote\n", rf.me)
				rf.status = FOLLOWER
			case <-rf.heartBeatChan:
				// fmt.Printf("%d receive msg with id: %d\n", rf.me, msg.CommandIndex)
				time.Sleep(time.Millisecond * time.Duration(HEARTBEAT_INTERVAL))
			}
		} else if rf.status == FOLLOWER {
			rand.Seed(time.Now().UnixNano())
			waitTime := rand.Intn(VOTE_INTERVAL) + VOTE_INTERVAL

			select {
			case <- rf.stopChan:
				return
			case <- time.After(time.Millisecond * time.Duration(waitTime)):
				//fmt.Printf("%d timeout and will ask for vote\n", rf.me)
			case <-rf.heartBeatChan:
				//fmt.Printf("%d receive msg with id: %d\n", rf.me, msg.CommandIndex)
				time.Sleep(time.Millisecond * time.Duration(HEARTBEAT_INTERVAL))
				continue
			}

			// ask for vote
			severNum := len(rf.peers)
			rf.currentTerm += 1
			req := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}

			// fmt.Printf("%d ask for vote with term %d\n", rf.me, rf.currentTerm)
			voteCount := 0
			rsp := rf.sendMsg(MSG_ASK_VOTE, &req, NO_MSG_ID, BROADCAST)

			select {
			case <- time.After(time.Millisecond * time.Duration(VOTE_INTERVAL)):
				for i := 0; i < severNum; i += 1 {
					if rsp[i].(*RequestVoteReply).VoteGranted == true {
						voteCount += 1
					}
					if rsp[i].(*RequestVoteReply).Term > rf.currentTerm {
						rf.currentTerm = rsp[i].(*RequestVoteReply).Term
					}
				}
			}

			if voteCount > severNum / 2 {
				rf.status = LEADER
				//fmt.Printf("%d ask for vote and success with term %d\n", rf.me, rf.currentTerm)
			} else {
				rf.status = FOLLOWER
				//fmt.Printf("%d ask for vote but failed with term %d\n", rf.me, rf.currentTerm)
			}
		}
	}
}

func (rf *Raft) sendMsg(msgTye int, msg interface{}, msgId int, who int) []interface{} {
	severNum := len(rf.peers)
	switch msgTye {
	case MSG_ASK_VOTE:
		rsp := make([]interface{}, severNum)
		for i := 0; i < severNum; i += 1 {
			rsp[i] = &RequestVoteReply{}
			go rf.sendRequestVote(i, msg.(*RequestVoteArgs), rsp[i].(*RequestVoteReply))
		}
		return rsp
	case MSG_HERATBEAT:
		req := &RequestAppendEntryArgs {
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			Msg: LEADER_HEARTBEAT,
		}
		// 心跳，不需要reply
		for i := 0; i < severNum; i += 1 {
			rf.sendRequestAppendEntry(i, req, &RequestAppendEntryReply{})
		}
		return nil
	case MSG_BROADCAST_APPLY_ENTRY:
		req := RequestAppendEntryArgs {
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			MsgId: msgId,
			Msg:  msg,
		}
		rsp := make([]interface{}, severNum)
		for i := 0; i < severNum; i += 1 {
			rsp[i] = &RequestAppendEntryReply{}
			rf.sendRequestAppendEntry(i, &req, rsp[i].(*RequestAppendEntryReply))
		}
		return rsp
	case MSG_APPLY_ENTRY:
		rsp := make([]interface{}, 1)
		rsp[0] = &RequestAppendEntryReply{}
		rf.sendRequestAppendEntry(who, msg, rsp[0].(*RequestAppendEntryReply))
		return rsp
	case MSG_STOP:
		rf.stopChan <- true
		break
	}
	return nil
}