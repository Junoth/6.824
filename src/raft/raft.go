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
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// Raft state
type NodeState int

const (
	FOLLOWER  NodeState = 0
	CANDIDATE           = 1
	LEADER              = 2
)

// log entry
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	// persistent
	state       NodeState
	currentTerm int
	votedFor    int
	heartbeat   bool
	voteNum     int
	log         []LogEntry

	// volatile
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// lock critical section
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fail-fast
	if args.CandidateId < 0 || args.CandidateId >= len(rf.peers) {
		panic("Invalid candidate id")
	}
	if args.Term < 0 {
		panic("Invalid term")
	}
	if args.CandidateId == rf.me {
		panic("Don't send RPC to self")
	}

	// always return current term
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// got request from lower term, reject it
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		// got request from higher term
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.heartbeat = false
		rf.voteNum = 0
		reply.VoteGranted = true
	} else {
		// same term
		switch rf.state {
		case FOLLOWER:
			if rf.votedFor != -1 {
				reply.VoteGranted = false
			} else {
				reply.VoteGranted = true
			}
		case CANDIDATE:
			// candidate should have voted to itself
			reply.VoteGranted = false
		case LEADER:
			// candidate should have voted to itself
			reply.VoteGranted = false
		}
	}

	if len(rf.log) > 0 {
		if rf.log[len(rf.log)-1].Term > args.LastLogTerm ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex) {
			reply.VoteGranted = false
		}
	}

	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// lock critical section
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fail-fast
	if args.LeaderId < 0 || args.LeaderId >= len(rf.peers) {
		panic("Invalid leader id")
	}
	if args.Term < 0 {
		panic("Invalid term")
	}
	if args.LeaderId == rf.me {
		panic("Don't send RPC to self")
	}

	// always return current term
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// got request from stale leader, ignore
		reply.Success = false
		return
	}

	// heartbeat
	switch rf.state {
	case FOLLOWER:
		// refresh
		rf.refresh()
	case CANDIDATE:
		// candidate should fall back to follower
		rf.fallback()
	case LEADER:
		if args.Term > rf.currentTerm {
			// we're stale leader, fall back
			rf.fallback()
		} else {
			panic("Two leaders in the same term, is it possible?")
		}
	}

	// append entries
	if args.PrevLogIndex >= 0 && (len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// previous log not matched
		reply.Success = false
	} else {
		// only append entries when prev log matches
		for i, entry := range args.Entries {
			// for simplicity, we'll just override/append entries
			if i+args.PrevLogIndex+1 >= len(rf.log) {
				rf.log = append(rf.log, entry)
			} else {
				rf.log[i+args.PrevLogIndex+1] = entry
			}
		}

		reply.Success = true

		// commit only when log actually matched
		if args.LeaderCommit > rf.commitIndex {
			updateCommitIndex := min(args.LeaderCommit, len(rf.log))
			for i := rf.commitIndex + 1; i <= min(i+args.PrevLogIndex+1, updateCommitIndex); i++ {
				// as each Raft peer becomes aware that successive log entries are
				// committed, the peer should send an ApplyMsg to the service (or
				// tester) on the same server, via the applyCh passed to Make(). set
				// CommandValid to true to indicate that the ApplyMsg contains a newly
				// committed log entry.
				applyMsg := &ApplyMsg{}
				applyMsg.Command = rf.log[i].Command
				applyMsg.CommandIndex = i + 1
				applyMsg.CommandValid = true

				DPrintf("Follower server %v try to commit index %d and command %v",
					rf.me, i+1, rf.log[i].Command)

				rf.applyCh <- *applyMsg
			}

			rf.commitIndex = updateCommitIndex
		}
	}

	// update term at last
	rf.currentTerm = args.Term
}

//
// example code to send a RequestVote RPC to a server.
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		// response from old term, skip
		return false
	}

	DPrintf("I'm %v with current term %v and arg term %v, I request vote from %v for which term is %v and result is %v, rpc response is %v\n",
		args.CandidateId, rf.currentTerm, args.Term, server, reply.Term, reply.VoteGranted, ok)

	if rf.state != CANDIDATE {
		// not a candidate anymore, skip
		return false
	}

	if ok {
		if reply.Term > rf.currentTerm {
			// if got larger term, fall back to follower
			// reset other state
			rf.fallback()
			rf.currentTerm = reply.Term
			return false
		} else {
			if reply.VoteGranted == true {
				rf.voteNum++
			}

			if rf.voteNum >= len(rf.peers)/2+1 {
				// got vote from majority, become leader and broadcast heartbeat
				DPrintf("I'm %v, I got %v vote, will become leader in term %v\n",
					rf.me, rf.voteNum, rf.currentTerm)
				rf.becomeLeader()
				rf.broadcastHeartbeat()
			}
		}
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("I'm %v with current term %v and arg term %v, I append entries to %v for which term is %v, "+
		"prev index is %v, prev term is %v, entries len is %v, result is %v and rpc return is %v\n",
		args.LeaderId, rf.currentTerm, args.Term, server, reply.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries),
		reply.Success, ok)

	if args.Term != rf.currentTerm {
		// response from old term, skip
		return false
	}

	if rf.state != LEADER {
		return false
	}

	if ok && reply.Term > rf.currentTerm {
		// if got larger term, fall back to follower
		// reset other state
		rf.fallback()
		rf.currentTerm = reply.Term
		return false
	}

	if ok && reply.Success {
		if len(args.Entries) > 0 {
			// make sure we sent some log entries
			// since we may send empty log entries for heartbeat or no-op
			rf.nextIndex[server]++
		}

		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

		// get the majority on commit index and update
		cp := make([]int, len(rf.matchIndex))
		copy(cp, rf.matchIndex)
		sort.Sort(sort.Reverse(sort.IntSlice(cp)))
		if cp[len(cp)/2] > rf.commitIndex {
			for i := rf.commitIndex + 1; i <= cp[len(cp)/2]; i++ {
				// as each Raft peer becomes aware that successive log entries are
				// committed, the peer should send an ApplyMsg to the service (or
				// tester) on the same server, via the applyCh passed to Make(). set
				// CommandValid to true to indicate that the ApplyMsg contains a newly
				// committed log entry.
				applyMsg := &ApplyMsg{}
				applyMsg.Command = rf.log[i].Command
				applyMsg.CommandIndex = i + 1
				applyMsg.CommandValid = true

				DPrintf("Leader server %v try to commit index %d and command %v",
					rf.me, i+1, rf.log[i].Command)

				rf.applyCh <- *applyMsg
			}
		}
		rf.commitIndex = cp[len(cp)/2]
	}

	if ok && !reply.Success {
		// need to move to previous for consistency check
		rf.nextIndex[server]--

		// retry
		DPrintf("AppendEntries fails due to log inconsistency, retry")
		rf.makeAppendEntriesCall(server)
	}

	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return 0, 0, false
	}

	entry := LogEntry{rf.currentTerm, command}
	rf.log = append(rf.log, entry)

	DPrintf("Start a command in server %v with state %v, current term is %v and new log entry len is %v\n",
		rf.me, rf.state, rf.currentTerm, len(rf.log))

	rf.matchIndex[rf.me] = len(rf.log) - 1

	return len(rf.log), rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// heartbeat interval should be larger than 100ms
	// we'll set it to 200 ~ 300 ms
	heartbeatInterval := rand.Int31n(100) + 200

	// elect timeout should be much larger than heartbeat
	// we'll set it to 800ms ~ 1000ms
	electInterval := rand.Int31n(200) + 800

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		var sleepTime int32

		rf.mu.Lock()
		switch rf.state {
		case FOLLOWER:
			if !rf.heartbeat {
				// if no heartbeat
				rf.becomeCandidate()
			} else {
				rf.heartbeat = false
			}

			sleepTime = electInterval
		case CANDIDATE:
			// retry
			rf.becomeCandidate()
			sleepTime = electInterval
		case LEADER:
			rf.broadcastHeartbeat()
			sleepTime = heartbeatInterval
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.heartbeat = true
	rf.voteNum = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initial value is -1 to match with slice index
	rf.commitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// Helper functions
func (rf *Raft) fallback() {
	// fallback to follower, reset everything
	// NOTE: THIS FUNCTION SHOULD BE CALLED WITH MUTEX LOCKED

	rf.state = FOLLOWER
	rf.heartbeat = true
	rf.votedFor = -1
	rf.voteNum = 0
}

func (rf *Raft) refresh() {
	// refresh when recving heartbeat
	// NOTE: THIS FUNCTION SHOULD BE CALLED WITH MUTEX LOCKED
	rf.heartbeat = true
}

func (rf *Raft) becomeLeader() {
	// become leader
	// NOTE: THIS FUNCTION SHOULD BE CALLED WITH MUTEX LOCKED

	rf.state = LEADER
	rf.heartbeat = false
	rf.votedFor = rf.me
	rf.voteNum = 0

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.makeAppendEntriesCall(i)
		}
	}
}

func (rf *Raft) makeAppendEntriesCall(server int) {
	// send heartbeat to other nodes, also try to update log
	args := &AppendEntriesArgs{}
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex < 0 {
		args.PrevLogTerm = -1
	} else {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	args.LeaderCommit = rf.commitIndex
	if len(rf.log) > rf.nextIndex[server] {
		args.Entries = rf.log[rf.nextIndex[server]:]
	}

	reply := &AppendEntriesReply{}

	go rf.sendAppendEntries(server, args, reply)
}

func (rf *Raft) becomeCandidate() {
	// become candidate
	// NOTE: THIS FUNCTION SHOULD BE CALLED WITH MUTEX LOCKED
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.heartbeat = false
	rf.votedFor = rf.me
	rf.voteNum = 1 // vote for itself

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// send request vote to other nodes
			args := &RequestVoteArgs{}
			args.CandidateId = rf.me
			args.Term = rf.currentTerm
			args.LastLogTerm = -1
			args.LastLogIndex = -1
			if len(rf.log) > 0 {
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
				args.LastLogIndex = len(rf.log) - 1
			}
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(i, args, reply)
		}
	}
}

func min(n1 int, n2 int) int {
	if n1 < n2 {
		return n1
	}

	return n2
}
