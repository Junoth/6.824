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

	"6.824/labgob"
	"bytes"
	"math/rand"
	"reflect"
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

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
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
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	LastIncludedTerm  int
	LastIncludedIndex int
	NodeSnapshot      []byte

	// volatile
	State       NodeState
	VoteNum     int
	Ticker      int32
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	commitMsgs  []ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.State == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	if enc.Encode(rf.CurrentTerm) != nil ||
		enc.Encode(rf.VotedFor) != nil ||
		enc.Encode(rf.Log) != nil ||
		enc.Encode(rf.LastIncludedTerm) != nil ||
		enc.Encode(rf.LastIncludedIndex) != nil {
		panic("Fail to encode")
	}
	data := buf.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.NodeSnapshot)

	DPrintf("Server %v persist with state %v\n", rf.me, rf)
}

// restore previously persisted state.
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
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var (
		currentTerm       int
		votedFor          int
		log               []LogEntry
		lastIncludedTerm  int
		lastIncludedIndex int
	)
	if dec.Decode(&currentTerm) != nil ||
		dec.Decode(&votedFor) != nil ||
		dec.Decode(&log) != nil ||
		dec.Decode(&lastIncludedTerm) != nil ||
		dec.Decode(&lastIncludedIndex) != nil {
		panic("Fail to decode")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.LastIncludedTerm = lastIncludedTerm
		rf.LastIncludedIndex = lastIncludedIndex
	}

	DPrintf("Server %v read persistent value: %v\n", rf.me, rf)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastIncludedIndex -= 1

	if lastIncludedIndex <= rf.LastIncludedIndex {
		// old snapshot
		return false
	}

	DPrintf("node %v cond install snapshot with lastIncludedTerm %v, lastIncludedIndex %v and current state is %v",
		rf.me, lastIncludedTerm, lastIncludedIndex, rf)

	// trim log
	logLen := len(rf.Log)
	for i := 0; i < logLen; i++ {
		if rf.getLogIndex(i) <= lastIncludedIndex {
			rf.Log = rf.Log[1:]
		}
	}

	// write snapshot
	rf.NodeSnapshot = make([]byte, len(snapshot))
	copy(rf.NodeSnapshot, snapshot)
	rf.LastIncludedIndex = lastIncludedIndex
	rf.LastIncludedTerm = lastIncludedTerm

	// persist data
	rf.persist()

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.LastIncludedIndex+1 {
		return
	}

	index -= 1
	relativeIndex := rf.getRelativeIndex(index)
	if relativeIndex >= len(rf.Log) {
		panic("Snapshot index is out of range")
	}
	term := rf.Log[relativeIndex].Term

	DPrintf("node %v snapshot with index %v and current state is %v",
		rf.me, index, rf)

	// trim log
	logLen := len(rf.Log)
	for i := 0; i < logLen; i++ {
		if rf.getLogIndex(i) <= index {
			rf.Log = rf.Log[1:]
		}
	}

	// write snapshot
	rf.NodeSnapshot = make([]byte, len(snapshot))
	copy(rf.NodeSnapshot, snapshot)
	rf.LastIncludedIndex = index
	rf.LastIncludedTerm = term

	// persist data
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
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
	Term           int
	Success        bool
	FirstTermIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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
	reply.Term = rf.CurrentTerm

	DPrintf("node %v got vote arg %v and current state is %v", rf.me, args, rf)

	if args.Term < rf.CurrentTerm {
		// got request from lower term, reject it
		reply.VoteGranted = false
	} else if args.Term > rf.CurrentTerm {
		// got request from higher term
		rf.fallback(args.Term)
		reply.VoteGranted = true
	} else {
		// same term
		switch rf.State {
		case FOLLOWER:
			if rf.VotedFor != -1 {
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

	// election restriction
	if reply.VoteGranted {
		lastLogIndex := rf.getLastLogIndex()
		lastLogTerm := rf.getLastLogTerm()
		if lastLogIndex >= 0 {
			// compare only when there is a log already
			if lastLogTerm > args.LastLogTerm ||
				(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
				reply.VoteGranted = false
			}
		}
	}

	if reply.VoteGranted {
		rf.VotedFor = args.CandidateId
		rf.refresh()
	}

	rf.persist()
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
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		// got request from stale leader, ignore
		reply.Success = false
		return
	}

	DPrintf("node %v got append arg %v and current state is %v", rf.me, args, rf)

	// heartbeat
	switch rf.State {
	case FOLLOWER:
		// refresh
		rf.CurrentTerm = args.Term
		rf.refresh()
	case CANDIDATE:
		// candidate should fall back to follower
		rf.fallback(args.Term)
		rf.refresh()
	case LEADER:
		if args.Term > rf.CurrentTerm {
			// we're stale leader, fall back
			rf.fallback(args.Term)
			rf.refresh()
		} else {
			panic("Two leaders in the same term, is it possible?")
		}
	}

	// append entries
	if args.PrevLogIndex == rf.LastIncludedIndex {
		reply.Success = args.PrevLogTerm == rf.LastIncludedTerm
	} else if args.PrevLogIndex > rf.LastIncludedIndex && args.PrevLogIndex <= rf.getLastLogIndex() {
		//DPrintf("We got prev log index and last log index")
		if len(rf.Log) == 0 {
			reply.Success = args.PrevLogIndex == rf.getLastLogIndex() && args.PrevLogTerm == rf.getLastLogTerm()
		} else {
			reply.Success = rf.getLog(args.PrevLogIndex).Term == args.PrevLogTerm
		}
	} else {
		reply.Success = false
	}

	if reply.Success == false {
		if args.PrevLogIndex > rf.getLastLogIndex() {
			reply.FirstTermIndex = rf.getLastLogIndex() + 1
		} else if args.PrevLogIndex < rf.LastIncludedIndex {
			reply.FirstTermIndex = rf.LastIncludedIndex + 1
		} else {
			for i := args.PrevLogIndex; i >= rf.getLastLogIndex(); i-- {
				if rf.getLog(args.PrevLogIndex).Term == rf.getLog(i).Term {
					reply.FirstTermIndex = i
				}
			}
		}
	} else {
		// NOTE: WE SHOULD NOT TRUNCATE LOG IF ENTRIES MATCH SINCE WE MAY RECEIVE OUTDATED RPC REQUEST
		if args.PrevLogIndex+len(args.Entries) > rf.getLastLogIndex() {
			// log entries are larger
			rf.Log = append(rf.Log[:rf.getRelativeIndex(args.PrevLogIndex)+1], args.Entries...)
		} else {
			// check if all entries match
			for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex+len(args.Entries); i++ {
				// have to use DeepEqual since Log content may not have equality defined
				// e.x: []string
				if !reflect.DeepEqual(args.Entries[i-args.PrevLogIndex-1], rf.getLog(i)) {
					rf.Log = append(rf.Log[:rf.getRelativeIndex(args.PrevLogIndex)+1], args.Entries...)
					break
				}
			}
		}

		// commit only when log actually matched
		if args.LeaderCommit > rf.commitIndex {
			updateCommitIndex := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))

			startIndex := max(rf.commitIndex+1, rf.LastIncludedIndex+1)
			endIndex := min(updateCommitIndex, rf.getLastLogIndex())
			for i := startIndex; i <= endIndex; i++ {
				applyMsg := ApplyMsg{}
				applyMsg.Command = rf.getLog(i).Command
				applyMsg.CommandIndex = i + 1
				applyMsg.CommandValid = true

				rf.commitMsgs = append(rf.commitMsgs, applyMsg)
			}
			rf.commitIndex = max(rf.commitIndex, endIndex)

			// persist before commit
			rf.persist()
		}
	}

	// persist
	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// lock critical section
	rf.mu.Lock()

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

	DPrintf("node %v got snapshot arg %v and current state is %v", rf.me, args, rf)

	// always return current term
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		// got request from stale leader, ignore
		rf.mu.Unlock()
		return
	}

	// send msg to service
	applyMsg := &ApplyMsg{}
	applyMsg.SnapshotTerm = args.LastIncludedTerm
	applyMsg.SnapshotIndex = args.LastIncludedIndex + 1
	applyMsg.SnapshotValid = true
	applyMsg.Snapshot = make([]byte, len(args.Data))
	copy(applyMsg.Snapshot, args.Data)

	DPrintf("Follower server %v try to send snapshot %v", rf.me, applyMsg)

	// persist
	rf.persist()

	rf.mu.Unlock()
	rf.applyCh <- *applyMsg
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		// has been killed
		return false
	}
	// optimization: save unnecessary rpc call
	rf.mu.Lock()
	if args.Term != rf.CurrentTerm {
		rf.mu.Unlock()
		return false
	}

	DPrintf("node %v with arg %v tries to request vote from node %v, current state is %v\n",
		args.CandidateId, args, server, rf)

	// persist before sending a RPC
	rf.persist()

	rf.mu.Unlock()

	// make rpc call
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("node %v with arg %v tries to request vote from node %v got response %v and rpc response %v\n",
		args.CandidateId, args, server, reply, ok)

	// some cases that should skip
	if !ok || args.Term != rf.CurrentTerm || rf.State != CANDIDATE {
		return false
	}

	if reply.Term > rf.CurrentTerm {
		// if got larger term, fall back to follower
		// reset other state
		rf.fallback(reply.Term)
	} else {
		if reply.VoteGranted == true {
			rf.VoteNum++
		}

		DPrintf("node %v got %v vote in term %v\n",
			rf.me, rf.VoteNum, rf.CurrentTerm)

		if rf.VoteNum >= len(rf.peers)/2+1 {
			// got vote from majority, become leader and broadcast heartbeat
			DPrintf("node %v got %v vote and became leader in term %v\n",
				rf.me, rf.VoteNum, rf.CurrentTerm)
			rf.becomeLeader()
			rf.broadcastHeartbeat()
			//rf.addNoop()
		}
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// optimization: save unnecessary rpc call
	rf.mu.Lock()
	if args.Term != rf.CurrentTerm {
		rf.mu.Unlock()
		return false
	}
	DPrintf("node %v with arg %v tries to append entries to %v, current state is %v\n",
		args.LeaderId, args, server, rf)

	// persist before sending a RPC
	rf.persist()

	rf.mu.Unlock()

	// make rpc call
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()

	DPrintf("node %v with arg %v tries to append entries to %v, got response %v and rpc response %v\n",
		args.LeaderId, args, server, reply, ok)

	if !ok || args.Term != rf.CurrentTerm || rf.State != LEADER {
		rf.mu.Unlock()
		return false
	}

	if reply.Term > rf.CurrentTerm {
		// if got larger term, fall back to follower
		// reset other state
		rf.fallback(reply.Term)
		rf.mu.Unlock()
		return false
	}

	if reply.Success {
		if args.PrevLogIndex+1+len(args.Entries) > rf.nextIndex[server] {
			rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
		}
		if args.PrevLogIndex+len(args.Entries) > rf.matchIndex[server] {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		}

		// get the majority on commit index and update
		// NOTE: ONLY COMMIT INDEX IS IN CURRENT TERM WE CAN COMMIT, THIS IS SAFETY CONDITION IN RAFT
		cp := make([]int, len(rf.matchIndex))
		copy(cp, rf.matchIndex)
		sort.Sort(sort.Reverse(sort.IntSlice(cp)))
		if cp[len(cp)/2] > rf.LastIncludedIndex && cp[len(cp)/2] <= rf.getLastLogIndex() && rf.getLog(cp[len(cp)/2]).Term == rf.CurrentTerm {
			startIndex := max(rf.commitIndex+1, rf.LastIncludedIndex+1)
			endIndex := min(cp[len(cp)/2], rf.getLastLogIndex())
			for i := startIndex; i <= endIndex; i++ {
				applyMsg := ApplyMsg{}
				applyMsg.Command = rf.getLog(i).Command
				applyMsg.CommandIndex = i + 1
				applyMsg.CommandValid = true
				rf.commitMsgs = append(rf.commitMsgs, applyMsg)
			}
			rf.commitIndex = max(rf.commitIndex, endIndex)
		}

		rf.persist()
		rf.mu.Unlock()
	} else {
		if reply.FirstTermIndex <= rf.LastIncludedIndex {
			// we need to install snapshot
			newArgs, newReply := rf.getInstallSnapshotPara()
			rf.mu.Unlock()
			return rf.sendInstallSnapshot(server, newArgs, newReply)
		} else {
			// retry
			if reply.FirstTermIndex <= rf.getLastLogIndex() {
				rf.nextIndex[server] = reply.FirstTermIndex
				newArgs, newReply := rf.getAppendEntriesPara(server)
				rf.mu.Unlock()
				return rf.sendAppendEntries(server, newArgs, newReply)
			}
			rf.mu.Unlock()
		}
	}

	return true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		// has been killed
		return false
	}
	// optimization: save unnecessary rpc call
	rf.mu.Lock()
	if args.Term != rf.CurrentTerm {
		rf.mu.Unlock()
		return false
	}
	DPrintf("node %v with arg %v tries to install snapshot to %v, current state is %v\n",
		args.LeaderId, args, server, rf)

	// persist before sending an RPC
	rf.persist()

	rf.mu.Unlock()

	// make rpc call
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()

	DPrintf("node %v with arg %v tries to install snapshot to %v, got response %v and rpc response %v\n",
		args.LeaderId, args, server, reply, ok)

	if !ok || args.Term != rf.CurrentTerm || rf.State != LEADER {
		rf.mu.Unlock()
		return false
	}

	if reply.Term > rf.CurrentTerm {
		// if got larger term, fall back to follower
		// reset other state
		rf.fallback(reply.Term)
		rf.mu.Unlock()
		return false
	}

	// update matchIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex

	// retry to make append entries
	if rf.nextIndex[server] > rf.LastIncludedIndex {
		newArgs, newReply := rf.getAppendEntriesPara(server)
		rf.mu.Unlock()
		return rf.sendAppendEntries(server, newArgs, newReply)
	}

	rf.mu.Unlock()
	return false
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != LEADER || rf.killed() {
		return 0, 0, false
	}

	entry := LogEntry{rf.CurrentTerm, command}
	rf.Log = append(rf.Log, entry)

	index := rf.getLastLogIndex()
	rf.matchIndex[rf.me] = index
	DPrintf("Start a command in server %v with state %v, current term is %v and new log entry len is %v and "+
		"index is %v\n",
		rf.me, rf.State, rf.CurrentTerm, len(rf.Log), index+1)

	rf.persist()
	rf.broadcastHeartbeat()
	rf.Ticker = rf.getHeartbeatTime()

	return index + 1, rf.CurrentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
	// we'll set it to 120 ms
	heartbeatInterval := rf.getHeartbeatTime()

	// interval 5ms
	tickInterval := int32(30)

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		rf.Ticker -= tickInterval
		switch rf.State {
		case FOLLOWER:
			if rf.Ticker < 0 {
				// if no heartbeat
				rf.becomeCandidate()
			}
		case CANDIDATE:
			// retry
			if rf.Ticker < 0 {
				DPrintf("Server %v tries to become candidate in term %v", rf.me, rf.CurrentTerm)
				rf.becomeCandidate()
			}
		case LEADER:
			if rf.Ticker < 0 {
				rf.broadcastHeartbeat()
				rf.Ticker = heartbeatInterval
			}
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(tickInterval) * time.Millisecond)
	}
}

func (rf *Raft) commitCommand() {
	for rf.killed() == false {
		time.Sleep(time.Duration(2) * time.Millisecond)
		rf.mu.Lock()
		if len(rf.commitMsgs) == 0 {
			rf.mu.Unlock()
			continue
		}
		applyMsgs := make([]ApplyMsg, len(rf.commitMsgs))
		copy(applyMsgs, rf.commitMsgs)
		rf.commitMsgs = rf.commitMsgs[:0]
		rf.mu.Unlock()
		for _, msg := range applyMsgs {
			rf.applyCh <- msg
			DPrintf("Server %v: commit msg %v", rf.me, msg)
		}
	}
}

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetSnapshot() []byte {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.ReadSnapshot()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.State = FOLLOWER
	rf.VotedFor = -1
	rf.Ticker = rf.getRandElecTime()
	rf.VoteNum = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitMsgs = make([]ApplyMsg, 0)

	// initial value is -1 to match with slice index
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.LastIncludedTerm = -1
	rf.LastIncludedIndex = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.NodeSnapshot = persister.ReadSnapshot()

	// initialize other states
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.LastIncludedIndex + 1
		rf.matchIndex[i] = -1
	}
	rf.commitIndex = -1
	rf.lastApplied = -1

	// start ticker goroutine to start elections
	go rf.ticker()

	// start commit goroutine
	go rf.commitCommand()

	return rf
}

// Helper functions
func (rf *Raft) fallback(term int) {
	// fallback to follower, reset everything
	// NOTE: THIS FUNCTION SHOULD BE WRAPPED WITH MUTEX LOCKED

	rf.State = FOLLOWER
	rf.CurrentTerm = term
	// NOTE: WE CANNOT SET HEARTBEAT AS TRUE FOR REQUEST VOTE
	// OR SOME STALE LEADER CAN CAUSE LIVELINESS ISSUE
	rf.VotedFor = -1
	rf.VoteNum = 0
}

func (rf *Raft) refresh() {
	// refresh when recv heartbeat
	// NOTE: THIS FUNCTION SHOULD BE CALLED WITH MUTEX LOCKED
	rf.Ticker = rf.getRandElecTime()
}

func (rf *Raft) becomeLeader() {
	// become leader
	// NOTE: THIS FUNCTION SHOULD BE CALLED WITH MUTEX LOCKED

	rf.State = LEADER
	rf.Ticker = rf.getHeartbeatTime()
	rf.VotedFor = rf.me
	rf.VoteNum = 0
	rf.commitIndex = -1
	rf.lastApplied = -1

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = -1
	}
}

func (rf *Raft) addNoop() {
	// add no-op log
	noop := LogEntry{}
	noop.Term = rf.CurrentTerm
	noop.Command = nil
	rf.Log = append(rf.Log, noop)
}

func (rf *Raft) broadcastHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if rf.nextIndex[i] <= rf.LastIncludedIndex {
				newArgs, newReply := rf.getInstallSnapshotPara()
				go rf.sendInstallSnapshot(i, newArgs, newReply)
			} else {
				// retry
				newArgs, newReply := rf.getAppendEntriesPara(i)
				go rf.sendAppendEntries(i, newArgs, newReply)
			}
		}
	}
}

func (rf *Raft) becomeCandidate() {
	// become candidate
	// NOTE: THIS FUNCTION SHOULD BE CALLED WITH MUTEX LOCKED
	rf.State = CANDIDATE
	rf.CurrentTerm++
	rf.refresh()
	rf.VotedFor = rf.me
	rf.VoteNum = 1 // vote for itself

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// send request vote to other nodes
			args := &RequestVoteArgs{}
			args.CandidateId = rf.me
			args.Term = rf.CurrentTerm
			args.LastLogIndex = rf.getLastLogIndex()
			args.LastLogTerm = rf.getLastLogTerm()
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

func max(n1 int, n2 int) int {
	if n1 > n2 {
		return n1
	}

	return n2
}

func (rf *Raft) getLogIndex(curIdx int) int {
	if rf.LastIncludedIndex == -1 {
		// no snapshot yet
		return curIdx
	}

	return curIdx + rf.LastIncludedIndex + 1
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.Log) > 0 {
		// have log entry
		return rf.getLogIndex(len(rf.Log) - 1)
	}

	// no log entry
	return rf.LastIncludedIndex
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.Log) > 0 {
		// have log entry
		return rf.Log[len(rf.Log)-1].Term
	}

	// no log entry
	return rf.LastIncludedTerm
}

func (rf *Raft) getRelativeIndex(index int) int {
	if rf.LastIncludedIndex < 0 {
		return index
	}
	return index - rf.LastIncludedIndex - 1
}

func (rf *Raft) getLog(logIndex int) LogEntry {
	return rf.Log[rf.getRelativeIndex(logIndex)]
}

func (rf *Raft) getRandElecTime() int32 {
	electionTimeout := rand.Intn(300) + 300
	return int32(electionTimeout)
}

func (rf *Raft) getHeartbeatTime() int32 {
	return int32(120)
}

func (rf *Raft) getAppendEntriesPara(server int) (*AppendEntriesArgs, *AppendEntriesReply) {
	// NOTE: THIS FUNCTION SHOULD BE CALLED WITH MUTEX LOCKED
	// send heartbeat to other nodes, also try to update log
	args := &AppendEntriesArgs{}
	args.LeaderId = rf.me
	args.Term = rf.CurrentTerm
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex < rf.LastIncludedIndex {
		panic("Prev log index is smaller than last included index, something must be wrong")
	} else if args.PrevLogIndex == rf.LastIncludedIndex {
		args.PrevLogTerm = rf.LastIncludedTerm
	} else {
		args.PrevLogTerm = rf.getLog(args.PrevLogIndex).Term
	}
	args.LeaderCommit = rf.commitIndex
	if args.PrevLogIndex < rf.getLastLogIndex() && len(rf.Log) > 0 {
		// we should make a copy of log
		DPrintf("server %v try to copy log into arg, with state %v", rf.me, rf)
		relativeIdx := rf.getRelativeIndex(args.PrevLogIndex + 1)
		args.Entries = make([]LogEntry, len(rf.Log[relativeIdx:]))
		copy(args.Entries, rf.Log[relativeIdx:])
	}

	reply := &AppendEntriesReply{}
	return args, reply
}

func (rf *Raft) getInstallSnapshotPara() (*InstallSnapshotArgs, *InstallSnapshotReply) {
	// send snapshot to other nodes
	if rf.LastIncludedIndex == -1 {
		// no snapshot, something must be wrong
		panic("No snapshot but try to make install snapshot call, something must be wrong")
	}

	args := &InstallSnapshotArgs{}
	args.LeaderId = rf.me
	args.Term = rf.CurrentTerm
	args.LastIncludedIndex = rf.LastIncludedIndex
	args.LastIncludedTerm = rf.LastIncludedTerm
	args.Data = make([]byte, len(rf.NodeSnapshot))
	copy(args.Data, rf.NodeSnapshot)

	reply := &InstallSnapshotReply{}

	return args, reply
}
