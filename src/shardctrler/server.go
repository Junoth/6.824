package shardctrler

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"log"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	CommitIndex int
	dead        int32    // set by Kill()
	Configs     []Config // indexed by config num
	RequestMap  map[int64]*RequestContext
	CommitMap   map[int64]CommitContext
}

type Op struct {
	// Your data here.
	Operation string
	RequestId int64
	ClientId  int64
	OpArg     interface{}
}

func (sc *ShardCtrler) NewRequestContext(op Op) RequestContext {
	context := RequestContext{}
	context.commitChan = make(chan byte)
	context.op = op
	return context
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// make an op
	op := Op{}
	op.Operation = JOIN
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.OpArg = args

	DPrintf("server %v received join request %v", sc.me, args)

	// check if the same request has been applied
	sc.mu.Lock()
	if ctx, ok := sc.CommitMap[args.ClientId]; ok && ctx.CommitOp.RequestId == args.RequestId {
		DPrintf("Server %v: op %v has been committed before", sc.me, op)
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	// add context into map
	context := sc.NewRequestContext(op)
	sc.RequestMap[context.op.ClientId] = &context
	sc.mu.Unlock()

	// start command in raft
	_, _, res := sc.rf.Start(op)
	if !res {
		// current server is not leader
		reply.Err = ErrWrongLeader
		return
	}

	if ok := sc.CheckCommit(&context); ok {
		sc.mu.Lock()
		DPrintf("Server %v: op %v has been committed", sc.me, op)
	} else {
		sc.mu.Lock()
		DPrintf("Server %v: op %v commit fail", sc.me, op)
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{}
	op.Operation = LEAVE
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.OpArg = args

	DPrintf("server %v received leave request %v", sc.me, args)

	// check if the same request has been applied
	sc.mu.Lock()
	if ctx, ok := sc.CommitMap[args.ClientId]; ok && ctx.CommitOp.RequestId == args.RequestId {
		DPrintf("Server %v: op %v has been committed before", sc.me, op)
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	// add context into map
	context := sc.NewRequestContext(op)
	sc.RequestMap[context.op.ClientId] = &context
	sc.mu.Unlock()

	// start command in raft
	_, _, res := sc.rf.Start(op)
	if !res {
		// current server is not leader
		reply.Err = ErrWrongLeader
		return
	}

	if ok := sc.CheckCommit(&context); ok {
		sc.mu.Lock()
		DPrintf("Server %v: op %v has been committed", sc.me, op)
	} else {
		sc.mu.Lock()
		DPrintf("Server %v: op %v commit fail", sc.me, op)
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{}
	op.Operation = MOVE
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.OpArg = args

	DPrintf("server %v received move request %v", sc.me, args)

	// check if the same request has been applied
	sc.mu.Lock()
	if ctx, ok := sc.CommitMap[args.ClientId]; ok && ctx.CommitOp.RequestId == args.RequestId {
		DPrintf("Server %v: op %v has been committed before", sc.me, op)
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	// add context into map
	context := sc.NewRequestContext(op)
	sc.RequestMap[context.op.ClientId] = &context
	sc.mu.Unlock()

	// start command in raft
	_, _, res := sc.rf.Start(op)
	if !res {
		// current server is not leader
		reply.Err = ErrWrongLeader
		return
	}

	if ok := sc.CheckCommit(&context); ok {
		sc.mu.Lock()
		DPrintf("Server %v: op %v has been committed", sc.me, op)
	} else {
		sc.mu.Lock()
		DPrintf("Server %v: op %v commit fail", sc.me, op)
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{}
	op.Operation = MOVE
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.OpArg = args

	DPrintf("server %v received move request %v", sc.me, args)

	// check if the same request has been applied
	sc.mu.Lock()
	if ctx, ok := sc.CommitMap[args.ClientId]; ok && ctx.CommitOp.RequestId == args.RequestId {
		DPrintf("Server %v: op %v has been committed before", sc.me, op)
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	// add context into map
	context := sc.NewRequestContext(op)
	sc.RequestMap[context.op.ClientId] = &context
	sc.mu.Unlock()

	// start command in raft
	_, _, res := sc.rf.Start(op)
	if !res {
		// current server is not leader
		reply.Err = ErrWrongLeader
		return
	}

	if ok := sc.CheckCommit(&context); ok {
		reply.Config = context.Value
		DPrintf("Server %v: op %v has been committed", sc.me, op)
	} else {
		reply.Err = ErrWrongLeader
		DPrintf("Server %v: op %v commit fail", sc.me, op)
	}
}

func (sc *ShardCtrler) MakeSnapshot() {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	if enc.Encode(sc.Configs) != nil ||
		enc.Encode(sc.CommitMap) != nil ||
		enc.Encode(sc.CommitIndex) != nil {
		panic("Fail to encode")
	}
	data := buf.Bytes()

	DPrintf("Server %v: make snapshot into persister, state is %v", sc.me, sc)

	sc.rf.Snapshot(sc.CommitIndex, data)
}

func (sc *ShardCtrler) ReadSnapshot() {
	data := sc.rf.GetSnapshot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var (
		Configs     []Config
		CommitMap   map[int64]CommitContext
		CommitIndex int
	)
	if dec.Decode(&Configs) != nil ||
		dec.Decode(&CommitMap) != nil ||
		dec.Decode(&CommitIndex) != nil {
		panic("Fail to decode")
	} else {
		sc.Configs = Configs
		sc.CommitMap = CommitMap
		sc.CommitIndex = CommitIndex
	}

	DPrintf("Server %v: read snapshot from persister, state is %v", sc.me, sc)
}

func (sc *ShardCtrler) CheckCommit(context *RequestContext) bool {
	select {
	case _ = <-context.commitChan:
		// committed
		return true
	case <-time.After(2 * time.Second):
		// timeout
		return false
	}
}

func (sc *ShardCtrler) RunStateMachine() {
	for m := range sc.applyCh {
		if sc.killed() {
			break
		}

		if m.SnapshotValid {
			sc.mu.Lock()
			if sc.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				if m.SnapshotIndex > sc.CommitIndex {
					DPrintf("Server %v: cond install snapshot %v", sc.me, m)
					sc.ReadSnapshot()
					sc.CommitIndex = m.SnapshotIndex
				}
			}
			sc.mu.Unlock()
		}

		if m.CommandValid {
			op := m.Command.(Op)

			sc.mu.Lock()

			if m.CommandIndex <= sc.CommitIndex {
				sc.mu.Unlock()
				continue
			}

			sc.CommitIndex = m.CommandIndex

			// check if the same request has been applied once or if this is current request of client
			if context, ok := sc.CommitMap[op.ClientId]; ok {
				if context.CommitOp.RequestId >= op.RequestId {
					DPrintf("Server %v: context %v is in committed state, skip", sc.me, context)
					sc.mu.Unlock()
					continue
				}
			}

			var value Config
			switch op.Operation {
			case JOIN:
				// TODO: add JOIN operation
			case LEAVE:
				// TODO: add LEAVE operation
			case MOVE:
				// TODO: add MOVE operation
			case QUERY:
				arg := op.OpArg.(QueryArgs)
				if arg.Num == -1 || arg.Num >= len(sc.Configs) {
					value = sc.Configs[len(sc.Configs)-1]
				} else {
					value = sc.Configs[arg.Num]
				}
			}
			commitContext := CommitContext{}
			commitContext.CommitOp = op
			commitContext.Value = sc.Configs
			sc.CommitMap[op.ClientId] = commitContext

			if context, ok := sc.RequestMap[op.ClientId]; ok && context.op.RequestId == op.RequestId {
				context.Value = value
				DPrintf("Server %v: context %v committed", sc.me, context)
				delete(sc.RequestMap, op.ClientId)
				close(context.commitChan)
			}
			sc.mu.Unlock()
		}
	}
}

// Kill the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.Configs = make([]Config, 1)
	sc.Configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.CommitIndex = -1

	return sc
}
