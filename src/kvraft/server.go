package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

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

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	RequestId int64
	ClientId  int64
	Key       string
	Value     string
}

type RequestContext struct {
	commitChan chan byte
	op         Op
	hasValue   bool
	value      string
}

type CommitContext struct {
	CommitOp Op
	Value    string
	HasValue bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	CommitIndex int
	KvState     map[string]string
	contextMap  map[int64]*RequestContext
	CommitMap   map[int64]CommitContext
}

func (kv *KVServer) NewCommitContext(op Op) RequestContext {
	context := RequestContext{}
	context.commitChan = make(chan byte)
	context.op = op
	context.hasValue = false
	context.value = ""
	return context
}

func (kv *KVServer) MakeSnapshot() {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	if enc.Encode(kv.KvState) != nil ||
		enc.Encode(kv.CommitMap) != nil ||
		enc.Encode(kv.CommitIndex) != nil {
		panic("Fail to encode")
	}
	data := buf.Bytes()

	DPrintf("Server %v: make snapshot into persister, state is %v", kv.me, kv)

	kv.rf.Snapshot(kv.CommitIndex, data)
}

func (kv *KVServer) ReadSnapshot() {
	data := kv.rf.GetSnapshot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var (
		KvState     map[string]string
		CommitMap   map[int64]CommitContext
		CommitIndex int
	)
	if dec.Decode(&KvState) != nil ||
		dec.Decode(&CommitMap) != nil ||
		dec.Decode(&CommitIndex) != nil {
		panic("Fail to decode")
	} else {
		kv.KvState = KvState
		kv.CommitMap = CommitMap
		kv.CommitIndex = CommitIndex
	}

	DPrintf("Server %v: read snapshot from persister, state is %v", kv.me, kv)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// make an op
	op := Op{}
	op.Operation = GET
	op.RequestId = args.RequestId
	op.Key = args.Key
	op.ClientId = args.ClientId

	// check if the same request has been applied
	kv.mu.Lock()
	if context, ok := kv.CommitMap[args.ClientId]; ok && context.CommitOp.RequestId == args.RequestId {
		DPrintf("Server %v: op %v has been committed before", kv.me, op)
		reply.Err = OK
		reply.Value = context.Value
		kv.mu.Unlock()
		return
	}

	if _, state := kv.rf.GetState(); !state {
		DPrintf("Server %v: not the leader for op %v", kv.me, op)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// add context into map
	context := kv.NewCommitContext(op)
	kv.contextMap[context.op.ClientId] = &context
	kv.mu.Unlock()

	// start command in raft
	_, _, res := kv.rf.Start(op)
	if !res {
		// current server is not leader
		reply.Err = ErrWrongLeader
		return
	}

	if ok := kv.CheckCommit(&context); ok {
		kv.mu.Lock()
		DPrintf("Server %v: context %v has been committed", kv.me, context)
		if context.hasValue {
			reply.Err = OK
			reply.Value = context.value
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		kv.mu.Lock()
		DPrintf("Server %v: context %v commit timeout", kv.me, context)
		reply.Err = ErrWrongLeader
	}

	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// make an op
	op := Op{}
	if args.Op == "Put" {
		op.Operation = PUT
	} else if args.Op == "Append" {
		op.Operation = APPEND
	} else {
		panic("Invalid operation")
	}
	op.RequestId = args.RequestId
	op.Key = args.Key
	op.Value = args.Value
	op.ClientId = args.ClientId

	// check if the same request has been applied
	kv.mu.Lock()
	if context, ok := kv.CommitMap[args.ClientId]; ok && context.CommitOp.RequestId == args.RequestId {
		DPrintf("Server %v: op %v has been committed before", kv.me, op)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	if _, state := kv.rf.GetState(); !state {
		DPrintf("Server %v: not the leader for op %v", kv.me, op)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// add context into map
	context := kv.NewCommitContext(op)
	kv.contextMap[context.op.ClientId] = &context
	kv.mu.Unlock()

	// start command in raft
	_, _, res := kv.rf.Start(op)
	if !res {
		// current server is not leader
		reply.Err = ErrWrongLeader
		return
	}

	//DPrintf("Server %v: context %v has been started", kv.me, context)

	if ok := kv.CheckCommit(&context); ok {
		kv.mu.Lock()
		DPrintf("Server %v: context %v has been committed", kv.me, context)
		reply.Err = OK
	} else {
		kv.mu.Lock()
		DPrintf("Server %v: context %v commit timeout", kv.me, context)
		reply.Err = ErrWrongLeader
	}

	kv.mu.Unlock()
}

func (kv *KVServer) CheckCommit(context *RequestContext) bool {
	select {
	case _ = <-context.commitChan:
		// committed
		return true
	case <-time.After(2 * time.Second):
		// timeout
		return false
	}
}

func (kv *KVServer) CheckSnapshot() {
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
			// save snapshot
			kv.MakeSnapshot()
		}
		kv.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) RunStateMachine() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}

		if m.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				if m.SnapshotIndex > kv.CommitIndex {
					DPrintf("Server %v: cond install snapshot %v", kv.me, m)
					kv.ReadSnapshot()
					kv.CommitIndex = m.SnapshotIndex
				}
			}
			kv.mu.Unlock()
		}

		if m.CommandValid {
			op := m.Command.(Op)

			kv.mu.Lock()

			if m.CommandIndex <= kv.CommitIndex {
				kv.mu.Unlock()
				continue
			}

			kv.CommitIndex = m.CommandIndex

			// check if the same request has been applied once or if this is current request of client
			if context, ok := kv.CommitMap[op.ClientId]; ok {
				if context.CommitOp.RequestId >= op.RequestId {
					DPrintf("Server %v: context %v is in committed state, skip", kv.me, context)
					kv.mu.Unlock()
					continue
				}
			}

			hasValue := false
			value := ""
			switch op.Operation {
			case GET:
				if val, ok := kv.KvState[op.Key]; ok {
					hasValue = true
					value = val
				} else {
					hasValue = false
				}
			case PUT:
				kv.KvState[op.Key] = op.Value
			case APPEND:
				if val, ok := kv.KvState[op.Key]; ok {
					kv.KvState[op.Key] = val + op.Value
				} else {
					kv.KvState[op.Key] = op.Value
				}
			}
			commitContext := CommitContext{}
			commitContext.Value = value
			commitContext.HasValue = hasValue
			commitContext.CommitOp = op
			kv.CommitMap[op.ClientId] = commitContext

			if context, ok := kv.contextMap[op.ClientId]; ok && context.op.RequestId == op.RequestId {
				context.hasValue = hasValue
				context.value = value
				DPrintf("Server %v: context %v committed", kv.me, context)
				delete(kv.contextMap, op.ClientId)
				close(context.commitChan)
			}
			kv.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
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
	kv.KvState = make(map[string]string) // need snapshot
	kv.contextMap = make(map[int64]*RequestContext)
	kv.CommitMap = make(map[int64]CommitContext) // need snapshot
	kv.CommitIndex = -1

	kv.ReadSnapshot()

	go kv.CheckSnapshot()

	// You may need initialization code here.
	go kv.RunStateMachine()

	return kv
}

func max(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}
