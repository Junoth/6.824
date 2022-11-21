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
	term       int // we should record the raft term info for the request
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
	context.term, _ = kv.rf.GetState()
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

	kv.mu.Lock()
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
	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		// save snapshot
		kv.MakeSnapshot()
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
			if op.Operation == PUT || op.Operation == APPEND {
				if context, ok := kv.CommitMap[op.ClientId]; ok {
					if context.CommitOp.RequestId >= op.RequestId {
						DPrintf("Server %v: context %v is in committed state, skip", kv.me, context)
						kv.mu.Unlock()
						continue
					}
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
			if op.Operation == PUT || op.Operation == APPEND {
				commitContext := CommitContext{}
				commitContext.Value = value
				commitContext.HasValue = hasValue
				commitContext.CommitOp = op
				kv.CommitMap[op.ClientId] = commitContext
			}

			if context, ok := kv.contextMap[op.ClientId]; ok && context.op.RequestId == op.RequestId {
				if term, isLeader := kv.rf.GetState(); term == context.term && isLeader {
					// Refer to https://github.com/OneSizeFitsQuorum/MIT6.824-2021/blob/master/docs/lab3.md
					// 1. 仅对 leader 的 notifyChan 进行通知：目前的实现中读写请求都需要路由给 leader 去处理，所以在执行日志到状态机后，只有 leader 需将执行结果通过 notifyChan 唤醒阻塞的客户端协程，而 follower 则不需要；对于 leader 降级为 follower 的情况，该节点在 apply 日志后也不能对之前靠 index 标识的 channel 进行 notify，因为可能执行结果是不对应的，所以在加上只有 leader 可以 notify 的判断后，对于此刻还阻塞在该节点的客户端协程，就可以让其自动超时重试。如果读者足够细心，也会发现这里的机制依然可能会有问题，下一点会提到。
					// 2. 仅对当前 term 日志的 notifyChan 进行通知：上一点提到，对于 leader 降级为 follower 的情况，该节点需要让阻塞的请求超时重试以避免违反线性一致性。那么有没有这种可能呢？leader 降级为 follower 后又迅速重新当选了 leader，而此时依然有客户端协程未超时在阻塞等待，那么此时 apply 日志后，根据 index 获得 channel 并向其中 push 执行结果就可能出错，因为可能并不对应。对于这种情况，最直观地解决方案就是仅对当前 term 日志的 notifyChan 进行通知，让之前 term 的客户端协程都超时重试即可。当然，目前客户端超时重试的时间是 500ms，选举超时的时间是 1s，所以严格来说并不会出现这种情况，但是为了代码的鲁棒性，最好还是这么做，否则以后万一有人将客户端超时时间改到 5s 就可能出现这种问题了。
					context.hasValue = hasValue
					context.value = value
					DPrintf("Server %v: context %v committed", kv.me, context)
					delete(kv.contextMap, op.ClientId)
					close(context.commitChan)
				}
			}
			kv.CheckSnapshot()
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
