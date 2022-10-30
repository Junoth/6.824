package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const Debug = false

func (kv *ShardKV) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug && !kv.killed() {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
	MOVE   = "MOVE"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation  string
	RequestId  int64
	ClientId   int64
	Key        string
	Value      string
	MoveShards map[string]string
	NewConfig  shardctrler.Config
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead             int32
	CommitIndex      int
	KvState          map[string]string
	contextMap       map[int64]*RequestContext
	CommitMap        map[int64]CommitContext
	mck              *shardctrler.Clerk
	shardConfig      shardctrler.Config
	lastCommitConfig int
}

func (kv *ShardKV) NewCommitContext(op Op) RequestContext {
	context := RequestContext{}
	context.commitChan = make(chan byte)
	context.op = op
	context.hasValue = false
	context.value = ""
	return context
}

func (kv *ShardKV) MakeSnapshot() {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	if enc.Encode(kv.KvState) != nil ||
		enc.Encode(kv.CommitMap) != nil ||
		enc.Encode(kv.CommitIndex) != nil ||
		enc.Encode(kv.shardConfig) != nil ||
		enc.Encode(kv.lastCommitConfig) != nil {
		panic("Fail to encode")
	}
	data := buf.Bytes()

	kv.DPrintf("Server %v: make snapshot into persister, state is %v", kv.me, kv)

	kv.rf.Snapshot(kv.CommitIndex, data)
}

func (kv *ShardKV) ReadSnapshot() {
	data := kv.rf.GetSnapshot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var (
		KvState          map[string]string
		CommitMap        map[int64]CommitContext
		CommitIndex      int
		ShardConfig      shardctrler.Config
		LastCommitConfig int
	)
	if dec.Decode(&KvState) != nil ||
		dec.Decode(&CommitMap) != nil ||
		dec.Decode(&CommitIndex) != nil ||
		dec.Decode(&kv.shardConfig) != nil ||
		dec.Decode(&kv.lastCommitConfig) != nil {
		panic("Fail to decode")
	} else {
		kv.KvState = KvState
		kv.CommitMap = CommitMap
		kv.CommitIndex = CommitIndex
		kv.shardConfig = ShardConfig
		kv.lastCommitConfig = LastCommitConfig
	}

	kv.DPrintf("Server %v: read snapshot from persister, state is %v", kv.me, kv)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// make an op
	op := Op{}
	op.Operation = GET
	op.RequestId = args.RequestId
	op.Key = args.Key
	op.ClientId = args.ClientId

	kv.DPrintf("Server %v: received op %v", kv.me, op)

	// check if the same request has been applied
	kv.mu.Lock()
	if context, ok := kv.CommitMap[args.ClientId]; ok && context.CommitOp.RequestId == args.RequestId {
		kv.DPrintf("Server %v: op %v has been committed before", kv.me, op)
		reply.Err = OK
		reply.Value = context.Value
		kv.mu.Unlock()
		return
	}

	if _, state := kv.rf.GetState(); !state {
		kv.DPrintf("Server %v: not the leader for op %v", kv.me, op)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if !kv.IsShardInGroup(key2shard(op.Key)) {
		kv.DPrintf("Server %v: op %v has shard not in group %v", kv.me, op, kv.gid)
		reply.Err = ErrWrongGroup
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
		kv.DPrintf("Server %v: context %v has been committed", kv.me, context)
		if context.hasValue {
			reply.Err = OK
			reply.Value = context.value
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		kv.mu.Lock()
		kv.DPrintf("Server %v: context %v commit timeout", kv.me, context)
		reply.Err = ErrWrongLeader
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

	kv.DPrintf("Server %v: received op %v", kv.me, op)

	// check if the same request has been applied
	kv.mu.Lock()
	if context, ok := kv.CommitMap[args.ClientId]; ok && context.CommitOp.RequestId == args.RequestId {
		kv.DPrintf("Server %v: op %v has been committed before", kv.me, op)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	if _, state := kv.rf.GetState(); !state {
		kv.DPrintf("Server %v: not the leader for op %v", kv.me, op)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if !kv.IsShardInGroup(key2shard(op.Key)) {
		kv.DPrintf("Server %v: op %v has shard not in group %v", kv.me, op, kv.gid)
		reply.Err = ErrWrongGroup
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
		kv.DPrintf("Server %v: not the leader for op %v", kv.me, op)
		// current server is not leader
		reply.Err = ErrWrongLeader
		return
	}

	//DPrintf("Server %v: context %v has been started", kv.me, context)

	if ok := kv.CheckCommit(&context); ok {
		kv.mu.Lock()
		kv.DPrintf("Server %v: context %v has been committed", kv.me, context)
		reply.Err = OK
	} else {
		kv.mu.Lock()
		kv.DPrintf("Server %v: context %v commit timeout", kv.me, context)
		reply.Err = ErrWrongLeader
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) CheckCommit(context *RequestContext) bool {
	select {
	case _ = <-context.commitChan:
		// committed
		return true
	case <-time.After(2 * time.Second):
		// timeout
		return false
	}
}

func (kv *ShardKV) CheckSnapshot() {
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

func (kv *ShardKV) CheckShardGroup() {
	for kv.killed() == false {
		newConfig := kv.mck.Query(-1)

		// we should make an RPC call to remote server for new shards
		kv.mu.Lock()
		// I'm current leader, try to get the data from remote
		//kv.mu.Unlock()

		// TODO: make a RPC call
		//for i, gid := range newConfig.Shards {
		//	if gid == kv.gid && kv.shardConfig.Shards {
		//
		//	}
		//}
		//
		//kv.mu.Lock()
		// TODO: send op command to raft
		if newConfig.Num > kv.lastCommitConfig && newConfig.Num > kv.shardConfig.Num {
			op := Op{}
			op.Operation = MOVE
			kv.CopyConfig(&op.NewConfig, &newConfig)
			_, _, res := kv.rf.Start(op) // only leader will start the op
			if res {
				kv.lastCommitConfig = newConfig.Num
				kv.DPrintf("Server %v start the op %v and current leader is %v", kv.me, op, kv.rf.State)
			}
		}

		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) RunStateMachine() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}

		if m.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				if m.SnapshotIndex > kv.CommitIndex {
					kv.DPrintf("Server %v: cond install snapshot %v", kv.me, m)
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

			// check if this is a MOVE op
			if op.Operation == MOVE {
				if op.NewConfig.Num > kv.shardConfig.Num {
					kv.CopyConfig(&kv.shardConfig, &op.NewConfig)
					kv.DPrintf("Server %v, update config to %v", kv.me, op.NewConfig)
				}
				kv.mu.Unlock()
				continue
			}

			// check if the same request has been applied once or if this is current request of client
			if context, ok := kv.CommitMap[op.ClientId]; ok {
				if context.CommitOp.RequestId >= op.RequestId {
					kv.DPrintf("Server %v: context %v is in committed state, skip", kv.me, context)
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
				kv.DPrintf("Server %v: context %v committed", kv.me, context)
				delete(kv.contextMap, op.ClientId)
				close(context.commitChan)
			}
			kv.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.KvState = make(map[string]string) // need snapshot
	kv.contextMap = make(map[int64]*RequestContext)
	kv.CommitMap = make(map[int64]CommitContext) // need snapshot
	kv.CommitIndex = -1
	kv.lastCommitConfig = -1

	kv.ReadSnapshot()

	go kv.CheckSnapshot()

	// You may need initialization code here.
	go kv.RunStateMachine()

	go kv.CheckShardGroup()

	return kv
}

// utils, this should be called when locked
func (kv *ShardKV) IsShardInGroup(shard int) bool {
	return kv.shardConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) CopyConfig(dst *shardctrler.Config, src *shardctrler.Config) {
	dst.Num = src.Num
	for i, v := range src.Shards {
		dst.Shards[i] = v
	}
	dst.Groups = make(map[int][]string)
	for k, v := range src.Groups {
		copy(dst.Groups[k], v)
	}
}
