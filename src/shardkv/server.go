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

func (kv *ShardKV) DPrintf(format string, a ...interface{}) (n int) {
	if Debug && !kv.killed() {
		log.Printf(format, a...)
	}
	return
}

const (
	GET      = "GET"
	PUT      = "PUT"
	APPEND   = "APPEND"
	MOVE     = "MOVE"
	GETSHARD = "GETSHARD"
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
	Shard      int
	MoveShards map[string]string
	NewConfig  shardctrler.Config
}

type RequestContext struct {
	commitChan chan byte
	op         Op
	hasValue   bool
	value      string
	shardValue map[string]string
}

type CommitContext struct {
	CommitOp   Op
	Value      string
	HasValue   bool
	ShardValue map[string]string
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
	ShardConfig      shardctrler.Config
	LastCommitConfig int
	GroupLeader      map[int]int
	ServerId         int64
	SeqId            int64
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
		enc.Encode(kv.ShardConfig) != nil ||
		enc.Encode(kv.LastCommitConfig) != nil ||
		enc.Encode(kv.GroupLeader) != nil ||
		enc.Encode(kv.ServerId) != nil ||
		enc.Encode(kv.SeqId) != nil {
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
		GroupLeader      map[int]int
		ServerId         int64
		SeqId            int64
	)
	if dec.Decode(&KvState) != nil ||
		dec.Decode(&CommitMap) != nil ||
		dec.Decode(&CommitIndex) != nil ||
		dec.Decode(&ShardConfig) != nil ||
		dec.Decode(&LastCommitConfig) != nil ||
		dec.Decode(&GroupLeader) != nil ||
		dec.Decode(&ServerId) != nil ||
		dec.Decode(&SeqId) != nil {
		panic("Fail to decode")
	} else {
		kv.KvState = KvState
		kv.CommitMap = CommitMap
		kv.CommitIndex = CommitIndex
		kv.ShardConfig = ShardConfig
		kv.LastCommitConfig = LastCommitConfig
		kv.GroupLeader = GroupLeader
		kv.ServerId = ServerId
		kv.SeqId = SeqId
	}

	kv.DPrintf("Server %v: read snapshot from persister, state is %v", kv.me, kv)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// make an op
	op := Op{}
	if args.GetShard {
		op.Operation = GETSHARD
		op.Shard = args.Shard
	} else {
		op.Operation = GET
		op.Key = args.Key
	}
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId

	kv.DPrintf("Server %v: received op %v", kv.me, args)

	kv.mu.Lock()
	if args.ConfigNum != kv.ShardConfig.Num {
		kv.DPrintf("Server %v: arg %v config num not matched with server num %v", kv.me, args, kv.ShardConfig.Num)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// check if the same request has been applied
	if context, ok := kv.CommitMap[args.ClientId]; ok && context.CommitOp.RequestId == args.RequestId {
		kv.DPrintf("Server %v: op %v has been committed before", kv.me, op)
		reply.Err = OK
		if op.Operation == GET {
			reply.Value = context.Value
		} else {
			reply.Shard = context.ShardValue
		}
		kv.mu.Unlock()
		return
	}

	if _, state := kv.rf.GetState(); !state {
		kv.DPrintf("Server %v: not the leader for op %v", kv.me, op)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	if op.Operation == GET && !kv.IsShardInGroup(key2shard(op.Key)) {
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
		if op.Operation == GET {
			if context.hasValue {
				reply.Err = OK
				reply.Value = context.value
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			reply.Err = OK
			reply.Shard = context.shardValue
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

	kv.DPrintf("Server %v: received op %v", kv.me, args)

	kv.mu.Lock()
	if args.ConfigNum != kv.ShardConfig.Num {
		kv.DPrintf("Server %v: arg %v config num not matched with server num %v", kv.me, args, kv.ShardConfig.Num)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// check if the same request has been applied
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
	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		// save snapshot
		kv.MakeSnapshot()
	}
}

func (kv *ShardKV) CheckShardGroup() {
	for kv.killed() == false {
		newConfig := kv.mck.Query(-1)
		op := Op{}
		op.Operation = MOVE
		op.NewConfig = newConfig
		op.MoveShards = make(map[string]string)

		// we should make an RPC call to remote server for new shards
		kv.mu.Lock()

		if newConfig.Num <= kv.LastCommitConfig {
			goto sleep
		}

		if _, isLeader := kv.rf.GetState(); !isLeader {
			goto sleep
		}

		for i, gid := range newConfig.Shards {
			if kv.ShardConfig.Num != 0 && gid == kv.gid && kv.ShardConfig.Shards[i] != kv.gid {
				// this shard is new, we should ask for the data
				// make RPC call
				args := GetArgs{}
				args.ClientId = kv.ServerId
				args.RequestId = kv.SeqId
				args.GetShard = true
				args.Shard = i
				args.ConfigNum = newConfig.Num

				kv.SeqId++
				if servers, ok := kv.ShardConfig.Groups[kv.ShardConfig.Shards[i]]; ok {
					if leader, ok := kv.GroupLeader[gid]; ok {
						// found leader info, try to call directly
						srv := kv.make_end(servers[leader])

						kv.mu.Unlock()
						// here we need a new RPC call
						var reply GetReply
						ok := srv.Call("ShardKV.Get", &args, &reply)
						kv.mu.Lock()
						if ok && reply.Err == OK {
							for k, v := range reply.Shard {
								op.MoveShards[k] = v
							}
							continue
						}
					}

					moveSuccess := false
					for index, server := range servers {
						srv := kv.make_end(server)

						kv.mu.Unlock()
						var reply GetReply
						ok := srv.Call("ShardKV.Get", &args, &reply)
						kv.mu.Lock()
						if ok && reply.Err == OK {
							for k, v := range reply.Shard {
								op.MoveShards[k] = v
							}
							kv.GroupLeader[gid] = index
							moveSuccess = true
							break
						}
					}

					if !moveSuccess {
						// not able to get shard, we may get outdated config, try to sleep and retry later
						goto sleep
					}
				}
			}
		}

		if newConfig.Num > kv.LastCommitConfig && newConfig.Num > kv.ShardConfig.Num {
			_, _, res := kv.rf.Start(op) // only leader will start the op
			if res {
				kv.LastCommitConfig = newConfig.Num
				kv.DPrintf("Server %v start the op %v and current leader is %v", kv.me, op, kv.rf.State)
			}
		}
	sleep:
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
				if op.NewConfig.Num > kv.ShardConfig.Num {
					kv.DPrintf("Server %v, update config from %v to %v and add shard %v", kv.me, kv.ShardConfig, op.NewConfig, op.MoveShards)
					kv.ShardConfig = op.NewConfig
					for k, v := range op.MoveShards {
						kv.KvState[k] = v
					}
				}
				kv.CheckSnapshot()
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
			shardValue := make(map[string]string)
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
			case GETSHARD:
				for k, v := range kv.KvState {
					if key2shard(k) == op.Shard {
						shardValue[k] = v
					}
				}
			}
			commitContext := CommitContext{}
			commitContext.Value = value
			commitContext.ShardValue = shardValue
			commitContext.HasValue = hasValue
			commitContext.CommitOp = op
			kv.CommitMap[op.ClientId] = commitContext

			if context, ok := kv.contextMap[op.ClientId]; ok && context.op.RequestId == op.RequestId {
				context.hasValue = hasValue
				context.value = value
				context.shardValue = shardValue
				kv.DPrintf("Server %v: context %v committed", kv.me, context)
				delete(kv.contextMap, op.ClientId)
				close(context.commitChan)
			}
			kv.CheckSnapshot()
			kv.mu.Unlock()
		}
	}
}

// Kill the tester calls Kill() when a ShardKV instance won't
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

// StartServer servers[] contains the ports of the servers in this group.
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
	kv.LastCommitConfig = -1
	kv.ServerId = nrand()
	kv.SeqId = 0
	kv.GroupLeader = make(map[int]int)

	kv.ReadSnapshot()

	// You may need initialization code here.
	go kv.RunStateMachine()

	go kv.CheckShardGroup()

	return kv
}

// IsShardInGroup utils, this should be called when locked
func (kv *ShardKV) IsShardInGroup(shard int) bool {
	return kv.ShardConfig.Shards[shard] == kv.gid
}

// CopyConfig utils, this should be called when locked
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
