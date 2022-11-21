package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const Debug = false

func (kv *ShardKV) DPrintf(format string, a ...interface{}) (n int) {
	newFormat := fmt.Sprintf("[Group: %v][Server: %v]: ", kv.gid, kv.me)
	if Debug && !kv.killed() {
		log.Printf(newFormat+format, a...)
	}
	return
}

type RequestContext struct {
	commitChan chan byte
	op         Op
	Result     interface{}
	term       int
}

type GetResult struct {
	Err   Err
	value string
}

type PutAppendResult struct {
	Err Err
}

type GetShardResult struct {
	Err        Err
	ShardIndex []int
	Shards     []Shard
}

type CommitContext struct {
	RequestId int64
	Err       Err
	Value     string
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
	dead               int32
	CommitIndex        int
	Shards             []Shard
	contextMap         map[int64]*RequestContext
	mck                *shardctrler.Clerk
	LastShardConfig    shardctrler.Config
	CurrentShardConfig shardctrler.Config

	// Used to connect with other groups
	ServerId int64
	SeqNum   int64
}

// Snapshot related
func (kv *ShardKV) MakeSnapshot() {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	if enc.Encode(kv.Shards) != nil ||
		enc.Encode(kv.CommitIndex) != nil ||
		enc.Encode(kv.CurrentShardConfig) != nil ||
		enc.Encode(kv.LastShardConfig) != nil ||
		enc.Encode(kv.ServerId) != nil ||
		enc.Encode(kv.SeqNum) != nil {
		panic("Fail to encode")
	}
	data := buf.Bytes()

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
		Shards             []Shard
		CommitIndex        int
		CurrentShardConfig shardctrler.Config
		LastShardConfig    shardctrler.Config
		ServerId           int64
		SeqNum             int64
	)
	if dec.Decode(&Shards) != nil ||
		dec.Decode(&CommitIndex) != nil ||
		dec.Decode(&CurrentShardConfig) != nil ||
		dec.Decode(&LastShardConfig) != nil ||
		dec.Decode(&ServerId) != nil ||
		dec.Decode(&SeqNum) != nil {
		panic("Fail to decode")
	} else {
		kv.Shards = Shards
		kv.CommitIndex = CommitIndex
		kv.CurrentShardConfig = CurrentShardConfig
		kv.LastShardConfig = LastShardConfig
		kv.ServerId = ServerId
		kv.SeqNum = SeqNum
	}
}

func (kv *ShardKV) CheckSnapshot() {
	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		// save snapshot
		kv.MakeSnapshot()
	}
}

// Exposed operations to outside
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// make an op
	op := Op{}
	op.Operation = GET
	op.Data = GetData{args.RequestId, args.ClientId, args.Key}

	if _, state := kv.rf.GetState(); !state {
		kv.DPrintf("not the leader for op %v", op)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	// pre-check, reduce raft log
	if !kv.CanServe(args.Key) {
		kv.DPrintf("cannot serve GET request %v and current config is %v", args, kv.CurrentShardConfig)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if ctx, ok := kv.Shards[key2shard(args.Key)].CommitMap[args.ClientId]; ok {
		if ctx.RequestId == args.RequestId {
			reply.Err = ctx.Err
			reply.Value = ctx.Value
			kv.mu.Unlock()
			return
		}
	}

	context := kv.NewContext(op)
	kv.contextMap[args.ClientId] = &context
	kv.mu.Unlock()

	// start command in raft
	_, _, res := kv.rf.Start(Op{op.Operation, op.Data})
	if !res {
		// current server is not leader
		reply.Err = ErrWrongLeader
		return
	}

	if ok := kv.CheckCommit(&context); ok {
		kv.mu.Lock()
		kv.DPrintf("context %v has been committed", context)
		result := context.Result.(GetResult)
		reply.Err = result.Err
		reply.Value = result.value
	} else {
		kv.mu.Lock()
		kv.DPrintf("context %v commit timeout", context)
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
	op.Data = PutAppendData{args.RequestId, args.ClientId, args.Key, args.Value}

	if _, state := kv.rf.GetState(); !state {
		kv.DPrintf("not the leader for op %v", op)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	// pre-check, reduce raft log
	if !kv.CanServe(args.Key) {
		kv.DPrintf("cannot serve PUTAPPEND request %v and current config is %v", args, kv.CurrentShardConfig)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if ctx, ok := kv.Shards[key2shard(args.Key)].CommitMap[args.ClientId]; ok {
		if ctx.RequestId == args.RequestId {
			reply.Err = ctx.Err
			kv.mu.Unlock()
			return
		}
	}

	context := kv.NewContext(op)
	kv.contextMap[args.ClientId] = &context
	kv.mu.Unlock()

	// start command in raft
	_, _, res := kv.rf.Start(Op{op.Operation, op.Data})
	if !res {
		kv.DPrintf("not the leader for op %v", op)
		// current server is not leader
		reply.Err = ErrWrongLeader
		return
	}

	if ok := kv.CheckCommit(&context); ok {
		kv.mu.Lock()
		kv.DPrintf("context %v has been committed", context)
		result := context.Result.(PutAppendResult)
		reply.Err = result.Err
	} else {
		kv.mu.Lock()
		kv.DPrintf("context %v commit timeout", context)
		reply.Err = ErrWrongLeader
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	// Your code here.
	// make an op
	op := Op{}
	op.Operation = GET_SHARD
	op.Data = GetShardData{args.RequestId, args.ClientId, args.Shards, args.ConfigNum}

	if _, state := kv.rf.GetState(); !state {
		kv.DPrintf("not the leader for op %v", op)
		reply.Err = ErrWrongLeader
		return
	}

	// add context into map
	kv.mu.Lock()

	// check if ready to be pulled
	if args.ConfigNum != kv.CurrentShardConfig.Num {
		kv.DPrintf("Not ready to get shard for args %v", args)
		reply.Err = ErrNotReady
		kv.mu.Unlock()
		return
	}
	for _, shard := range args.Shards {
		if kv.Shards[shard].Status != BEING_PULLED {
			kv.DPrintf("Status of shard %v not being BEING PULLED", shard)
			reply.Err = ErrOutDated
			kv.mu.Unlock()
			return
		}
	}

	context := kv.NewContext(op)
	kv.contextMap[args.ClientId] = &context
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
		kv.DPrintf("context %v has been committed", context)
		result := context.Result.(GetShardResult)
		reply.Err = result.Err
		reply.ShardIndex = result.ShardIndex
		reply.Shards = result.Shards
	} else {
		kv.mu.Lock()
		kv.DPrintf("context %v commit timeout", context)
		reply.Err = ErrWrongLeader
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	// Your code here.
	// make an op
	op := Op{}
	op.Operation = DELETE_SHARD
	op.Data = DeleteShardData{args.RequestId, args.ClientId, args.Shards, args.ConfigNum}

	if _, state := kv.rf.GetState(); !state {
		kv.DPrintf("not the leader for op %v", op)
		reply.Err = ErrWrongLeader
		return
	}

	// add context into map
	kv.mu.Lock()

	// check if same config
	if args.ConfigNum > kv.CurrentShardConfig.Num {
		reply.Err = ErrNotReady
		kv.DPrintf("Try to delete shard %v but current config is %v", args, kv.CurrentShardConfig)
		kv.mu.Unlock()
		return
	} else if args.ConfigNum < kv.CurrentShardConfig.Num {
		// 旧的delete shard request可能已经成功，但是另一边并没有收到
		// 这样当前group可能所有shard已经变成serving并且更新到下一个config
		reply.Err = OK
		kv.DPrintf("Try to delete shard %v but current config is %v", args, kv.CurrentShardConfig)
		kv.mu.Unlock()
		return
	}

	// if current shard is in SERVING, it should have been GC'ed
	for _, shard := range args.Shards {
		if kv.Shards[shard].Status == SERVING {
			kv.DPrintf("Status of shard %v is SERVING, skip delete", shard)
			reply.Err = OK
			kv.mu.Unlock()
			return
		}

		if kv.Shards[shard].Status != BEING_PULLED {
			// other states, something could go wrong
			kv.DPrintf("Invalid state %v when try to delete shard", kv.Shards[shard].Status)
			reply.Err = ErrOutDated
			kv.mu.Unlock()
			return
		}
	}

	context := kv.NewContext(op)
	kv.contextMap[args.ClientId] = &context
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
		kv.DPrintf("context %v has been committed", context)
		reply.Err = OK
	} else {
		kv.mu.Lock()
		kv.DPrintf("context %v commit timeout", context)
		reply.Err = ErrWrongLeader
	}

	kv.mu.Unlock()
}

// Apply operation for commands
func (kv *ShardKV) ApplyGetOperation(data GetData) {
	commitContext := CommitContext{}
	commitContext.RequestId = data.RequestId

	// get the shard
	shard := &kv.Shards[key2shard(data.Key)]

	// check if kv can serve
	if !kv.CanServe(data.Key) {
		kv.DPrintf("cannot serve Get op %v, skip", data)
		commitContext.Err = ErrWrongGroup
		goto notify
	}

	// check if dup request
	if ctx, ok := shard.CommitMap[data.ClientId]; ok && ctx.RequestId == data.RequestId {
		kv.DPrintf("data %v is in committed state, skip", data)
		commitContext.Err = ctx.Err
		commitContext.Value = ctx.Value

		// if the op has been committed, we should try to notify client directly instead of applying the same
		// change again
		goto notify
	}

	if val, ok := shard.KvState[data.Key]; ok {
		commitContext.Value = val
		commitContext.Err = OK
	} else {
		commitContext.Err = ErrNoKey
	}

	// add to commit map
	shard.CommitMap[data.ClientId] = commitContext

notify:
	// put result into context map and notify waiting thread
	if context, ok := kv.contextMap[data.ClientId]; ok {
		if context.op.Operation != GET || context.op.Data.(GetData).RequestId != data.RequestId {
			kv.DPrintf("op %v in context map not matched with op %v", context.op.Data, data)
		} else {
			if term, isLeader := kv.rf.GetState(); term == context.term && isLeader {
				// Refer to https://github.com/OneSizeFitsQuorum/MIT6.824-2021/blob/master/docs/lab3.md
				// 1. 仅对 leader 的 notifyChan 进行通知：目前的实现中读写请求都需要路由给 leader 去处理，所以在执行日志到状态机后，只有 leader 需将执行结果通过 notifyChan 唤醒阻塞的客户端协程，而 follower 则不需要；对于 leader 降级为 follower 的情况，该节点在 apply 日志后也不能对之前靠 index 标识的 channel 进行 notify，因为可能执行结果是不对应的，所以在加上只有 leader 可以 notify 的判断后，对于此刻还阻塞在该节点的客户端协程，就可以让其自动超时重试。如果读者足够细心，也会发现这里的机制依然可能会有问题，下一点会提到。
				// 2. 仅对当前 term 日志的 notifyC&& context.op.RequestId == op.RequestId han 进行通知：上一点提到，对于 leader 降级为 follower 的情况，该节点需要让阻塞的请求超时重试以避免违反线性一致性。那么有没有这种可能呢？leader 降级为 follower 后又迅速重新当选了 leader，而此时依然有客户端协程未超时在阻塞等待，那么此时 apply 日志后，根据 index 获得 channel 并向其中 push 执行结果就可能出错，因为可能并不对应。对于这种情况，最直观地解决方案就是仅对当前 term 日志的 notifyChan 进行通知，让之前 term 的客户端协程都超时重试即可。
				result := GetResult{}
				result.Err = commitContext.Err
				result.value = commitContext.Value
				context.Result = result
				delete(kv.contextMap, data.ClientId)
				close(context.commitChan)
			} else {
				kv.DPrintf("current node is not leader or term not matched, cannot respond to context")
			}
		}
	} else {
		kv.DPrintf("Get %v not found in context map %v", data, kv.contextMap)
	}
}

func (kv *ShardKV) ApplyPutAppendOperation(data PutAppendData, append bool) {
	commitContext := CommitContext{}
	commitContext.RequestId = data.RequestId

	// get the shard
	shard := &kv.Shards[key2shard(data.Key)]

	// check if kv can serve
	if !kv.CanServe(data.Key) {
		kv.DPrintf("cannot serve PutAppend op %v, skip", data)
		commitContext.Err = ErrWrongGroup
		goto notify
	}

	// check if dup request
	if ctx, ok := shard.CommitMap[data.ClientId]; ok && ctx.RequestId == data.RequestId {
		kv.DPrintf("data %v is in committed state, skip", data)
		commitContext.Err = ctx.Err
		commitContext.Value = ctx.Value

		// if the op has been committed, we should try to notify client directly instead of applying the same
		// change again
		goto notify
	}

	if append {
		if val, ok := shard.KvState[data.Key]; ok {
			shard.KvState[data.Key] = val + data.Value
		} else {
			shard.KvState[data.Key] = data.Value
		}
		commitContext.Err = OK
	} else {
		shard.KvState[data.Key] = data.Value
		commitContext.Err = OK
	}

	// add to commit map
	shard.CommitMap[data.ClientId] = commitContext

notify:
	// put result into context map and notify waiting thread
	if context, ok := kv.contextMap[data.ClientId]; ok {
		if (context.op.Operation != PUT && context.op.Operation != APPEND) || context.op.Data.(PutAppendData).RequestId != data.RequestId {
			kv.DPrintf("op %v in context map not matched with op %v", context.op.Data, data)
		} else {
			if term, isLeader := kv.rf.GetState(); term == context.term && isLeader {
				// Refer to https://github.com/OneSizeFitsQuorum/MIT6.824-2021/blob/master/docs/lab3.md
				// 1. 仅对 leader 的 notifyChan 进行通知：目前的实现中读写请求都需要路由给 leader 去处理，所以在执行日志到状态机后，只有 leader 需将执行结果通过 notifyChan 唤醒阻塞的客户端协程，而 follower 则不需要；对于 leader 降级为 follower 的情况，该节点在 apply 日志后也不能对之前靠 index 标识的 channel 进行 notify，因为可能执行结果是不对应的，所以在加上只有 leader 可以 notify 的判断后，对于此刻还阻塞在该节点的客户端协程，就可以让其自动超时重试。如果读者足够细心，也会发现这里的机制依然可能会有问题，下一点会提到。
				// 2. 仅对当前 term 日志的 notifyC&& context.op.RequestId == op.RequestId han 进行通知：上一点提到，对于 leader 降级为 follower 的情况，该节点需要让阻塞的请求超时重试以避免违反线性一致性。那么有没有这种可能呢？leader 降级为 follower 后又迅速重新当选了 leader，而此时依然有客户端协程未超时在阻塞等待，那么此时 apply 日志后，根据 index 获得 channel 并向其中 push 执行结果就可能出错，因为可能并不对应。对于这种情况，最直观地解决方案就是仅对当前 term 日志的 notifyChan 进行通知，让之前 term 的客户端协程都超时重试即可。
				result := PutAppendResult{}
				result.Err = commitContext.Err
				context.Result = result
				delete(kv.contextMap, data.ClientId)
				close(context.commitChan)
			} else {
				kv.DPrintf("current node is not leader or term not matched, cannot respond to context")
			}
		}
	} else {
		kv.DPrintf("PutAppend %v not found in context map %v", data, kv.contextMap)
	}
}

func (kv *ShardKV) ApplySnapshot(term int, index int, snapshot []byte) {
	if kv.rf.CondInstallSnapshot(term, index, snapshot) {
		if index > kv.CommitIndex {
			kv.DPrintf("cond install snapshot with term %v and index %v", term, index)
			kv.ReadSnapshot()
			kv.CommitIndex = index
		}
	}
}

func (kv *ShardKV) ApplyConfigUpdate(data UpdateConfigData) {
	// check if config is newer
	if data.NewConfig.Num <= kv.CurrentShardConfig.Num {
		kv.DPrintf("current config num %v is newer than command %v", kv.CurrentShardConfig.Num, data.NewConfig.Num)
		return
	}

	if data.NewConfig.Num > kv.CurrentShardConfig.Num+1 {
		kv.DPrintf("config is too new to update", kv.CurrentShardConfig.Num, data.NewConfig.Num)
	}

	// check if all shards are serving
	for i, shard := range kv.Shards {
		if shard.Status != SERVING {
			kv.DPrintf("Shard %v is in %v status, skip", i, shard.Status)
			return
		}
	}

	kv.LastShardConfig = kv.CurrentShardConfig
	kv.CurrentShardConfig = data.NewConfig

	for i, _ := range kv.Shards {
		// if need to pull data, change status
		if kv.CurrentShardConfig.Shards[i] == kv.gid && (kv.LastShardConfig.Shards[i] != 0 && kv.LastShardConfig.Shards[i] != kv.gid) {
			kv.Shards[i].Status = PULLING
		}

		// if need to be pulled, change status
		if kv.CurrentShardConfig.Shards[i] != kv.gid && kv.LastShardConfig.Shards[i] == kv.gid {
			kv.Shards[i].Status = BEING_PULLED
		}
	}

	kv.DPrintf("Update shard config to %v", data.NewConfig)
}

func (kv *ShardKV) ApplyDeleteShard(data DeleteShardData) {
	if kv.CurrentShardConfig.Num != data.ConfigNum {
		kv.DPrintf("Try to delete shard %v but config num not match {%v, %v}", kv.CurrentShardConfig.Num, data.ConfigNum)
	}

	for _, shard := range data.Shards {
		if kv.Shards[shard].Status != BEING_PULLED {
			kv.DPrintf("Try to delete shard %v but current shard status %v", shard, kv.Shards[shard].Status)
			return
		}

	}

	for _, shard := range data.Shards {
		kv.Shards[shard].KvState = nil
		kv.Shards[shard].CommitMap = nil
		kv.Shards[shard].Status = SERVING
	}
	kv.DPrintf("deleted shards %v", data.Shards)

	// put result into context map and notify waiting thread
	if context, ok := kv.contextMap[data.ClientId]; ok {
		if context.op.Operation != DELETE_SHARD || context.op.Data.(DeleteShardData).RequestId != data.RequestId {
			kv.DPrintf("op %v in context map not matched with op %v", context.op.Data, data)
		} else {
			if term, isLeader := kv.rf.GetState(); term == context.term && isLeader {
				// Refer to https://github.com/OneSizeFitsQuorum/MIT6.824-2021/blob/master/docs/lab3.md
				// 1. 仅对 leader 的 notifyChan 进行通知：目前的实现中读写请求都需要路由给 leader 去处理，所以在执行日志到状态机后，只有 leader 需将执行结果通过 notifyChan 唤醒阻塞的客户端协程，而 follower 则不需要；对于 leader 降级为 follower 的情况，该节点在 apply 日志后也不能对之前靠 index 标识的 channel 进行 notify，因为可能执行结果是不对应的，所以在加上只有 leader 可以 notify 的判断后，对于此刻还阻塞在该节点的客户端协程，就可以让其自动超时重试。如果读者足够细心，也会发现这里的机制依然可能会有问题，下一点会提到。
				// 2. 仅对当前 term 日志的 notifyChan 进行通知：上一点提到，对于 leader 降级为 follower 的情况，该节点需要让阻塞的请求超时重试以避免违反线性一致性。那么有没有这种可能呢？leader 降级为 follower 后又迅速重新当选了 leader，而此时依然有客户端协程未超时在阻塞等待，那么此时 apply 日志后，根据 index 获得 channel 并向其中 push 执行结果就可能出错，因为可能并不对应。对于这种情况，最直观地解决方案就是仅对当前 term 日志的 notifyChan 进行通知，让之前 term 的客户端协程都超时重试即可。当然，目前客户端超时重试的时间是 500ms，选举超时的时间是 1s，所以严格来说并不会出现这种情况，但是为了代码的鲁棒性，最好还是这么做，否则以后万一有人将客户端超时时间改到 5s 就可能出现这种问题了。
				delete(kv.contextMap, data.ClientId)
				close(context.commitChan)
			} else {
				kv.DPrintf("current node is not leader or term not matched, cannot respond to shard context")
			}
		}

	} else {
		kv.DPrintf("DeleteShard %v not found in context map %v", data, kv.contextMap)
	}
}

func (kv *ShardKV) FinishGc(data FinishGcData) {
	if kv.CurrentShardConfig.Num != data.ConfigNum {
		kv.DPrintf("Try to finish GC on shards %v but config num not match {%v, %v}", data.Shards, kv.CurrentShardConfig.Num, data.ConfigNum)
	}
	for _, shard := range data.Shards {
		if kv.Shards[shard].Status != GCING {
			kv.DPrintf("Try to finish GC on shard %v but current shard status is %v", shard, kv.Shards[shard].Status)
			return
		}
	}

	for _, shard := range data.Shards {
		kv.Shards[shard].Status = SERVING
	}
	kv.DPrintf("GC'ed shards %v", data.Shards)
}

func (kv *ShardKV) ApplyInsertShard(data InsertShardData) {
	if kv.CurrentShardConfig.Num != data.ConfigNum {
		kv.DPrintf("Try to insert shard %v but config num not match {%v, %v}", kv.CurrentShardConfig.Num, data.ConfigNum)
	}
	for _, shard := range data.ShardIndex {
		if kv.Shards[shard].Status != PULLING {
			kv.DPrintf("Try to insert shard %v but current shard status is not PULLING", data)
			return
		}
	}

	for i, shard := range data.ShardIndex {
		kv.Shards[shard].KvState = kv.CopyKvState(&data.Shards[i].KvState)
		kv.Shards[shard].CommitMap = kv.CopyCommitMap(&data.Shards[i].CommitMap)
		kv.Shards[shard].Status = GCING
	}

	kv.DPrintf("inserted shards %v", data.ShardIndex)
}

func (kv *ShardKV) ApplyGetShard(data GetShardData) {
	result := GetShardResult{}
	if data.ConfigNum != kv.CurrentShardConfig.Num {
		kv.DPrintf("Not ready for get shard request %v", data)
		result.Err = ErrNotReady
		return
	}
	for _, shard := range data.Shards {
		if data.ConfigNum != kv.CurrentShardConfig.Num || kv.Shards[shard].Status != BEING_PULLED {
			kv.DPrintf("Not ready for get shard request %v", data)
			result.Err = ErrNotReady
		}
	}

	for _, shardIndex := range data.Shards {
		result.ShardIndex = append(result.ShardIndex, shardIndex)
		shard := Shard{kv.CopyKvState(&kv.Shards[shardIndex].KvState),
			SERVING,
			kv.CopyCommitMap(&kv.Shards[shardIndex].CommitMap)}
		result.Shards = append(result.Shards, shard)
	}
	result.Err = OK

	// put result into context map and notify waiting thread
	if context, ok := kv.contextMap[data.ClientId]; ok {
		if context.op.Operation != GET_SHARD || context.op.Data.(GetShardData).RequestId != data.RequestId {
			kv.DPrintf("op %v in context map not matched with op %v", context.op.Data, data)
		} else {
			if term, isLeader := kv.rf.GetState(); term == context.term && isLeader {
				// Refer to https://github.com/OneSizeFitsQuorum/MIT6.824-2021/blob/master/docs/lab3.md
				// 1. 仅对 leader 的 notifyChan 进行通知：目前的实现中读写请求都需要路由给 leader 去处理，所以在执行日志到状态机后，只有 leader 需将执行结果通过 notifyChan 唤醒阻塞的客户端协程，而 follower 则不需要；对于 leader 降级为 follower 的情况，该节点在 apply 日志后也不能对之前靠 index 标识的 channel 进行 notify，因为可能执行结果是不对应的，所以在加上只有 leader 可以 notify 的判断后，对于此刻还阻塞在该节点的客户端协程，就可以让其自动超时重试。如果读者足够细心，也会发现这里的机制依然可能会有问题，下一点会提到。
				// 2. 仅对当前 term 日志的 notifyChan 进行通知：上一点提到，对于 leader 降级为 follower 的情况，该节点需要让阻塞的请求超时重试以避免违反线性一致性。那么有没有这种可能呢？leader 降级为 follower 后又迅速重新当选了 leader，而此时依然有客户端协程未超时在阻塞等待，那么此时 apply 日志后，根据 index 获得 channel 并向其中 push 执行结果就可能出错，因为可能并不对应。对于这种情况，最直观地解决方案就是仅对当前 term 日志的 notifyChan 进行通知，让之前 term 的客户端协程都超时重试即可。当然，目前客户端超时重试的时间是 500ms，选举超时的时间是 1s，所以严格来说并不会出现这种情况，但是为了代码的鲁棒性，最好还是这么做，否则以后万一有人将客户端超时时间改到 5s 就可能出现这种问题了。
				context.Result = result
				delete(kv.contextMap, data.ClientId)
				close(context.commitChan)
			} else {
				kv.DPrintf("current node is not leader or term not matched, cannot respond to shard context")
			}
		}

	} else {
		kv.DPrintf("GetShard %v not found in context map %v", data, kv.contextMap)
	}
}

// Running job
func (kv *ShardKV) UpdateShardConfig() {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			currentNum := kv.CurrentShardConfig.Num
			kv.mu.Unlock()
			// only leader should update shard config
			newConfig := kv.mck.Query(currentNum + 1)

			kv.mu.Lock()
			// though we'll check when apply operation, better check first here to avoid extra log in raft
			// check if config is newer
			if newConfig.Num <= kv.CurrentShardConfig.Num {
				kv.mu.Unlock()
				goto sleep
			}

			// check if all shards are serving
			for _, shard := range kv.Shards {
				if shard.Status != SERVING {
					kv.mu.Unlock()
					goto sleep
				}
			}

			op := Op{}
			op.Data = UpdateConfigData{newConfig}
			op.Operation = UDPATE_CONFIG
			_, _, res := kv.rf.Start(op) // only leader will start the op
			if res {
				kv.DPrintf("start the command to update shard config %v", op)
			}

			kv.mu.Unlock()
		}

	sleep:
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) PullShardData() {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			// try to pull shard data
			var wg sync.WaitGroup
			kv.mu.Lock()
			toPull := make(map[int][]int)
			for i, shard := range kv.Shards {
				if shard.Status == PULLING {
					toPull[kv.LastShardConfig.Shards[i]] = append(toPull[kv.LastShardConfig.Shards[i]], i)
				}
			}
			kv.mu.Unlock()

			for gid, shards := range toPull {
				wg.Add(1)
				go func(kv *ShardKV, gid int, shards []int) {
					defer wg.Done()

					// need to pull from remote
					kv.mu.Lock()
					args := GetShardArgs{}
					args.RequestId = atomic.AddInt64(&kv.SeqNum, 1)
					args.ClientId = kv.ServerId
					args.Shards = shards
					args.ConfigNum = kv.CurrentShardConfig.Num

					for _, server := range kv.LastShardConfig.Groups[gid] {
						srv := kv.make_end(server)

						kv.mu.Unlock()
						var reply GetShardReply
						ok := srv.Call("ShardKV.GetShard", &args, &reply)
						kv.mu.Lock()
						if ok && reply.Err == OK {
							op := Op{}
							op.Operation = INSERT_SHARD
							op.Data = InsertShardData{kv.CurrentShardConfig.Num, reply.ShardIndex, reply.Shards}
							_, _, res := kv.rf.Start(op) // only leader will start the op
							if res {
								kv.DPrintf("Start the command to insert shard %v", op)
								break
							}
						}
					}

					kv.mu.Unlock()
				}(kv, gid, shards)
			}
			wg.Wait()
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func (kv *ShardKV) GcShardData() {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			var wg sync.WaitGroup
			kv.mu.Lock()
			toPush := make(map[int][]int)
			for i, shard := range kv.Shards {
				if shard.Status == GCING {
					toPush[kv.LastShardConfig.Shards[i]] = append(toPush[kv.LastShardConfig.Shards[i]], i)
				}
			}
			kv.mu.Unlock()

			// try to GC shard data
			for gid, shards := range toPush {
				wg.Add(1)
				go func(kv *ShardKV, gid int, shards []int) {
					defer wg.Done()

					// need to notify remote
					kv.mu.Lock()
					args := DeleteShardArgs{}
					args.RequestId = atomic.AddInt64(&kv.SeqNum, 1)
					args.ClientId = kv.ServerId
					args.Shards = shards
					args.ConfigNum = kv.CurrentShardConfig.Num

					for _, server := range kv.LastShardConfig.Groups[gid] {
						srv := kv.make_end(server)

						kv.mu.Unlock()
						var reply DeleteShardReply
						ok := srv.Call("ShardKV.DeleteShard", &args, &reply)
						kv.mu.Lock()
						if ok && reply.Err == OK {
							op := Op{}
							op.Operation = FINISH_GC
							op.Data = FinishGcData{shards, kv.CurrentShardConfig.Num}
							_, _, res := kv.rf.Start(op) // only leader will start the op
							if res {
								kv.DPrintf("Start the command to finish gc for shards %v", shards)
								break
							}
						}
					}

					kv.mu.Unlock()
				}(kv, gid, shards)
			}
			wg.Wait()
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func (kv *ShardKV) RunStateMachine() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}

		kv.mu.Lock()

		if m.SnapshotValid {
			kv.ApplySnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot)
		} else if m.CommandValid {
			if m.CommandIndex <= kv.CommitIndex {
				kv.mu.Unlock()
				continue
			} else {
				kv.CommitIndex = m.CommandIndex
			}
			op := m.Command.(Op)
			switch op.Operation {
			case GET:
				data := op.Data.(GetData)
				kv.ApplyGetOperation(data)
			case PUT:
				data := op.Data.(PutAppendData)
				kv.ApplyPutAppendOperation(data, false)
			case APPEND:
				data := op.Data.(PutAppendData)
				kv.ApplyPutAppendOperation(data, true)
			case GET_SHARD:
				data := op.Data.(GetShardData)
				kv.ApplyGetShard(data)
			case INSERT_SHARD:
				data := op.Data.(InsertShardData)
				kv.ApplyInsertShard(data)
			case UDPATE_CONFIG:
				data := op.Data.(UpdateConfigData)
				kv.ApplyConfigUpdate(data)
			case DELETE_SHARD:
				data := op.Data.(DeleteShardData)
				kv.ApplyDeleteShard(data)
			case FINISH_GC:
				data := op.Data.(FinishGcData)
				kv.FinishGc(data)
			case EMPTY_ENTRY:
				// do nothing
			default:
				panic("Unknown operation found")
			}
		} else {
			panic("Either not command valid or snapshot valid")
		}

		kv.CheckSnapshot()
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) CheckEntryInCurrentTermAction() {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			if !kv.rf.HasLogInCurrentTerm() {
				op := Op{}
				op.Operation = EMPTY_ENTRY
				kv.rf.Start(op)
			}
		}
		time.Sleep(10 * time.Millisecond)
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
	labgob.Register(GetData{})
	labgob.Register(PutAppendData{})
	labgob.Register(GetShardData{})
	labgob.Register(InsertShardData{})
	labgob.Register(UpdateConfigData{})
	labgob.Register(DeleteShardData{})
	labgob.Register(FinishGcData{})

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
	kv.contextMap = make(map[int64]*RequestContext)
	kv.CommitIndex = -1
	kv.ServerId = nrand()
	kv.SeqNum = 0
	kv.CurrentShardConfig = shardctrler.Config{}
	kv.LastShardConfig = shardctrler.Config{}
	kv.Shards = make([]Shard, 10)
	for i, _ := range kv.Shards {
		kv.Shards[i].KvState = make(map[string]string)
		kv.Shards[i].CommitMap = make(map[int64]CommitContext)
		kv.Shards[i].Status = SERVING
	}

	kv.ReadSnapshot()

	// You may need initialization code here.
	go kv.RunStateMachine()
	go kv.PullShardData()
	go kv.UpdateShardConfig()
	go kv.GcShardData()
	go kv.CheckEntryInCurrentTermAction()

	return kv
}

// Utils
func (kv *ShardKV) NewContext(op Op) RequestContext {
	context := RequestContext{}
	context.commitChan = make(chan byte)
	context.op = op
	context.term, _ = kv.rf.GetState()
	return context
}

func (kv *ShardKV) CanServe(key string) bool {
	idx := key2shard(key)
	return kv.CurrentShardConfig.Shards[idx] == kv.gid && (kv.Shards[idx].Status == SERVING || kv.Shards[idx].Status == GCING)
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

func (kv *ShardKV) CopyKvState(src *map[string]string) map[string]string {
	newMap := make(map[string]string)
	for k, v := range *src {
		newMap[k] = v
	}
	return newMap
}

func (kv *ShardKV) CopyCommitMap(src *map[int64]CommitContext) map[int64]CommitContext {
	newMap := make(map[int64]CommitContext)
	for k, v := range *src {
		commitContext := CommitContext{}
		commitContext.RequestId = v.RequestId
		commitContext.Err = v.Err
		commitContext.Value = v.Value
		newMap[k] = v
	}
	return newMap
}

func (kv *ShardKV) CheckCommit(context *RequestContext) bool {
	select {
	case _ = <-context.commitChan:
		// committed
		return true
	case <-time.After(500 * time.Millisecond):
		// timeout
		return false
	}
}
