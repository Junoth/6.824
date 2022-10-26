package shardctrler

import (
	"bytes"
	"sort"
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
	GIDs      []int
	Servers   map[int][]string
	GID       int
	Shard     int
	Num       int
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
	op.Servers = args.Servers

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
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	if ok := sc.CheckCommit(&context); ok {
		sc.mu.Lock()
		DPrintf("Server %v: op %v has been committed", sc.me, op)
	} else {
		sc.mu.Lock()
		DPrintf("Server %v: op %v commit fail", sc.me, op)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}

	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{}
	op.Operation = LEAVE
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.GIDs = args.GIDs

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
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	if ok := sc.CheckCommit(&context); ok {
		sc.mu.Lock()
		DPrintf("Server %v: op %v has been committed", sc.me, op)
	} else {
		sc.mu.Lock()
		DPrintf("Server %v: op %v commit fail", sc.me, op)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}

	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{}
	op.Operation = MOVE
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.GID = args.GID
	op.Shard = args.Shard

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
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	if ok := sc.CheckCommit(&context); ok {
		sc.mu.Lock()
		DPrintf("Server %v: op %v has been committed", sc.me, op)
	} else {
		sc.mu.Lock()
		DPrintf("Server %v: op %v commit fail", sc.me, op)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}

	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{}
	op.Operation = QUERY
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId
	op.Num = args.Num

	DPrintf("server %v received query request %v", sc.me, args)

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
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	if ok := sc.CheckCommit(&context); ok {
		sc.mu.Lock()
		reply.Config = context.Value
		DPrintf("Server %v: op %v has been committed", sc.me, op)
	} else {
		sc.mu.Lock()
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		DPrintf("Server %v: op %v commit fail", sc.me, op)
	}

	sc.mu.Unlock()
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

// JoinGroups join new groups and rebalance, this will produce a new group
// should hold the lock when call this function
func (sc *ShardCtrler) JoinGroups(JoinGroups map[int][]string) {
	latest := sc.Configs[len(sc.Configs)-1]
	groupShardMap := make(map[int][]int)

	newGroups := make(map[int][]string)
	newConfig := Config{len(sc.Configs), latest.Shards, newGroups}

	for k, v := range JoinGroups {
		newGroups[k] = v
		groupShardMap[k] = []int{}
	}

	for k, v := range latest.Groups {
		newGroups[k] = v
		groupShardMap[k] = []int{}
	}

	for i, v := range newConfig.Shards {
		if v == 0 {
			continue
		}
		// valid shard assigned to a group
		groupShardMap[v] = append(groupShardMap[v], i)
	}

	sortedGroups := make([]int, 0)
	for k, _ := range newGroups {
		sortedGroups = append(sortedGroups, k)
	}

	// no existing group, assign all shards to groups
	for i, v := range newConfig.Shards {
		if v == 0 {
			sort.Slice(sortedGroups, func(i, j int) bool {
				if len(groupShardMap[sortedGroups[i]]) == len(groupShardMap[sortedGroups[j]]) {
					return sortedGroups[i] < sortedGroups[j]
				}

				return len(groupShardMap[sortedGroups[i]]) < len(groupShardMap[sortedGroups[j]])
			})

			minGroup := sortedGroups[0]
			groupShardMap[minGroup] = append(groupShardMap[minGroup], i)
			newConfig.Shards[i] = minGroup
		}
	}

	// balance
	unbalanced := true
	for unbalanced {
		// sort the group according to the num of shards and GID
		// this is to make assignment deterministic
		sort.Slice(sortedGroups, func(i, j int) bool {
			if len(groupShardMap[sortedGroups[i]]) == len(groupShardMap[sortedGroups[j]]) {
				return sortedGroups[i] < sortedGroups[j]
			}

			return len(groupShardMap[sortedGroups[i]]) < len(groupShardMap[sortedGroups[j]])
		})

		minGroup := sortedGroups[0]
		maxGroup := sortedGroups[len(sortedGroups)-1]

		// only if the diff is larger than 1, we'll move shard
		if len(groupShardMap[maxGroup])-len(groupShardMap[minGroup]) > 1 {
			// move min shard
			toMove := groupShardMap[maxGroup][0]
			for _, v := range groupShardMap[maxGroup] {
				if v < toMove {
					toMove = v
				}
			}
			if newConfig.Shards[toMove] != maxGroup {
				panic("The shard to move is not in the expected group")
			}
			newConfig.Shards[toMove] = minGroup
			groupShardMap[maxGroup] = groupShardMap[maxGroup][1:]
			groupShardMap[minGroup] = append(groupShardMap[minGroup], toMove)
		} else {
			unbalanced = false
		}
	}

	DPrintf("Server %v has new config: %v", sc.me, newConfig)
	sc.Configs = append(sc.Configs, newConfig)
}

// LeaveGroups leave groups and rebalance, this will produce a new group
// should hold the lock when call this function
func (sc *ShardCtrler) LeaveGroups(LeaveGroups []int) {
	latest := sc.Configs[len(sc.Configs)-1]
	groupShardMap := make(map[int][]int)

	newGroups := make(map[int][]string)
	newConfig := Config{len(sc.Configs), latest.Shards, newGroups}

	for k, v := range latest.Groups {
		toLeave := false
		for _, group := range LeaveGroups {
			if k == group {
				// leave group
				toLeave = true
				break
			}
		}
		if !toLeave {
			newGroups[k] = v
			groupShardMap[k] = []int{}
		}
	}

	for i, v := range newConfig.Shards {
		if v == 0 {
			continue
		}
		toLeave := false
		for _, group := range LeaveGroups {
			if v == group {
				// leave group
				newConfig.Shards[i] = 0
				toLeave = true
				break
			}
		}
		if !toLeave {
			groupShardMap[v] = append(groupShardMap[v], i)
		}
	}

	sortedGroups := make([]int, 0)
	for k, _ := range newGroups {
		sortedGroups = append(sortedGroups, k)
	}

	// assign all unassigned shards to groups
	if len(newGroups) > 0 {
		for i, v := range newConfig.Shards {
			if v == 0 {
				sort.Slice(sortedGroups, func(i, j int) bool {
					if len(groupShardMap[sortedGroups[i]]) == len(groupShardMap[sortedGroups[j]]) {
						return sortedGroups[i] < sortedGroups[j]
					}

					return len(groupShardMap[sortedGroups[i]]) < len(groupShardMap[sortedGroups[j]])
				})

				minGroup := sortedGroups[0]
				groupShardMap[minGroup] = append(groupShardMap[minGroup], i)
				newConfig.Shards[i] = minGroup
			}
		}

		// balance
		unbalanced := true
		for unbalanced {
			// sort the group according to the num of shards and GID
			// this is to make assignment deterministic
			sort.Slice(sortedGroups, func(i, j int) bool {
				if len(groupShardMap[sortedGroups[i]]) == len(groupShardMap[sortedGroups[j]]) {
					return sortedGroups[i] < sortedGroups[j]
				}

				return len(groupShardMap[sortedGroups[i]]) < len(groupShardMap[sortedGroups[j]])
			})

			minGroup := sortedGroups[0]
			maxGroup := sortedGroups[len(sortedGroups)-1]

			// only if the diff is larger than 1, we'll move shard
			if len(groupShardMap[maxGroup])-len(groupShardMap[minGroup]) > 1 {
				// move min shard
				toMove := groupShardMap[maxGroup][0]
				for _, v := range groupShardMap[maxGroup] {
					if v < toMove {
						toMove = v
					}
				}
				if newConfig.Shards[toMove] != maxGroup {
					panic("The shard to move is not in the expected group")
				}
				newConfig.Shards[toMove] = minGroup
				groupShardMap[maxGroup] = groupShardMap[maxGroup][1:]
				groupShardMap[minGroup] = append(groupShardMap[minGroup], toMove)
			} else {
				unbalanced = false
			}
		}
	}

	DPrintf("Server %v has new config: %v", sc.me, newConfig)
	sc.Configs = append(sc.Configs, newConfig)
}

// MoveShard move shard to given GID, this will produce a new group
// should hold the lock when call this function
// if GID not exist in current config, this will be a no-op
func (sc *ShardCtrler) MoveShard(Shard int, GID int) {
	latest := sc.Configs[len(sc.Configs)-1]
	if _, ok := latest.Groups[GID]; !ok {
		DPrintf("Group %v not exist in config", GID)
	} else {
		newGroups := make(map[int][]string)
		for k, v := range latest.Groups {
			newServers := make([]string, len(v))
			copy(newServers, v)
			newGroups[k] = newServers
		}
		newConfig := Config{len(sc.Configs), latest.Shards, newGroups}
		newConfig.Shards[Shard] = GID
		DPrintf("Server %v has new config: %v", sc.me, newConfig)
		sc.Configs = append(sc.Configs, newConfig)
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
				sc.JoinGroups(op.Servers)
			case LEAVE:
				sc.LeaveGroups(op.GIDs)
			case MOVE:
				sc.MoveShard(op.Shard, op.GID)
			case QUERY:
				num := op.Num
				if num == -1 || num >= len(sc.Configs) {
					value = sc.Configs[len(sc.Configs)-1]
				} else {
					value = sc.Configs[num]
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
	sc.Configs[0].Num = 0

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.CommitIndex = -1
	sc.CommitMap = make(map[int64]CommitContext)
	sc.RequestMap = make(map[int64]*RequestContext)

	sc.ReadSnapshot()

	// run state machine
	go sc.RunStateMachine()

	return sc
}
