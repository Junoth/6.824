package shardkv

import (
	"6.824/shardctrler"
	"fmt"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotReady    = "ErrNotReady"
	ErrOutDated    = "ErrOutDated"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int64
	ClientId  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int64
	ClientId  int64
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	RequestId int64
	ClientId  int64
	Shards    []int
	ConfigNum int
}

type GetShardReply struct {
	Err        Err
	ShardIndex []int
	Shards     []Shard
}

type DeleteShardArgs struct {
	ClientId  int64
	RequestId int64
	Shards    []int
	ConfigNum int
}

type DeleteShardReply struct {
	Err Err
}

type Operation string

const (
	GET           = "GET"
	PUT           = "PUT"
	APPEND        = "APPEND"
	UDPATE_CONFIG = "UDPATE_CONFIG"
	GET_SHARD     = "GET_SHARD"
	INSERT_SHARD  = "INSERT_SHARD"
	DELETE_SHARD  = "DELETE_SHARD"
	FINISH_GC     = "FINISH_GC"
	EMPTY_ENTRY   = "EMPTY_ENTRY"
)

type Status string

const (
	SERVING      = "SERVING"
	PULLING      = "PULLING"
	BEING_PULLED = "BEING_PULLED"
	GCING        = "GCING"
)

type Shard struct {
	KvState   map[string]string
	Status    Status
	CommitMap map[int64]CommitContext
}

type Op struct {
	Operation Operation
	Data      interface{}
}

func (op Op) String() string {
	return fmt.Sprintf("{Type:%v,Data:%v}", op.Operation, op.Data)
}

type PutAppendData struct {
	RequestId int64
	ClientId  int64
	Key       string
	Value     string
}

type GetData struct {
	RequestId int64
	ClientId  int64
	Key       string
}

type GetShardData struct {
	RequestId int64
	ClientId  int64
	Shards    []int
	ConfigNum int
}

type UpdateConfigData struct {
	NewConfig shardctrler.Config
}

type InsertShardData struct {
	ConfigNum  int
	ShardIndex []int
	Shards     []Shard
}

type DeleteShardData struct {
	RequestId int64
	ClientId  int64
	Shards    []int
	ConfigNum int
}

type FinishGcData struct {
	Shards    []int
	ConfigNum int
}
