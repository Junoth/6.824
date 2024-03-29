package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64
	seqId       int64 // should be monotonically
	groupLeader map[int]int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.groupLeader = make(map[int]int)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.RequestId = ck.seqId
	ck.seqId++

	for {
		currentConfig := ck.config
		shard := key2shard(key)
		gid := currentConfig.Shards[shard]
		if servers, ok := currentConfig.Groups[gid]; ok {
			// try leader if exist
			if leader, ok := ck.groupLeader[gid]; ok {
				srv := ck.make_end(servers[leader])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok {
					if reply.Err == OK || reply.Err == ErrNoKey {
						return reply.Value
					} else if reply.Err == ErrWrongGroup {
						goto sleep
					}
				}
			}
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok {
					if reply.Err == OK || reply.Err == ErrNoKey {
						ck.groupLeader[gid] = si
						return reply.Value
					} else if reply.Err == ErrWrongGroup {
						goto sleep
					}
				}
			}
		}
	sleep:
		ck.config = ck.sm.Query(-1)
		time.Sleep(100 * time.Millisecond)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.RequestId = ck.seqId
	ck.seqId++

	for {
		currentConfig := ck.config
		shard := key2shard(key)
		gid := currentConfig.Shards[shard]
		if servers, ok := currentConfig.Groups[gid]; ok {
			// try leader if exist
			if leader, ok := ck.groupLeader[gid]; ok {
				srv := ck.make_end(servers[leader])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok {
					if reply.Err == OK {
						return
					} else if reply.Err == ErrWrongGroup {
						goto sleep
					}
				}
			}
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok {
					if reply.Err == OK {
						ck.groupLeader[gid] = si
						return
					} else if reply.Err == ErrWrongGroup {
						ck.groupLeader[gid] = si
						goto sleep
					}
				}
			}
		}
	sleep:
		ck.config = ck.sm.Query(-1)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
