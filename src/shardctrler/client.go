package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader   int
	clientId int64
	seqId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leader = -1
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.RequestId = ck.seqId

	// update seq id
	ck.seqId++

	if ck.leader != -1 {
		var reply QueryReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = i
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.RequestId = ck.seqId

	// update seq id
	ck.seqId++

	if ck.leader != -1 {
		var reply JoinReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.RequestId = ck.seqId

	// update seq id
	ck.seqId++

	if ck.leader != -1 {
		var reply LeaveReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.RequestId = ck.seqId

	// update seq id
	ck.seqId++

	if ck.leader != -1 {
		var reply MoveReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
