package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	clientId int64
	seqId    int64 // should be monotonically
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
	// You'll have to add code here.
	ck.leader = -1
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// Get arg
	args := &GetArgs{}
	args.Key = key
	args.RequestId = ck.seqId
	args.ClientId = ck.clientId

	// update seq Id
	ck.seqId++

	// Get reply
	reply := &GetReply{}

	i := 0
	for {
		ok := false
		if ck.leader != -1 {
			ok = ck.servers[ck.leader].Call("KVServer.Get", args, reply)
		} else {
			ok = ck.servers[i].Call("KVServer.Get", args, reply)
		}

		if ok {
			if reply.Err == ErrWrongLeader {
				if ck.leader != -1 {
					ck.leader = -1
				} else {
					i = (i + 1) % len(ck.servers)
				}
			} else {
				DPrintf("Got result for get call, reply is %v", reply)
				if ck.leader == -1 {
					ck.leader = i
				}
				break
			}
		} else {
			i = (i + 1) % len(ck.servers)
		}
	}

	// You will have to modify this function.
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// PutAppend arg
	args := &PutAppendArgs{}
	args.Key = key
	args.RequestId = ck.seqId
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId

	// update seq Id
	ck.seqId++

	// Get reply
	reply := &PutAppendReply{}

	i := 0
	for {
		ok := false
		if ck.leader != -1 {
			ok = ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
			DPrintf("Send put/append call to server %v", ck.leader)
		} else {
			ok = ck.servers[i].Call("KVServer.PutAppend", args, reply)
			DPrintf("Send put/append call to server %v", i)
		}

		if ok {
			if reply.Err == ErrWrongLeader {
				if ck.leader != -1 {
					ck.leader = -1
				} else {
					i = (i + 1) % len(ck.servers)
				}
			} else {
				DPrintf("Got result for put/append call, reply is %v", reply)
				if ck.leader == -1 {
					ck.leader = i
				}
				break
			}
		} else {
			i = (i + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
