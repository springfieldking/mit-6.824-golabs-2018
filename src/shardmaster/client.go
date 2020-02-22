package shardmaster

//
// Shardmaster clerk.
//

import (
	"labrpc"
	"sync"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu        sync.Mutex
	leader	  int
	sessionId int64
	requestId uint32
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
	ck.sessionId = nrand()
	ck.requestId = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) nextSeqId() uint32 {
	return atomic.AddUint32(&ck.requestId, 1)
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{SessionId:ck.sessionId, RequestId:ck.nextSeqId(), Num:num}
	// Your code here.
	args.Num = num
	for {
		/*
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		*/
		var reply QueryReply
		ok := ck.servers[ck.leader].Call("ShardMaster.Query", args, &reply)
		if ok && reply.Err != FaultWrongLeader {
			// if reply.Err == "OK"{} else {}
			return reply.Config
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{SessionId:ck.sessionId, RequestId:ck.nextSeqId(), Servers:servers}
	// Your code here.
	args.Servers = servers

	for {
		/*
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		*/
		var reply JoinReply
		ok := ck.servers[ck.leader].Call("ShardMaster.Join", args, &reply)
		if ok && reply.Err != FaultWrongLeader {
			return
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{SessionId:ck.sessionId, RequestId:ck.nextSeqId(), GIDs:gids}
	// Your code here.
	args.GIDs = gids

	for {
		/*
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		*/
		var reply LeaveReply
		ok := ck.servers[ck.leader].Call("ShardMaster.Leave", args, &reply)
		if ok && reply.Err != FaultWrongLeader {
			return
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{SessionId:ck.sessionId, RequestId:ck.nextSeqId(), Shard:shard, GID:gid}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		/*
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		*/
		var reply MoveReply
		ok := ck.servers[ck.leader].Call("ShardMaster.Move", args, &reply)
		if ok && reply.Err != FaultWrongLeader {
			return
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
