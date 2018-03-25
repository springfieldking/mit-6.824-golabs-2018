package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	leader	  uint32
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
	// You'll have to add code here.
	ck.sessionId = nrand()
	ck.requestId = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) setLastLeader(leader uint32) {
	ck.mu.Lock()
	ck.leader = leader
	ck.mu.Unlock()
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	ck.requestId++
	args := GetArgs{key, ck.sessionId, ck.requestId}
	reply := GetReply{}
	leader := ck.leader
	ck.mu.Unlock()

	//
	for {
		server := ck.servers[leader]
		ok := server.Call("KVServer.Get", &args, &reply);
		if !ok || reply.Err == FaultWrongLeader {
			leader ++
			leader = leader % uint32(len(ck.servers))
			continue
		}

		ck.setLastLeader(leader)

		switch reply.Err {
		case OK:
			return reply.Value
		case ErrNoKey:
			return ""
		default:
			continue
		}

	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.requestId++
	args := PutAppendArgs{key, value, op, ck.sessionId, ck.requestId}
	reply := PutAppendReply{}
	leader := ck.leader
	ck.mu.Unlock()

	for {
		server := ck.servers[leader]
		ok := server.Call("KVServer.PutAppend", &args, &reply);
		if !ok || reply.Err == FaultWrongLeader {
			leader ++
			leader = leader % uint32(len(ck.servers))
			continue
		}

		ck.setLastLeader(leader)

		switch reply.Err {
		case OK:
			return
		default:
			continue
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
