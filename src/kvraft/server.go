package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


const (
	OpGet  		= "Get"
	OpPut		= "Put"
	OpAppend	= "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type	string
	Key 	string
	Val 	string

	SessionId int64
	RequestId uint32
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore  	map[string]string 	// current committed key-value pairs
	history     map[int64]uint32 	// client session map to committed requestID
	indexChan 	map[int] chan raft.ApplyMsg
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// execute
	ret := kv.execute(args.SessionId, args.RequestId, Op{OpGet,"","", args.SessionId, args.RequestId})

	// ret
	reply.Err = ret
	if reply.Err != OK {
		DPrintf("[server=%-2d] args=%v reply=%v", kv.me, args, reply)
		return
	}

	// check value
	kv.mu.Lock()
	v, ok := kv.kvStore[args.Key]
	kv.mu.Unlock()

	// reply
	if ok {
		reply.Err = OK
		reply.Value = v
	} else {
		reply.Err = ErrNoKey
	}

	DPrintf("[server=%-2d] args=%v reply=%v", kv.me, args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// check args
	if args.Op != OpPut && args.Op != OpAppend {
		// i can not handle this command, try other server
		reply.Err = FaultWrongLeader
		DPrintf("[server=%-2d] args=%v reply=%v", kv.me, args, reply)
		return
	}

	// execute
	reply.Err = kv.execute(args.SessionId, args.RequestId, Op{args.Op,args.Key,args.Value, args.SessionId, args.RequestId})

	DPrintf("[server=%-2d] args=%v reply=%v", kv.me, args, reply)
}

func (kv *KVServer) execute(sessionId int64, requestId uint32, op Op) Err {

	kv.mu.Lock()

	// check req
	if kv.history[sessionId] >= requestId {
		kv.mu.Unlock()
		return OK
	}

	// check leader
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return FaultWrongLeader
	}

	// create index chan for wait
	applyC := make(chan raft.ApplyMsg)
	kv.indexChan[index] = applyC
	kv.mu.Unlock()


	// destroy chan before return
	defer func() {
		kv.mu.Lock()
		close(kv.indexChan[index])
		delete(kv.indexChan, index)
		kv.mu.Unlock()
	}()

	// wait
	var msg raft.ApplyMsg
	select {
	case msg = <-applyC:
		// check term
		if msg.CommandTerm != term {
			// leader changed maybe
			return FaultWrongLeader
		}
	case <-time.After(time.Second):
		// leader changed maybe
		return FaultWrongLeader
	}

	return OK
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.history = make(map[int64]uint32)
	kv.indexChan = make(map[int]chan raft.ApplyMsg)
	go kv.readApplyC(kv.applyCh)

	return kv
}

func (kv *KVServer) readApplyC(applyC <-chan raft.ApplyMsg) {

	doNotify := func(index int, msg raft.ApplyMsg) {
		// get index chan
		kv.mu.Lock()
		indexChan, ok := kv.indexChan[index]
		kv.mu.Unlock()

		// notify
		if ok {
			indexChan <- msg
		}
	}

	for msg := range applyC {

		if !msg.CommandValid {
			continue
		}

		if op, ok := msg.Command.(Op); ok {
			sessionId := op.SessionId
			requestId := op.RequestId
			index 	  := msg.CommandIndex

			// check req has applied
			kv.mu.Lock()
			hasApplied := kv.history[sessionId] >= requestId
			kv.mu.Unlock()

			// if has applied, notify and skip
			if hasApplied {
				doNotify(index, msg)
				continue
			}

			// if has not applied, do apply and notify
			kv.mu.Lock()
			// apply log
			switch op.Type {
			case OpGet:
				// do nothing
			case OpPut:
				kv.kvStore[op.Key] = op.Val
			case OpAppend:
				kv.kvStore[op.Key] += op.Val
			}

			// set req history
			kv.history[sessionId] = requestId
			kv.mu.Unlock()

			// do notify
			doNotify(index, msg)
		}
	}
}
