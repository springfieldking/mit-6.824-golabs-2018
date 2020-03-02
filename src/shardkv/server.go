package shardkv

// import "shardmaster"
import (
	"bytes"
	"labrpc"
	"log"
	"shardmaster"
	"time"
)
import "raft"
import "sync"
import "labgob"

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SessionId int64
	RequestId uint32

	Type string
	Key  string
	Val  string
}

type ApplyRet struct {
	Err Err
	Val string
	ApplyMsg raft.ApplyMsg
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore    map[string]string // current committed key-value pairs
	history    map[int64]uint32  // client session map to committed requestID
	noticeChan map[int]chan ApplyRet

	config    shardmaster.Config
	mck       *shardmaster.Clerk
	cfgTicker *time.Ticker
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := &Op{
		SessionId: args.SessionId,
		RequestId: args.RequestId,
		Type:      OpGet,
		Key:       args.Key,
	}
	isLeader, err := kv.exec(op)
	reply.WrongLeader = !isLeader
	reply.Err = err
	reply.Value = op.Val
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	isLeader, err := kv.exec(&Op{
		SessionId: args.SessionId,
		RequestId: args.RequestId,
		Type:      args.Op,
		Val:       args.Value,
		Key:       args.Key,
	})

	reply.WrongLeader = !isLeader
	reply.Err = err
}

func (kv *ShardKV) exec(op *Op) (isLeader bool, err Err) {
	// Your code here.
	kv.mu.Lock()

	// check req
	/*
	if kv.history[op.SessionId] >= op.RequestId {
		kv.mu.Unlock()
		DPrintf("[server=%-2d] repeated call return %v", kv.me, *op)
		return true, OK
	}
	*/

	var index, term int

	index, term, isLeader = kv.rf.Start(*op)
	if !isLeader {
		err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// create index chan for wait
	noticeCh := make(chan ApplyRet)
	kv.noticeChan[index] = noticeCh
	kv.mu.Unlock()

	defer DPrintf("[server=%-2d] exec op=%v index=%v term=%v", kv.me, *op, index, term)

	// destroy chan before return
	defer func() {
		kv.mu.Lock()
		delete(kv.noticeChan, index)
		kv.mu.Unlock()
	}()

	// wait apply msg
	var ret ApplyRet
	select {
	case ret = <-noticeCh:
		// check term
		if ret.ApplyMsg.CommandTerm != term {
			// leader changed maybe
			DPrintf("[server=%-2d] execute fail, term out, op=%v", kv.me, *op)
			return false, ErrWrongLeader
		}
	case <-time.After(time.Second):
		// leader changed maybe
		DPrintf("[server=%-2d] execute fail, time out, op=%v", kv.me, *op)
		return false, ErrWrongLeader
	}

	DPrintf("[server=%-2d] execute end, op=%v", kv.me, *op)
	err = ret.Err
	op.Val = ret.Val
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvStore = make(map[string]string)
	kv.history = make(map[int64]uint32)
	kv.noticeChan = make(map[int]chan ApplyRet)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.cfgTicker = time.NewTicker(50 * time.Millisecond)

	// start bg
	go kv.tick()
	go kv.readApplyCh()
	return kv
}

func (kv *ShardKV) tick() {

	// exec before tick
	kv.fetchConfig()

	for {
		select {
		case <-kv.cfgTicker.C:
			go kv.fetchConfig()
		}
	}
}

func (kv *ShardKV) fetchConfig() {
	if _, isLeader := kv.rf.GetState();!isLeader{return}
	DPrintf("[server=%-2d] configFetch config num=%v", kv.me, kv.config.Num + 1)
	var config = kv.mck.Query(kv.config.Num + 1)
	kv.rf.Start(config)
}

func (kv *ShardKV) readApplyCh() {

	doNotify := func(index int, msg ApplyRet) {
		// get index chan
		kv.mu.Lock()
		indexChan, ok := kv.noticeChan[index]
		kv.mu.Unlock()

		// notify
		if ok {
			DPrintf("[server=%-2d] apply ret=%v", kv.me, msg)
			indexChan <- msg
		}
	}

	for msg := range kv.applyCh {

		if !msg.CommandValid {
			continue
		}

		if msg.CommandIsSnapShot {
			if data, ok := msg.Command.([]byte); ok {
				kv.restoreSnapshot(data)
			}
			continue
		}

		if op, ok := msg.Command.(Op); ok {
			sessionId := op.SessionId
			requestId := op.RequestId
			index := msg.CommandIndex

			// check req has applied
			var val = ""
			kv.mu.Lock()
			hasApplied := kv.history[sessionId] >= requestId
			if op.Type == OpGet {val = kv.kvStore[op.Key]}
			kv.mu.Unlock()

			// if has applied, notify and skip
			if hasApplied {
				doNotify(index, ApplyRet{Err:OK, Val:val, ApplyMsg:msg})
				continue
			}

			// if has not applied, do apply and notify
			kv.mu.Lock()
			var err Err
			if shard := key2shard(op.Key); kv.config.Shards[shard] == kv.gid {
				// apply log
				switch op.Type {
				case OpGet:
					val = kv.kvStore[op.Key]
				case OpPut:
					kv.kvStore[op.Key] = op.Val
				case OpAppend:
					kv.kvStore[op.Key] += op.Val
				}
				err = OK
			} else {
				//err = ErrWrongGroup
				DPrintf("[server=%-2d] apply ErrWrongGroup gid=%v shard=%v", kv.me, kv.gid, kv.config.Shards)
			}

			// set req history
			kv.history[sessionId] = requestId
			kv.mu.Unlock()

			// do notify
			doNotify(index, ApplyRet{Err:err, Val:val, ApplyMsg:msg})

			if 0 < kv.maxraftstate && int(float32(kv.maxraftstate)*float32(0.5)) <= kv.rf.GetRaftStateSize() {
				kv.saveSnapshot(index)
			}
		} else if config, ok := msg.Command.(shardmaster.Config); ok {
			kv.mu.Lock()
			kv.config = config
			kv.mu.Unlock()

			DPrintf("[server=%-2d] configApply config=%v", kv.me, config)
		}
	}
}

func (kv *ShardKV) saveSnapshot(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.history)
	data := w.Bytes()

	kv.rf.SaveSnapshot(index, data)
}

func (kv *ShardKV) restoreSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data == nil || len(data) < 1 {
		// bootstrap without any state?
		kv.kvStore = make(map[string]string)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.kvStore) != nil ||
		d.Decode(&kv.history) != nil {
		panic("readSnapshot decode error !")
	}
}

const Debug = 1

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
