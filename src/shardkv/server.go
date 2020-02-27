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
	CSOpGet    = "Get"
	CSOpPut    = "Put"
	CSOpAppend = "Append"

	SSSyncCfg = "SyncCfg"
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

	Ext interface{}
}

type OpResult struct {
	IsLeader 	bool
	Err 		Err
	applyMsg 	raft.ApplyMsg
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
	noticeChan map[int]chan OpResult

	config    shardmaster.Config
	mck       *shardmaster.Clerk
	cfgTicker *time.Ticker
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ret := kv.exec(Op{
		SessionId: args.SessionId,
		RequestId: args.RequestId,
		Type:      CSOpGet,
		Key:       args.Key,
	})

	reply.WrongLeader = !ret.IsLeader
	reply.Err = ret.Err

	// get value
	if ret.Err == OK {
		kv.mu.Lock()
		v, ok := kv.kvStore[args.Key]
		if ok {
			reply.Value = v
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ret := kv.exec(Op{
		SessionId: args.SessionId,
		RequestId: args.RequestId,
		Type:      args.Op,
		Val:       args.Value,
		Key:       args.Key,
	})

	reply.WrongLeader = !ret.IsLeader
	reply.Err = ret.Err
}

func (kv *ShardKV) exec(op Op) OpResult {
	// Your code here.
	kv.mu.Lock()

	// check req
	if kv.history[op.SessionId] >= op.RequestId {
		kv.mu.Unlock()
		DPrintf("[server=%-2d] repeated call return %v", kv.me, op)
		return OpResult{IsLeader: true, Err: OK}
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return OpResult{IsLeader: false, Err: ErrWrongLeader}
	}

	// create index chan for wait
	noticeCh := make(chan OpResult)
	kv.noticeChan[index] = noticeCh
	kv.mu.Unlock()

	defer DPrintf("[server=%-2d] exec op=%v index=%v term=%v", kv.me, op, index, term)

	// destroy chan before return
	defer func() {
		kv.mu.Lock()
		delete(kv.noticeChan, index)
		kv.mu.Unlock()
	}()

	// wait apply msg
	var opRet OpResult
	select {
	case opRet = <-noticeCh:
		// check term
		if opRet.applyMsg.CommandTerm != term {
			// leader changed maybe
			DPrintf("[server=%-2d] execute fail, term out, op=%v", kv.me, op)
			return OpResult{IsLeader: false, Err: ErrWrongLeader}
		}
	case <-time.After(time.Second):
		// leader changed maybe
		DPrintf("[server=%-2d] execute fail, time out, op=%v", kv.me, op)
		return OpResult{IsLeader: false, Err: ErrWrongLeader}
	}

	DPrintf("[server=%-2d] execute end, op=%v", kv.me, op)
	opRet.IsLeader = true
	return OpResult{IsLeader: false, Err: OK}
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
	kv.noticeChan = make(map[int]chan OpResult)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.cfgTicker = time.NewTicker(100 * time.Millisecond)

	// start bg
	go kv.readApplyCh(kv.applyCh)
	go kv.tick()
	return kv
}

func (kv *ShardKV) tick() {
	select {
	case <-kv.cfgTicker.C:
		go kv.pullConfig()

	}
}

func (kv *ShardKV) pullConfig() {
	if _, isLeader := kv.rf.GetState(); isLeader {
		var config = kv.mck.Query(kv.config.Num + 1)
		kv.exec(Op{
			Type: SSSyncCfg,
			Ext:  config,
		})
	}
}

func (kv *ShardKV) readApplyCh(applyC <-chan raft.ApplyMsg) {

	doNotify := func(index int, opRet OpResult) {
		// get index chan
		kv.mu.Lock()
		indexChan, ok := kv.noticeChan[index]
		kv.mu.Unlock()

		// notify
		if ok {
			DPrintf("[server=%-2d] apply opRet=%v", kv.me, opRet)
			indexChan <- opRet
		}
	}

	for msg := range applyC {

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
			// sessionId 0 mean server req, so no need check
			var hasApplied = false
			if sessionId > 0 {
				kv.mu.Lock()
				hasApplied = kv.history[sessionId] >= requestId
				kv.mu.Unlock()
			}

			// if has applied, notify and skip
			if hasApplied {
				doNotify(index, OpResult{IsLeader:true, Err:OK, applyMsg:msg})
				continue
			}

			checkShard := func(key string) (err Err) {
				err = "OK"
				shard := key2shard(op.Key)
				if kv.config.Shards[shard] != kv.gid {
					//err = ErrWrongGroup
				}
				return
			}

			// if has not applied, do apply and notify
			kv.mu.Lock()
			// apply log
			var err Err = OK
			switch op.Type {
			case CSOpGet: {
				err = checkShard(op.Key)
			}
			case CSOpPut: {
				err = checkShard(op.Key)
				if err == OK {
					kv.kvStore[op.Key] = op.Val
				}
			}
			case CSOpAppend: {
				err = checkShard(op.Key)
				if err == OK {
					kv.kvStore[op.Key] += op.Val
				}
			}
			case SSSyncCfg: {
				if cfg, ok := op.Ext.(shardmaster.Config); ok {
					kv.config = cfg
				}
			}
			}

			// set req history
			kv.history[sessionId] = requestId
			kv.mu.Unlock()

			// do notify
			doNotify(index, OpResult{IsLeader:true, Err:err, applyMsg:msg})

			if 0 < kv.maxraftstate && int(float32(kv.maxraftstate)*float32(0.5)) <= kv.rf.GetRaftStateSize() {
				kv.saveSnapshot(index)
			}
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
