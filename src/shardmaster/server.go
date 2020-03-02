package shardmaster


import (
	"bytes"
	"log"
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

const (
	OpJoin	= "Join"
	OpLeave	= "Leave"
	OpMove	= "Move"
	OpQuery	= "Query"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	history     map[int64]uint32 	// client session map to committed requestID
	noticeChan 	map[int] chan raft.ApplyMsg
}

type Op struct {
	// Your data here.
	SessionId 	int64
	RequestId 	uint32
	Type		string

	JoinServers map[int][]string // join req params new GID -> servers mappings
	LeaveGIDs 	[]int			 // leave req params
	MoveShard 	int				// move req params
	MoveGID   	int
	QueryNum 	int 			// query req params desired config number
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	isLeader, err := sm.exec(Op{
		Type: 		OpJoin,
		SessionId: 	args.SessionId,
		RequestId: 	args.RequestId,
		JoinServers: args.Servers,
	})
	reply.Err = err
	reply.WrongLeader = !isLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	isLeader, err := sm.exec(Op{
		Type: OpLeave,
		SessionId: args.SessionId,
		RequestId: args.RequestId,
		LeaveGIDs: args.GIDs,
	})
	reply.Err = err
	reply.WrongLeader = !isLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	isLeader, err := sm.exec(Op{
		Type: OpMove,
		SessionId: args.SessionId,
		RequestId: args.RequestId,
		MoveGID: args.GID,
		MoveShard: args.Shard,
	})
	reply.Err = err
	reply.WrongLeader = !isLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	isLeader, err := sm.exec(Op{
		Type: OpQuery,
		SessionId: args.SessionId,
		RequestId: args.RequestId,
		QueryNum: args.Num,
	})
	reply.Err = err
	reply.WrongLeader = !isLeader

	if reply.Err == OK {
		sm.mu.Lock()
		reply.Config = sm.getConfig(args.Num)
		sm.mu.Unlock()
		DPrintf("%v", reply)
	}
}

func (sm *ShardMaster) exec(op Op) (isLeader bool, err Err) {
	// Your code here.
	sm.mu.Lock()

	// check req
	if sm.history[op.SessionId] >= op.RequestId {
		sm.mu.Unlock()
		DPrintf("[server=%-2d] repeated call return %v", sm.me, op)
		return true, OK
	}

	var index, term int

	index, term, isLeader = sm.rf.Start(op)
	if !isLeader {
		err = FaultWrongLeader
		sm.mu.Unlock()
		return
	}

	// create index chan for wait
	noticeCh := make(chan raft.ApplyMsg)
	sm.noticeChan[index] = noticeCh
	sm.mu.Unlock()

	defer DPrintf("[server=%-2d] exec op=%v index=%v term=%v", sm.me, op, index, term)

	// destroy chan before return
	defer func() {
		sm.mu.Lock()
		delete(sm.noticeChan, index)
		sm.mu.Unlock()
	}()


	// wait apply msg
	var msg raft.ApplyMsg
	select {
	case msg = <-noticeCh:
		// check term
		if msg.CommandTerm != term {
			// leader changed maybe
			DPrintf("[server=%-2d] execute fail, term out, op=%v", sm.me, op)
			return false, FaultWrongLeader
		}
	case <-time.After(time.Second):
		// leader changed maybe
		DPrintf("[server=%-2d] execute fail, time out, op=%v", sm.me, op)
		return false, FaultWrongLeader
	}

	DPrintf("[server=%-2d] execute end, op=%v", sm.me, op)
	err = OK
	return
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.history = make(map[int64]uint32)
	sm.noticeChan = make(map[int]chan raft.ApplyMsg)

	// start bg
	go sm.readApplyCh()
	return sm
}

func (sm *ShardMaster) readApplyCh() {

	doNotify := func(index int, msg raft.ApplyMsg) {
		// get index chan
		sm.mu.Lock()
		indexChan, ok := sm.noticeChan[index]
		sm.mu.Unlock()

		// notify
		if ok {
			DPrintf("[server=%-2d] apply msg=%v", sm.me, msg)
			indexChan <- msg
		}
	}

	for msg := range sm.applyCh {

		if !msg.CommandValid {
			continue
		}

		if msg.CommandIsSnapShot {
			if data, ok := msg.Command.([]byte); ok {
				sm.restoreSnapshot(data)
			}
			continue
		}

		if op, ok := msg.Command.(Op); ok {
			sessionId := op.SessionId
			requestId := op.RequestId
			index 	  := msg.CommandIndex

			sm.mu.Lock()
			hasApplied := sm.history[sessionId] >= requestId
			sm.mu.Unlock()

			// if has applied, notify and skip
			if hasApplied {
				DPrintf("[server=%-2d] hasApplied call return %v", sm.me, op)
				doNotify(index, msg)
				continue
			}

			// if has not applied, do apply and notify
			sm.mu.Lock()
			// apply log
			switch op.Type {
			case OpJoin:
				// handle join
				sm.handleJoin(op.JoinServers)
			case OpLeave:
				// handle leave
				sm.handleLeave(op.LeaveGIDs)
			case OpMove:
				// handle move
				sm.handleMove(op.MoveShard, op.MoveGID)
			case OpQuery:
				// do noting
			}

			// set req history
			sm.history[sessionId] = requestId
			sm.mu.Unlock()

			// do notify
			doNotify(index, msg)

			DPrintf("[server=%-2d] readApplyMsg op=%v",sm.me, op)
		}
	}
}

func (sm *ShardMaster) handleJoin(servers map[int][]string) {
	var config = sm.getConfig(-1)
	defer func() {
		config.Num = len(sm.configs)
		sm.configs = append(sm.configs, config)
	}()

	// merge groups
	var newGIDs []int
	for gid, ss := range servers {
		if gs, ok := config.Groups[gid]; ok {
			config.Groups[gid] = append(gs, ss...)
		} else {
			config.Groups[gid] = ss
			newGIDs = append(newGIDs, gid)
		}
	}

	shardEvenly(&config, newGIDs)
}

func (sm *ShardMaster) handleLeave(GIDs []int) {
	var config = sm.getConfig(-1)
	defer func() {
		config.Num = len(sm.configs)
		sm.configs = append(sm.configs, config)
	}()

	delGIDs := make(map[int]bool)
	for _, g := range GIDs {
		delete(config.Groups, g)
		delGIDs[g] = true
	}

	// clear if no group exist and return
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
		return
	}

	// clear shard at s
	for s, g := range config.Shards {
		if delGIDs[g] {
			config.Shards[s] = 0
		}
	}

	// get remaining gid
	var remainingGIDs []int
	for g, _ := range config.Groups {
		remainingGIDs = append(remainingGIDs, g)
	}

	shardEvenly(&config, remainingGIDs)
}

func (sm *ShardMaster) handleMove(shard int, gid int) {
	var config = sm.getConfig(-1)
	defer func() {
		config.Num = len(sm.configs)
		sm.configs = append(sm.configs, config)
	}()

	config.Shards[shard] = gid
}

func (sm *ShardMaster) getConfig(i int) Config {
	var srcConfig Config
	if i < 0 || i >= len(sm.configs) {
		srcConfig = sm.configs[len(sm.configs)-1]
	} else {
		srcConfig = sm.configs[i]
	}
	dstConfig := Config{Num: srcConfig.Num, Shards: srcConfig.Shards, Groups: make(map[int][]string)}
	for gid, servers := range srcConfig.Groups {
		dstConfig.Groups[gid] = append([]string{}, servers...)
	}
	return dstConfig
}

func shardEvenly(config *Config, insertGIDS []int) {

	var minShardsPerGroup = NShards / len(config.Groups)
	var maxShardsPerGroup = minShardsPerGroup
	var shardRemaining = NShards % len(config.Groups)
	if shardRemaining != 0 {
		maxShardsPerGroup += 1
	}

	var insertGIDIndex = 0
	var shardByGID = make(map[int]int)
	for s, g := range config.Shards {

		var insert = false
		if g == 0 {
			// empty shard could be insert at s
			insert = true
		} else if shardByGID[g] < minShardsPerGroup {
			// less than min shard
			insert = false
		} else if shardByGID[g] < maxShardsPerGroup && shardRemaining > 0 {
			// greater than min and less than max and has remaining
			insert = false
			shardRemaining --
		} else {
			insert = true
		}

		// find a valid gid insert at s
		if insert {
			var newGID = 0
			for ;;insertGIDIndex++ {
				newGID = insertGIDS[insertGIDIndex%len(insertGIDS)]; insertGIDIndex++
				if shardByGID[newGID] < minShardsPerGroup {
					// less than min shard
					break
				} else if shardByGID[newGID] < maxShardsPerGroup && shardRemaining > 0 {
					// greater than min and less than max and has remaining
					shardRemaining --
					break
				}
			}

			g = newGID
			config.Shards[s] = g
		}

		shardByGID[g] += 1
	}

}


func (sm *ShardMaster) saveSnapshot(index int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.history)
	data := w.Bytes()

	sm.rf.SaveSnapshot(index, data)
}

func (sm *ShardMaster) restoreSnapshot(data []byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if data == nil || len(data) < 1 {
		// bootstrap without any state?
		sm.configs = make([]Config, 0)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&sm.configs) != nil ||
		d.Decode(&sm.history) != nil {
		panic("readSnapshot decode error !")
	}
}


const Debug = 0
func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}