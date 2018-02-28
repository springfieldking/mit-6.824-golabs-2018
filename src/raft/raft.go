package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "labgob"

// StateType represents the role of a node in a cluster.
type StateType uint32

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	term 	int				// when entry was received by leader
	command interface{}		// command for state machine
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int 		// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor	int			// candidateId that received vote in current term (or null if none)
	log 		[]LogEntry	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex	int			// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int			// index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	nextIndex 	[]int		// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex 	[]int		// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Other volatile state
	currState 			StateType
	voteCount			int
	// event quene
	chanEvent 			chan EventType
	// timer
	timers 				[]*time.Timer
	// state func
	eventHandlers  		[]StateEventHandler
	msgHandlers    		[]StateMsgHandler
	changeHandlers 		[]StateChange
	// rand
	rand                *rand.Rand
	// lockSate
	isLock 				bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = rf.currState == StateLeader

	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type AppendEntriesArgs struct {
	Term 			int 			// leader’s term
	LeaderId		int 			// so follower can redirect clients
	PrevLogIndex 	int 			// index of log entry immediately preceding new ones
	PrevLogTerm 	int 			// term of prevLogIndex entry
	Entries			[]interface{} 	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit	int 			// leader’s commitIndex
}

type AppendEntriesReply  struct {
	Term			int 		// currentTerm, for leader to update itself
	Success			bool		// true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// bcastAppend sends RRPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (rf *Raft) bcastAppendEntries() {
	//lastLogIndex, lastLogTerm := rf.getLastLog()
	currentTerm := rf.currentTerm

	go func() {
		for p := range rf.peers {
			if p == rf.me {
				continue
			}
			go func(peer int) {
				args := AppendEntriesArgs{Term:currentTerm, LeaderId:rf.me}
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(peer, &args, &reply); ok {

				}
			}(p)
		}
	}()
}

// bcastHeartbeat sends RRPC, without entries to all the peers.
func (rf *Raft) bcastHeartbeat() {
	//lastLogIndex, lastLogTerm := rf.getLastLog()
	currentTerm := rf.currentTerm

	go func() {
		for p := range rf.peers {
			if p == rf.me {
				continue
			}
			go func(peer int) {
				args := AppendEntriesArgs{Term:currentTerm, LeaderId:rf.me}
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(peer, &args, &reply); ok {

				}
			}(p)
		}
	}()
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int		// candidate’s term
	CandidateId		int		// candidate requesting vote
	LastLogIndex	int		// index of candidate’s last log entry
	LastLogTerm		int		// term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int		// currentTerm, for candidate to update itself
	voteGranted bool	// true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) bcastRequestVote() {

	go func() {
		for p := range rf.peers {
			if p == rf.me {
				continue
			}
			go func(peer int) {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(peer, &args, &reply); ok {

				}
			}(p)
		}
	}()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{term:0})
	go rf.background()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}


func (rf *Raft) doLock() {
	rf.mu.Lock()
	rf.isLock = true
}

func (rf *Raft) doUnlock() {
	rf.isLock = false
	rf.mu.Unlock()
}

func (rf *Raft) isLocked() bool {
	return rf.isLock == true
}

type MsgType uint32
const (
	AppendEntries 	MsgType = iota
	RequestVote
)

type EventType uint32
const (
	EventAppendEntries            EventType = iota // AppendEntries or HeartBeats
	EventRequestVote
	EventFollowerElectionTimeout
	EventCandidateElectionTimeout
	EventLeaderHeartbeatTimeout
	EventShutDown
)

type StateEventHandler 	func (EventType)
type StateMsgHandler 	func (MsgType, *struct{}, *struct{})
type StateChange 		func ()

var globalRandSeedInitFlag int32 = 0

//
// create a background, a state mashine loop
//
func (rf *Raft) background() {
	// init lock state
	rf.isLock = false

	// init rand
	rf.rand = rand.New(rand.NewSource(int64(rf.me)))
	
	// init event channel
	rf.chanEvent = make(chan EventType)

	// init timer
	rf.timers = []*time.Timer{time.NewTimer(0), time.NewTimer(0), time.NewTimer(0)}
	for _, timer := range rf.timers {
		timer.Stop()
	}

	// timer watch
	go func() {
		select {
		case <-rf.timers[StateFollower].C:
			rf.chanEvent <- EventFollowerElectionTimeout
		case <-rf.timers[StateCandidate].C:
			rf.chanEvent <- EventCandidateElectionTimeout
		case <-rf.timers[StateLeader].C:
			rf.chanEvent <- EventLeaderHeartbeatTimeout
		}
	}()

	// init state handler
	rf.eventHandlers = []StateEventHandler{rf.followerEventHandler, rf.candidateEventHandler, rf.leaderEventHandler}
	rf.msgHandlers = []StateMsgHandler{rf.followerMsgHandler, rf.candidateMsgHandler, rf.leaderMsgHandler}
	rf.changeHandlers = []StateChange{rf.followerChange, rf.candidateChange, rf.leaderChange}

	// init state
	rf.doLock()
	rf.become(StateFollower)
	rf.doUnlock()

	// state machine loop
	for {
		event := <- rf.chanEvent
		if event == EventShutDown {
			return
		}

		rf.doLock()
		rf.handleEvent(event)
		rf.doUnlock()
	}
}

func (rf *Raft) handleEvent(event EventType)  {
	if !rf.isLocked() {
		panic("need locked before call handleEvent!")
	}
	rf.eventHandlers[rf.currState](event)
}

func (rf *Raft) handleMsg(msg MsgType, args *struct{}, reply *struct{})  {
	if !rf.isLocked() {
		panic("need locked before call handleMsg!")
	}
	rf.msgHandlers[rf.currState](msg, args, reply)
}

func (rf *Raft) become(state StateType)  {
	if !rf.isLocked() {
		panic("need locked before call become!")
	}
	rf.currState = state
	rf.changeHandlers[rf.currState]()
}

const ElectionTimeOut = 300

func (rf *Raft) followerChange()  {
	rf.timers[StateFollower].Reset(time.Duration(rand.Intn(ElectionTimeOut) +ElectionTimeOut) * time.Millisecond)
}

func (rf *Raft) followerEventHandler(event EventType)  {
	// If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate: convert to candidate
	switch event {
	case EventAppendEntries, EventRequestVote:
		DPrintf("raft = ", rf.me, " state = ", rf.currState, " got event = ", event, " do become(StateFollower)")
		rf.become(StateFollower)
	case EventFollowerElectionTimeout:
		DPrintf("raft = ", rf.me, " state = ", rf.currState, " got event = ", event, " do become(StateCandidate)")
		rf.become(StateCandidate)
	default:
		DPrintf("raft = ", rf.me, " state = ", rf.currState, " got event = ", event, " Unexpected, do nothing")
	}
}

func (rf *Raft) followerMsgHandler(msg MsgType, args *struct{}, reply *struct{})  {

}

func (rf *Raft) getLastLog() (int, int) {
	len := len(rf.log)
	return len, rf.log[len - 1].term
}

func (rf *Raft) candidateChange()  {
	// increment currentTerm
	rf.currentTerm ++
	// vote for self
	rf.votedFor = rf.me
	// reset voteCount
	rf.voteCount = 0
	// reset election timer
	rf.timers[StateCandidate].Reset(time.Duration(rand.Intn(ElectionTimeOut) +ElectionTimeOut) * time.Millisecond)

	// send RequestVote RPCs to all other servers
	rf.bcastRequestVote()
}

func (rf *Raft) candidateEventHandler(event EventType)  {
	// If AppendEntries RPC received from new leader: convert to follower
	// If election timeout elapses: start new election
	switch event {
	case EventAppendEntries:
		DPrintf("raft = ", rf.me, " state = ", rf.currState, " got event = ", event, " do become(StateFollower)")
		rf.become(StateFollower)
	case EventCandidateElectionTimeout:
		DPrintf("raft = ", rf.me, " state = ", rf.currState, " got event = ", event, " do become(StateCandidate)")
		rf.become(StateCandidate)
	default:
		DPrintf("raft = ", rf.me, " state = ", rf.currState, " got event = ", event, " Unexpected, do nothing")
	}
}

func (rf *Raft) candidateMsgHandler(msg MsgType, args *struct{}, reply *struct{})  {

}

func (rf *Raft) leaderChange()  {
	// reset timer
	rf.timers[StateLeader].Reset(time.Duration(ElectionTimeOut) * time.Millisecond)

	// send initial empty AppendEntries RPCs (heartbeat) to each server
	rf.bcastHeartbeat()
}

func (rf *Raft) leaderEventHandler(event EventType)  {
	switch event {
	case EventLeaderHeartbeatTimeout:
		DPrintf("raft = ", rf.me, " state = ", rf.currState, " got event = ", event, " do become(StateLeader)")
		rf.become(StateLeader)
	default:
		DPrintf("raft = ", rf.me, " state = ", rf.currState, " got event = ", event, " Unexpected, do nothing")
	}
}

func (rf *Raft) leaderMsgHandler(msg MsgType, args *struct{}, reply *struct{})  {

}