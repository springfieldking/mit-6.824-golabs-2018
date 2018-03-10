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
	"sort"
	"bytes"
	"labgob"
	"log"
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
	Index 	int 			// index
	Term 	int				// when entry was received by leader
	Command interface{}		// command for state machine
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
	// event channel
	eventCh 			chan EventType
	// ApplyMsg channel
	applyCh 			chan ApplyMsg
	// timer
	timers 				[]*time.Timer
	// state func
	steps   			[]StateStep
	becomes 			[]StateChange
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
	rf.doLock()
	term = rf.currentTerm
	isleader = rf.currState == StateLeader
	rf.doUnlock()

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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log	[]LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		 d.Decode(&log) != nil	{
		 	panic("readPersist decode error !")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

type AppendEntriesArgs struct {
	Term 			int 			// leader’s term
	LeaderId		int 			// so follower can redirect clients
	PrevLogIndex 	int 			// index of log entry immediately preceding new ones
	PrevLogTerm 	int 			// term of prevLogIndex entry
	Entries			[]LogEntry 		// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit	int 			// leader’s commitIndex
}

type AppendEntriesReply  struct {
	Term			int 		// currentTerm, for leader to update itself
	Success			bool		// true if follower contained entry matching prevLogIndex and prevLogTerm
	Index			int 		// for update nextIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// recieve AppendEntries req
	rf.doLock()
	rf.handleAppendEntries(args, reply)
	rf.doUnlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		// recieve AppendEntries res
		rf.doLock()
		rf.handleReply(Event{event:EventAppendEntriesRes, success:reply.Success, term:reply.Term, peer:server, index:reply.Index})
		rf.doUnlock()
	}
	return ok
}

// bcastAppend sends RRPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (rf *Raft) bcastAppendEntries() {
	rf.assertLockHeld("need locked before call bcastAppendEntries!")

	for p := range rf.peers {
		if p == rf.me {
			continue
		}

		rf.sendAppendEntries2peer(p)
	}
}

func (rf *Raft) sendAppendEntries2peer(p int) {
	term 			:= rf.currentTerm
	leaderId		:= rf.me
	leaderCommit	:= rf.commitIndex
	nextLogIndex 	:= rf.nextIndex[p]
	prevLogIndex 	:= rf.matchIndex[p]
	prevLogTerm 	:= 0
	entries			:= []LogEntry{}

	if prevLogIndex > 0 {
		arrIndex := prevLogIndex - 1
		prevLogTerm 	= rf.log[arrIndex].Term
	}

	/** copy from matchIndex to min(nextLogIndex,end)
	 *
	 *	matchIndex	nextLogIndex	end
	 * 	--- | --------- | ---------- |
	 */
	matchIndex := rf.matchIndex[p]
	matchEndIndex := nextLogIndex
	if len(rf.log) < matchEndIndex  {
		matchEndIndex = len(rf.log)
	}
	entries = rf.log[matchIndex:matchEndIndex]

	go func(peer int) {
		args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(peer, &args, &reply)
	}(p)
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
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// recieve AppendEntries req
	rf.doLock()
	rf.handleRequestVote(args, reply)
	rf.doUnlock()
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
	if ok {
		// recieve AppendEntries res
		rf.doLock()
		rf.handleReply(Event{event:EventRequestVoteRes, success:reply.VoteGranted, term:reply.Term, peer:server})
		rf.doUnlock()
	}
	return ok
}

func (rf *Raft) bcastRequestVote() {
	rf.assertLockHeld("need locked before call bcastRequestVote!")

	term			:= rf.currentTerm
	candidateId		:= rf.me
	lastLogIndex	:= len(rf.log)
	lastLogTerm		:= 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex - 1].Term
	}

	go func() {
		for p := range rf.peers {
			if p == rf.me {
				continue
			}

			go func(peer int) {
				args := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
				reply := RequestVoteReply{}
				rf.sendRequestVote(peer, &args, &reply)
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
	rf.doLock()

	isLeader = rf.currState == StateLeader
	if isLeader {
		DPrintf("[raft=%-2d state=%-1d term=%-2d] Leader recieve cmd, before append log count = %d!", rf.me, rf.currState, rf.currentTerm, len(rf.log))

		index = len(rf.log)
		term = rf.currentTerm

		// If command received from client: append entry to local log,
		// respond after entry applied to state machine
		index ++
		entry := LogEntry{index, term, command}
		rf.log = append(rf.log, entry)
		rf.matchIndex[rf.me] = len(rf.log)

		DPrintf("[raft=%-2d state=%-1d term=%-2d] Leader recieve cmd, after append log count = %d!", rf.me, rf.currState, rf.currentTerm, len(rf.log))

		// check commit
		rf.leaderMaybeCommit()

		// todo too many rpcs need bcast?
		// rf.bcastAppendEntries()
	}

	rf.doUnlock()

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
	rf.eventCh <- EventShutDown
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
	// (initialized to 0 on first boot, increases monotonically)
	rf.currentTerm = 0
	// (initialized to 0, increases monotonically)
	rf.commitIndex = 0
	// (initialized to 0, increases monotonically)
	rf.lastApplied = 0
	// (or null if none)
	rf.votedFor = -1
	// (first index is 1)
	rf.log = []LogEntry{}

	// init apply chan
	rf.applyCh = applyCh

	// start a background
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

func (rf * Raft) assertLockHeld(v interface{})  {
	if !rf.isLock {
		panic(v)
	}
}

type EventType uint32
const (
	EventFollowerElectionTimeout EventType = iota
	EventCandidateElectionTimeout
	EventLeaderHeartbeatTimeout
	EventAppendEntriesReq
	EventAppendEntriesRes
	EventRequestVoteReq
	EventRequestVoteRes
	EventShutDown
)

type Event struct {
	event 	EventType

	// for req & reply
	term	int
	success bool

	// for reply
	peer 	int

	// for app reply
	index 	int
}

type StateStep 			func (Event)
type StateChange 		func ()

//
// create a background, a state mashine loop
//
func (rf *Raft) background() {
	// init lock state
	rf.isLock = false

	// init rand
	rf.rand = rand.New(rand.NewSource(int64(rf.me)))
	
	// init event channel
	rf.eventCh = make(chan EventType)

	// init timer
	rf.timers = []*time.Timer{time.NewTimer(0), time.NewTimer(0), time.NewTimer(0)}
	for _, timer := range rf.timers {
		timer.Stop()
	}

	// channel watch
	go func() {
		for {
			select {
			case <-rf.timers[StateFollower].C:
				rf.eventCh <- EventFollowerElectionTimeout
			case <-rf.timers[StateCandidate].C:
				rf.eventCh <- EventCandidateElectionTimeout
			case <-rf.timers[StateLeader].C:
				rf.eventCh <- EventLeaderHeartbeatTimeout
			}
		}
	}()

	// init state handler
	rf.steps = []StateStep{rf.stepFollower, rf.stepCandidate, rf.leaderStep}
	rf.becomes = []StateChange{rf.becomeFollower, rf.becomeCandidate, rf.becomeLeader}

	// init log
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// init state
	rf.doLock()
	DPrintf("[raft=%-2d state=%-1d term=%-2d] first init, become(StateFollower)!", rf.me, rf.currState, rf.currentTerm)
	rf.become(StateFollower)
	rf.doUnlock()

	// state machine loop
	for {
		event := <- rf.eventCh
		if event == EventShutDown {
			return
		}

		rf.doLock()
		rf.step(Event{event:event})
		rf.doUnlock()
	}
}

func (rf *Raft) q() int { return len(rf.peers)/2 + 1 }

func (rf *Raft) reset()  {
	rf.votedFor = -1
	rf.voteCount = 0
}

func (rf *Raft) step(event Event)  {
	rf.assertLockHeld("need locked before call handleEvent!")
	rf.steps[rf.currState](event)
}

func (rf *Raft) become(state StateType)  {
	rf.assertLockHeld("need locked before call become!")

	if rf.currState == StateFollower && state == StateLeader {
		panic("invalid transition [follower -> leader]")
	}

	if rf.currState == StateLeader && state == StateCandidate {
		panic("invalid transition [leader -> candidate]")
	}

	rf.currState = state
	rf.reset()
	rf.becomes[rf.currState]()
}

const ElectionTimeOut 	= 150
const LeaderTimeOut 	= 100
func (rf *Raft) resetTimer()  {
	switch rf.currState {
	case StateFollower:
		rf.timers[StateFollower].Reset(time.Duration(rand.Intn(ElectionTimeOut) +ElectionTimeOut) * time.Millisecond)
	case StateCandidate:
		rf.timers[StateCandidate].Reset(time.Duration(rand.Intn(ElectionTimeOut) +ElectionTimeOut) * time.Millisecond)
	case StateLeader:
		rf.timers[StateLeader].Reset(time.Duration(LeaderTimeOut) * time.Millisecond)
	}
}

func (rf *Raft) becomeFollower()  {
	rf.resetTimer()
}

func (rf *Raft) stepFollower(event Event)  {
	// If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate: convert to candidate
	switch event.event {
	case EventAppendEntriesReq:
		DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, resetTimer!", rf.me, rf.currState, rf.currentTerm, event.event)
		rf.resetTimer()
	case EventRequestVoteReq:
		DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, resetTimer!", rf.me, rf.currState, rf.currentTerm, event.event)
		rf.resetTimer()
	case EventFollowerElectionTimeout:
		DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, become(StateCandidate)!", rf.me, rf.currState, rf.currentTerm, event.event)
		rf.become(StateCandidate)
	default:
		DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, unexpected do nothing!", rf.me, rf.currState, rf.currentTerm, event.event)
	}
}

func (rf *Raft) becomeCandidate()  {
	// increment currentTerm
	rf.currentTerm ++
	// vote for self
	rf.votedFor = rf.me
	// reset voteCount
	rf.voteCount = 1
	// reset election timer
	rf.resetTimer();

	// send RequestVote RPCs to all other servers
	rf.bcastRequestVote()
}

func (rf *Raft) stepCandidate(event Event)  {
	switch event.event {
	case EventCandidateElectionTimeout:
		DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, become(StateCandidate)!", rf.me, rf.currState, rf.currentTerm, event.event)
		// If election timeout elapses: start new election
		rf.become(StateCandidate)
	case EventAppendEntriesReq:
		DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, become(StateFollower)!", rf.me, rf.currState, rf.currentTerm, event.event)
		// If AppendEntries RPC received from new leader: convert to follower
		rf.become(StateFollower)
	case EventRequestVoteRes:
		// If votes received from majority of servers: become leader
		if event.success {
			DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, add vote!", rf.me, rf.currState, rf.currentTerm, event.event)
			rf.voteCount ++
			if rf.voteCount >= rf.q() {
				DPrintf("[raft=%-2d state=%-1d term=%-2d] got enough vote, become(StateLeader)!", rf.me, rf.currState, rf.currentTerm)
				rf.become(StateLeader)
			}
		} else {
			DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, do nothing!", rf.me, rf.currState, rf.currentTerm, event.event)
			if event.term > rf.currentTerm {
				/* become before step
				rf.become(StateFollower)
				*/
			}
		}

	default:
		DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, unexpected do nothing!", rf.me, rf.currState, rf.currentTerm, event.event)
	}
}

func (rf *Raft) becomeLeader()  {
	// reset timer
	rf.resetTimer();

	// Reinitialized after election
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialized to leader last log index + 1
	lastLogIndex := len(rf.log)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex + 1
	}

	// Upon election: send initial empty AppendEntries RPCs
	// (heartbeat) to each server; repeat during idle periods to
	// prevent election timeouts
	rf.bcastAppendEntries()
}

func (rf *Raft) leaderStep(event Event)  {
	switch event.event {
	case EventLeaderHeartbeatTimeout:
		DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, resetTimer!", rf.me, rf.currState, rf.currentTerm, event.event)
		rf.resetTimer();
		rf.bcastAppendEntries()
	case EventAppendEntriesRes:
		DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, do something!", rf.me, rf.currState, rf.currentTerm, event.event)
		if event.success {
			// If successful: update nextIndex and matchIndex for follower
			if event.index > 0 {
				// this is not a heart beat res
				rf.matchIndex[event.peer] = event.index
				rf.nextIndex[event.peer] = event.index + 1
				if rf.leaderMaybeCommit() {
					// todo too many rpcs need bcast?
					// rf.bcastAppendEntries()
				}
			}
		} else {
			// If AppendEntries fails because of log inconsistency:
			// decrement nextIndex and retry
			if rf.nextIndex[event.peer] > 0 {
				rf.nextIndex[event.peer] --
				rf.sendAppendEntries2peer(event.peer)
			}
		}
	default:
		DPrintf("[raft=%-2d state=%-1d term=%-2d] got event = %d, unexpected do nothing!", rf.me, rf.currState, rf.currentTerm, event.event)
	}
}

// If RPC request or response contains term T > currentTerm:
// set currentTerm = T, convert to follower
func (rf *Raft) termChallenge(remoteTerm int)  {
	if remoteTerm > rf.currentTerm {
		DPrintf("[raft=%-2d state=%-1d term=%-2d] termChallengeFail, become(StateFollower)!", rf.me, rf.currState, rf.currentTerm)
		rf.currentTerm = remoteTerm
		rf.become(StateFollower)
	}
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {

	rf.assertLockHeld("need locked before call handleAppendEntries!")

	// check term
	rf.termChallenge(args.Term)

	// init defalt
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.Index = len(rf.log)

	// leader do not handle this msg
	if rf.currState == StateLeader {
		return
	}

	//  Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// do step before return
	defer rf.step(Event{event:EventAppendEntriesReq, term:args.Term})

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if len(rf.log) < args.PrevLogIndex {
		reply.Success = false
		return
	} else {
		if args.PrevLogIndex > 0 {
			arrIndex := args.PrevLogIndex - 1
			if rf.log[arrIndex].Term != args.PrevLogTerm {
				reply.Success = false
				return
			}
		}
	}

	// this is not a heart beat
	reply.Index = 0
	if len(args.Entries) > 0 {
		DPrintf("[raft=%-2d state=%-1d term=%-2d] log recieve count = %d, before merge cur log count = %d!", rf.me, rf.currState, rf.currentTerm, len(args.Entries), len(rf.log))

		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it
		// Append any new entries not already in the log
		for _, entry := range args.Entries {
			index := entry.Index
			if len(rf.log) >= index {
				if rf.log[index - 1].Term == entry.Term {
					// do nothing
				} else {
					// delete the existing entry and all that follow it
					rf.log = rf.log[:index - 1]
				}
			}

			if len(rf.log) < index {
				// append
				rf.log = append(rf.log, entry)
			}

			// update last index of reply
			reply.Index = entry.Index
		}

		// check local log index order
		for i := 0; i < len(rf.log); i ++ {
			if rf.log[i].Index != i + 1 {
				panic("log index order error")
			}
		}

		DPrintf("[raft=%-2d state=%-1d term=%-2d] log recieve count = %d, after merge cur log count = %d!", rf.me, rf.currState, rf.currentTerm, len(args.Entries), len(rf.log))
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		indexOfLastNewEntry :=  len(rf.log)
		commitTo := 0
		if args.LeaderCommit < indexOfLastNewEntry {
			commitTo = args.LeaderCommit
		} else {
			commitTo = indexOfLastNewEntry
		}

		if commitTo > rf.commitIndex {
			rf.commitTo(commitTo)
		} else if commitTo < rf.commitIndex {
			panic("commitTo less than rf.commitIndex !! ")
		}
	}
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply)  {
	rf.assertLockHeld("need locked before call handleRequestVote!")

	// do step before return
	defer rf.step(Event{event:EventRequestVoteReq, term:args.Term})

	// check term
	rf.termChallenge(args.Term)

	// init defalt
	reply.VoteGranted = false
	reply.Term = rf.currentTerm


	// leader candidate just reject
	if rf.currState == StateLeader || rf.currState == StateCandidate {
		reply.VoteGranted = false
		return
	}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote

	// If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		lastEntryIndex := len(rf.log)
		lastEntryTerm := 0
		if lastEntryIndex > 0 {
			lastEntryTerm = rf.log[lastEntryIndex - 1].Term
		}

		if args.LastLogTerm > lastEntryTerm {
			reply.VoteGranted = true
		}else if args.LastLogTerm == lastEntryTerm {
			if args.LastLogIndex >= lastEntryIndex {
				reply.VoteGranted = true
			}
		}
	}

	// update votedFor
	if reply.VoteGranted == true {
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) handleReply(event Event)  {

	rf.assertLockHeld("need locked before call handleReply!")
	defer func() {
		rf.termChallenge(event.term)
		rf.step(event)
	}()
}


type intSlice []int
func (p intSlice) Len() int           { return len(p) }
func (p intSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p intSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

/**
 * If there exists an N such that N > commitIndex, a majority
 * of matchIndex[i] ≥ N, and log[N].term == currentTerm:
 * set commitIndex = N
 */
func (rf *Raft) leaderMaybeCommit() bool {
	mis := make(intSlice, 0, len(rf.peers))
	for p := range rf.peers {
		mis = append(mis, rf.matchIndex[p])
	}
	sort.Sort(sort.Reverse(mis))
	mci := mis[rf.q()-1]

	// commit
	if mci > rf.commitIndex {
		rf.commitTo(mci)
		return true
	}

	return false
}

func (rf *Raft) commitTo(to int) {
	DPrintf("[raft=%-2d state=%-1d term=%-2d] log commit, commmit index = %d!", rf.me, rf.currState, rf.currentTerm, to)
	rf.commitIndex = to
	rf.applyToCommmit()
}

/**
 *	If commitIndex > lastApplied: increment lastApplied, apply
 *	log[lastApplied] to state machine
 */
func (rf *Raft) applyToCommmit() {

	// check
	if rf.lastApplied > rf.commitIndex {
		panic("lastApplied > commitIndex")
	}

	for tmp := rf.lastApplied + 1; tmp <= rf.commitIndex; tmp ++ {
		// do apply
		rf.lastApplied = tmp

		// apply msg to client
		arrIndex := rf.lastApplied - 1
		applyMsg := ApplyMsg{true, rf.log[arrIndex].Command, rf.log[arrIndex].Index}
		DPrintf("[raft=%-2d state=%-1d term=%-2d] log apply, apply index = %d, applyMsg = %v!", rf.me, rf.currState, rf.currentTerm, rf.lastApplied, applyMsg)

		rf.applyCh <- applyMsg
	}
}