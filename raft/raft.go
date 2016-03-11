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

import "sync"
import (
	"github.com/AlexShtarbev/mit_ds/labrpc"
	"time"
	"strconv"
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Captures the state of the Raft peer: leader, follower or candidate
type RaftState uint32

const (
	// Every peer starts as a Follower
	Follower RaftState = iota

	Candidate

	Leader
)

const(
	HeartbeatTimeout  = 1000 * time.Millisecond
	ElectionTimeout = 1000 * time.Millisecond
	LeaderLeaseTimeout = 500 *time.Millisecond
	minCheckInterval = 10 * time.Millisecond
	CommitTimeout = 50 * time.Millisecond
)

const (
	MaxAppendEntries = 64
)

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

// ------------ RAFT STATE ------------
type raftState struct {
	// The current Term
	currentTerm uint64
	currentTermLock sync.RWMutex

	lastLogIndex uint64
	lastLogIndexLock sync.RWMutex

	lastLogTerm uint64
	lastLogTermLock sync.RWMutex

	commitIndex uint64
	commitIndexLock sync.RWMutex

	lastApplied uint64
	lastAppliedLock sync.RWMutex

	// Track the number of running routines
	runningRoutines int32
	runningRoutinesLock sync.RWMutex

	lastVoteTerm uint64
	lastVoteTermLock sync.RWMutex

	lastVoteCandidate string
	lastVoteCandidateLock sync.RWMutex

	// The current state of the Raft peer
	state RaftState
	stateLock sync.RWMutex
}

func (rs *raftState) setCurrentTerm(t uint64) {
	rs.currentTermLock.Lock()
	rs.currentTerm = t
	rs.currentTermLock.Unlock()
}

func (rs *raftState) getCurrentTerm() uint64 {
	rs.currentTermLock.RLock()
	currentTerm := rs.currentTerm
	rs.currentTermLock.RUnlock()
	return currentTerm
}

func (rs *raftState) setLastLogIndex(t uint64) {
	rs.lastLogIndexLock.Lock()
	rs.lastLogIndex = t
	rs.lastLogIndexLock.Unlock()
}

// TODO implement getLastIndex when adding snapshotting
func (rs *raftState) getLastLogIndex() uint64 {
	rs.lastLogIndexLock.RLock()
	t := rs.lastLogIndex
	rs.lastLogIndexLock.RUnlock()
	return t
}

func (rs *raftState) setLastLogTerm(t uint64) {
	rs.lastLogTermLock.Lock()
	rs.lastLogTerm = t
	rs.lastLogTermLock.Unlock()
}

func (rs *raftState) getLastLogTerm() uint64 {
	rs.lastLogTermLock.RLock()
	t := rs.lastLogTerm
	rs.lastLogTermLock.RUnlock()
	return t
}

func (rs *raftState) setCommitIndex(t uint64) {
	rs.commitIndexLock.Lock()
	rs.commitIndex = t
	rs.commitIndexLock.Unlock()
}

func (rs *raftState) getCommitIndex() uint64 {
	rs.commitIndexLock.RLock()
	t := rs.commitIndex
	rs.commitIndexLock.RUnlock()
	return t
}

func (rs *raftState) setLastApplied(t uint64) {
	rs.lastAppliedLock.Lock()
	rs.lastApplied = t
	rs.lastAppliedLock.Unlock()
}

func (rs *raftState) getLastApplied() uint64 {
	rs.lastAppliedLock.RLock()
	t := rs.lastApplied
	rs.lastAppliedLock.RUnlock()
	return t
}

func (rs *raftState) setState(s RaftState) {
	rs.stateLock.Lock()
	rs.state = s
	rs.stateLock.Unlock()
}

func (rs *raftState) getState() RaftState {
	rs.stateLock.RLock()
	state := rs.state
	rs.stateLock.RUnlock()
	return state
}

func (rs *raftState) setLastVoteTerm(t uint64) {
	rs.lastVoteTermLock.Lock()
	rs.lastVoteTerm = t
	rs.lastVoteTermLock.Unlock()
}

func (rs *raftState) getLastVoteTerm() uint64 {
	rs.lastVoteTermLock.RLock()
	t := rs.lastVoteTerm
	rs.lastVoteTermLock.RUnlock()

	return t
}

func (rs *raftState) setLastVoteCandidate(t string) {
	rs.lastVoteCandidateLock.Lock()
	rs.lastVoteCandidate = t
	rs.lastVoteCandidateLock.Unlock()
}

func (rs *raftState) getLastVoteCandidate() string {
	rs.lastVoteCandidateLock.RLock()
	t := rs.lastVoteCandidate
	rs.lastVoteCandidateLock.RUnlock()

	return t
}

func (rs *raftState) addRoutine() {
	rs.runningRoutinesLock.Lock()
	rs.runningRoutines++
	rs.runningRoutinesLock.Unlock()
}

func (rs *raftState) removeRoutine() {
	rs.runningRoutinesLock.Lock()
	rs.runningRoutines--
	rs.runningRoutinesLock.Unlock()
}

func (rs *raftState) getRoutines() int32 {
	rs.runningRoutinesLock.Lock()
	routines := rs.runningRoutines
	rs.runningRoutinesLock.Unlock()
	return routines
}

// NOTE: here we start a groutine and handle race between a
// routing starting and incrementing and a routing that is
// stopping, hence decrementing
func (rs *raftState) goFunc(f func()) {
	rs.addRoutine()
	go func() {
		rs.removeRoutine()
		f()
	}()
}
// ------------ END RAFT STATE ------------

type followerReplication struct {
	peer string
	inflight  *inflight

	triggerCh chan struct{}

	currentTerm uint64
	matchIndex uint64
	nextIndex uint64

	// Save the last time this Raft was contacted by the leader
	lastContact time.Time
	lastContactLock sync.RWMutex

	failures uint64

	stepDown chan struct{}
}

func (fr *followerReplication) setLastContact() {
	fr.lastContactLock.Lock()
	fr.lastContact = time.Now()
	fr.lastContactLock.Unlock()
}

func (fr *followerReplication) getLastContact() time.Time {
	fr.lastContactLock.Lock()
	lastContact := fr.lastContact
	fr.lastContactLock.Unlock()
	return lastContact
}

// Only used while we are the leader
type leaderState struct {
	applyChan chan *logFuture
	commitCh chan struct{}
	inflight  *inflight
	replState map[string]*followerReplication
	stepDown chan struct{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	applyChan chan ApplyMsg

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	raftState

	// Save the last time this Raft was contacted by the leader
	lastContact time.Time
	lastContactLock sync.RWMutex

	// The current leader
	leader string
	leaderLock sync.RWMutex

	// leaderState is used only when the Raft has been elected as a leader
	leaderState leaderState

	store *LogStore
}

func (rf *Raft) setLastContact() {
	rf.lastContactLock.Lock()
	rf.lastContact = time.Now()
	rf.lastContactLock.Unlock()
}

func (rf *Raft) LastContact() time.Time {
	rf.lastContactLock.RLock()
	lastContact := rf.lastContact
	rf.lastContactLock.RUnlock()
	return lastContact
}

func (rf *Raft) Leader() string {
	rf.leaderLock.RLock()
	leader := rf.leader
	rf.leaderLock.RUnlock()
	return leader
}

func (rf *Raft) setLeader(leader string) {
	rf.leaderLock.Lock()
	rf.leader = leader
	rf.leaderLock.Unlock()
}

func (rf *Raft) setState(state RaftState) {
	// Since the state is changing, a new leader is most
	// likely being elected
	rf.setLeader("")
	rf.raftState.setState(state)
}

func (rf *Raft) state() RaftState {
	return rf.getState()
}

func (rf *Raft) setCurrentTerm(t uint64) {
	rf.raftState.setCurrentTerm(t)
}

func (rf *Raft) getCurrentTerm() uint64 {
	return rf.raftState.getCurrentTerm()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here.
	term = int(rf.getCurrentTerm())
	isleader = (rf.state() == Leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term uint64
	Candidate string

	LastLogIndex uint64
	LastLogTerm uint64
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.

	// If the leader is out of date, it has to update its term
	Term uint64

	// True or false
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
// TODO update when implementing persistent store
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.getCurrentTerm()
	reply.VoteGranted = false

	// Check to make sure that there is not an existing leader,
	// who is not the current candidate
	candidate := args.Candidate
	if leader := rf.Leader(); leader != "" && leader != candidate {
		printOut("[WARN] Rejecting vote from " + strconv.Itoa(rf.me) + "we already have " + leader + " as leader")
		return
	}

	// If the Raft's current term is bigger than the Raft, who sent the vote,
	// ignore the vote request
	if(rf.getCurrentTerm() > args.Term) {
		return
	}

	// If the Raft is behind on terms, then revert it to Follower and update
	// its term
	if(args.Term > rf.getCurrentTerm()) {
		rf.setState(Follower)
		rf.setCurrentTerm(args.Term)
		reply.Term = args.Term
	}

	lastVoteTerm := rf.getLastVoteTerm()
	lastVoteCandidate := rf.getLastVoteCandidate()

	if lastVoteTerm == args.Term && lastVoteCandidate != "" {
		printOut("[WARN] " + strconv.Itoa(rf.me) + ": Raft: already voted in term " + strconv.Itoa(int(lastVoteTerm)))
		if(lastVoteCandidate == args.Candidate) {
			printOut("[WARN] " + strconv.Itoa(rf.me) + ": Raft: already voted for" + lastVoteCandidate)
		}
		return
	}

	lastLogIdx := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	// Check for a stale term
	if lastLogTerm > args.LastLogTerm {
		printOut("[WARN] " + strconv.Itoa(rf.me) + ": Raft: Rejecting request from " + args.Candidate + " since our last log term is greater")
		return
	}

	if lastLogIdx > args.LastLogIndex {
		printOut("[WARN] " + strconv.Itoa(rf.me) + ": Raft: Rejecting request from " + args.Candidate + " since our last log index is greater")
		return
	}

	// Save our last vote
	rf.setLastVoteCandidate(args.Candidate)
	rf.setLastVoteTerm(args.Term)

	reply.VoteGranted = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should probably
// pass &reply.
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term uint64
	Leader string

	// Provide the previous entries for integrity checking
	PrevLogEntry uint64
	PrevLogTerm  uint64

	// New entries to commit
	Entries []*Log

	// Commit index on the leader
	LeaderCommitIndex uint64
}

type AppendEntriesReply struct {
	Term uint64

	// Last Log is a hint to help accelerate rebuilding slow nodes
	LastLog uint64

	// We may not succeed if we have a conflicting entry
	Success bool

	// There are scenarios where this request didn't succeed
	// but there's no need to wait/back-off the next attempt.
	NoRetryBackoff bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if(len(args.Entries) > 0) {
		printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: got AppendEntries RPC from " + args.Leader)
	}

	reply.Term = rf.getCurrentTerm()
	reply.Success = false

	// Reject entries from stale servers
	if args.Term < rf.getCurrentTerm() {
		printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries RPC fom " + args.Leader + ": term is stale ")
		printOut("[cont'd] args.Term = " + strconv.Itoa(int(args.Term)) + ": Raft: rf.getCurrentTerm() = " + strconv.Itoa(int(rf.getCurrentTerm())))
		return
	}

	// If the term in the arguments is bigger or a leader has been already
	// established - revert to Follower status
	if args.Term > rf.getCurrentTerm() || rf.getState() != Follower {
		printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries RPC fom " + args.Leader + ": term in the arguments is bigger or a leader has been alread established ")
		rf.setState(Follower)
		rf.setCurrentTerm(args.Term)
		reply.Term = args.Term
	}

	// Update the leader
	rf.setLeader(args.Leader)

	// Consistency check
	if args.PrevLogEntry > 0 {
		lastIdx := rf.getLastLogIndex()
		lastTerm := rf.getLastLogTerm()

		var prevLogTerm uint64
		if args.PrevLogEntry == lastIdx {
			prevLogTerm = lastTerm
		} else {
			var prevLog Log
			if ok := rf.store.GetLog(args.PrevLogEntry, &prevLog); !ok {
				printOut("[ERR] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries: Failed to get prevoius log = " + strconv.FormatUint(args.PrevLogEntry, 10))

				reply.NoRetryBackoff = true
				return
			}
			prevLogTerm = prevLogTerm
		}

		if args.PrevLogTerm != prevLogTerm {
			printOut("[ERR] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries: Previous log term mis-match: ours: " + strconv.FormatUint(prevLogTerm, 10) + " remote: " + strconv.FormatUint(args.PrevLogTerm, 10))
			reply.NoRetryBackoff = true
			return
		}
	}
	//printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: passed consistency check");

	if n := len(args.Entries); n > 0 {
		first := args.Entries[0]
		last := args.Entries[n - 1]

		// Check if there are logs that need to be truncated
		lastLogIndex := rf.getLastLogIndex()
		if first.Index <= lastLogIndex {
			printOut("[WARN] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries: removing unncessary logs: from" + strconv.FormatUint(first.Index, 10) + " to " + strconv.FormatUint(lastLogIndex, 10))
			if err := rf.store.DeleteRange(first.Index, lastLogIndex); err != nil {
				printOut("[WARN] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries: could not delete range of logs")
				return
			}
		}

		// Append the new logs
		if err := rf.store.StoreLogs(args.Entries); err != nil {
			printOut("[WARN] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries: could not store logs")
			return
		} else {
			printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries: logs stored")
		}

		// Update the last log index and term
		rf.setLastLogIndex(last.Index)
		rf.setLastLogTerm(last.Term)
	}

	if args.LeaderCommitIndex > 0 && args.LeaderCommitIndex > rf.getCommitIndex() {
		idx := min(args.LeaderCommitIndex, rf.getLastLogIndex())
		rf.setCommitIndex(idx)

		printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries: processLogs() called")
		rf.processLogs(idx, nil)
	}

	reply.Success = true

	rf.setLastContact()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	isLeader := (rf.getState() == Leader)
	//printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: New Log")

	// If the Raft server is not a leader, then return immediately
	if !isLeader {
		return index, term, isLeader
	}

	//printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: New Log")

	// Create a new log entry
	log := Log {
		Index: rf.getLastLogIndex() + 1,
		Term: rf.getCurrentTerm(),
		Command: command,
	}

	// Wrap it in a logFuture object
	future := logFuture{
		log: log,
	}

	// Send it to the leader's apply channel
	rf.leaderState.applyChan <- &future

	index = int(log.Index)
	term = int(log.Term)

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

func (rf *Raft) run() {
	for {
		switch rf.getState() {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		}

	}
}

func (rf *Raft) runFollower() {
	heartbeatTimer := randomTimeout(HeartbeatTimeout)
	for {
		select {
		case <-heartbeatTimer:
			// restart the timer
			heartbeatTimer = randomTimeout(HeartbeatTimeout)

			// Check if we have been contacted by the leader in
			// the heartbeat interval
			lastContact :=rf.LastContact()
			if time.Now().Sub(lastContact) < HeartbeatTimeout {
				continue
			}

			// No contact was made with the leader
			rf.setLeader("")
			rf.setState(Candidate)
			printOut(strconv.Itoa(rf.me) + " --> is now Candidate")
			return

		case <-rf.leaderState.applyChan:
		}
	}
}

// TODO not finished for replication
func (rf *Raft) electSelf() chan *RequestVoteReply {
	// Create a response channel where each peer will submit its vote and reply
	replyChan := make(chan *RequestVoteReply, len(rf.peers) + 1)

	// Increment the Candidate Raft's current term
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)

	// Construct the request arguments
	reqArgs := RequestVoteArgs{
		Term:	rf.getCurrentTerm(),
		Candidate: strconv.Itoa(rf.me),
	}

	askPeerToVote := func(peer int) {
		rf.goFunc(func() {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(peer, reqArgs, &reply)
			if !ok {
				printOut("[ERR] " + strconv.Itoa(rf.me) + " could not receive vote from peer = " + strconv.Itoa(peer))
			}

			replyChan <- &reply
		})
	}

	// Send RequestVote RPC's to all of the peers
	for i := 0 ; i < len(rf.peers); i++ {
		if(i != rf.me) {
			askPeerToVote(i)
		}
	}

	// Vote for our self
	replyChan <- &RequestVoteReply{
		Term:		reqArgs.Term,
		VoteGranted:	true,
	}

	return replyChan
}

func (rf *Raft) majoritySize() int {
	return (len(rf.peers)/2) + 1
}

// TODO finish implementing
func (rf *Raft) runCandidate() {
	// Start voting
	voteChan :=  rf.electSelf()
	// Set the election timeout
	electionTimer := randomTimeout(ElectionTimeout)

	// Count the votes
	votesGranted := 0
	majority := rf.majoritySize()

	for rf.state() == Candidate {
		// TODO deal with the apply channel redirection
		select {
		case vote :=<- voteChan:
			// Check if the term in the reply is bigger
			// than the Raft's. If so - revert ot Follower.
			if(vote.Term > rf.getCurrentTerm()) {
				printOut("[ERR] " + strconv.Itoa(rf.me) + ": Raft term stale. Revert to follower.")
				rf.setState(Follower)
				rf.setCurrentTerm(vote.Term)
				return
			}

			if vote.VoteGranted {
				votesGranted++
			}

			if votesGranted >= majority {
				rf.setState(Leader)
				rf.setLeader(strconv.Itoa(rf.me))
				printOut("[INFO] " + strconv.Itoa(rf.me) + ": is now the leader.")
			}

		case <-electionTimer:
			// NOTE: election failed miserably. We return, which will
			// move us back into the runCandidate function, restarting the
			// election procedure.
			printOut("[WARN] " + strconv.Itoa(rf.me) + ": Election timeout reached: restarting election.")
			return
		}
	}
}

// TODO finish implementing
func (rf *Raft) runLeader() {
	rf.leaderState.replState = make(map[string]*followerReplication)
	rf.leaderState.commitCh = make(chan struct{}, 1)
	rf.leaderState.applyChan = make(chan *logFuture, 1)
	rf.leaderState.inflight = newInflight(rf.leaderState.commitCh)
	// TODO implement stepDown?

	defer func() {
		rf.setLastContact()

		rf.leaderState.inflight.Cancel()

		// Clear state
		rf.leaderState.replState = nil
		rf.leaderState.commitCh = nil
		rf.leaderState.inflight = nil
		rf.leaderState.applyChan = nil

		// If we are stepping down for some reason the leader is unknown.
		// We may have stepped down due to an RPC call, which would
		// provide the leader, so we cannot always blank this out.
		rf.leaderLock.Lock()
		if(rf.leader == strconv.Itoa(rf.me)) {
			rf.leader = ""
		}
		rf.leaderLock.Unlock()

	}()

	for i, _ := range rf.peers {
		if(i != rf.me) {
			rf.startReplication(i)
		}
	}

	rf.leaderLoop()
}

func (rf *Raft) leaderLoop() {
	lease := time.After(LeaderLeaseTimeout)
	for rf.getState() == Leader {
		select {
		//case <-rf.leaderState.stepDown:
		//	rf.setState(Follower)
		case <- rf.leaderState.commitCh:
			printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: New Commit")
			committed := rf.leaderState.inflight.Committed()
			for e := committed.Front(); e != nil; e = e.Next() {
				commitLog := e.Value.(*logFuture)
				idx := commitLog.log.Index
				rf.setCommitIndex(idx)
				rf.processLogs(idx, commitLog)
			}

		case newLog := <- rf.leaderState.applyChan:
			printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: New Log")
			ready := []*logFuture{newLog}
			for i:= 0; i < MaxAppendEntries; i++ {
				//printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: Waiting for more appendentires. " + strconv.Itoa(i))
				select{
				case newLog := <- rf.leaderState.applyChan:
					ready = append(ready, newLog)
				default:
					break
				}
			}

			n := len(ready)
			//printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: Len of AppendEntires. " + strconv.Itoa(n))
			if n == 0 {
				continue
			}

			ready = ready[:n]
			rf.dispatchLogs(ready)

		case <- lease:
			// NOTE: check if we exceeded the lease. Potentially stepping down
			maxDiff := rf.checkLeaderLease()

			checkInterval := LeaderLeaseTimeout - maxDiff
			if(checkInterval < minCheckInterval) {
				checkInterval = minCheckInterval
			}
			printOut("[INFO] " + strconv.Itoa(rf.me) +": has check interval = " +checkInterval.String())
			lease = time.After(checkInterval)
		}
	}
}

func (rf *Raft) processLogs(index uint64, future *logFuture) {
	lastApplied := rf.getLastApplied()
	if index <= lastApplied {
		printOut("[WARN] processLogs():" + strconv.Itoa(rf.me) +": index <= lastApplied")
		return
	}

	printOut("[INFO] processLogs():" + strconv.Itoa(rf.me) +": iterating logs ");
	for i := lastApplied + 1; i <=index; i++ {
		if future != nil && future.log.Index == i {
			printOut("[INFO] processLogs():" + strconv.Itoa(rf.me) +": log being processed")
			rf.processLog(&future.log, future)
		} else {
			l := new(Log)
			if ok := rf.store.GetLog(i, l); ok {
				printOut("[INFO] processLogs():" + strconv.Itoa(rf.me) +": log being processed")
				rf.processLog(l, nil)
			} else {
				printOut("[INFO] processLogs():" + strconv.Itoa(rf.me) +": no such log")
			}
		}

		rf.setLastApplied(i)
	}
}

func (rf *Raft) processLog(l *Log, future *logFuture) {
	var log Log
	if future != nil {
		log = future.log
	} else {
		log = *l
	}

	am := ApplyMsg{
		Index: int(log.Index),
		Command: log.Command,
	}

	printOut("[INFO] " + strconv.Itoa(rf.me) +": has processed log " )
	rf.applyChan <- am
}

func (rf *Raft) dispatchLogs(applyLogs []*logFuture) {
	term := rf.getCurrentTerm()
	lastIndex := rf.getLastLogIndex()
	logs := make([]*Log, len(applyLogs))

	for i, applyLog := range applyLogs {
		applyLog.dispatch = time.Now()
		applyLog.log.Index = lastIndex + uint64(i) + 1
		applyLog.log.Term = term
		applyLog.policy = newMajorityQuorum(len(rf.peers) + 1)
		logs[i] = &applyLog.log
	}

	if err := rf.store.StoreLogs(logs); err != nil {
		printOut("[ERR] " + strconv.Itoa(rf.me) +": failed to store logs")
		rf.setState(Follower)
		return
	}

	printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: StartAll()")
	rf.leaderState.inflight.StartAll(applyLogs)

	rf.setLastLogIndex(lastIndex + uint64(len(applyLogs)))
	rf.setLastLogTerm(term)

	for _, f := range rf.leaderState.replState {
		asyncNotifyCh(f.triggerCh)
	}

}

// Here we check to make sure that we can contact a majority of peers
// within the leader lease interval. If we have not, then we step down
// as a Leader, because we might have lost connectivity.
// Return: the maximum amount of time that a peer has not received contact
func (rf *Raft) checkLeaderLease() time.Duration{
	contacted := 1

	// Check for each follower if contact has been made during the lease
	now := time.Now()
	var maxDiff time.Duration
	for _, f:= range rf.leaderState.replState {
		diff := now.Sub(f.getLastContact())
		if(diff <= LeaderLeaseTimeout) {
			contacted++
			if diff > maxDiff {
				maxDiff = diff
			}
		}
	}

	// NOTE: check if we contacted a majority of peers
	majority := rf.majoritySize()
	if contacted < majority {
		printOut("[WARN] " + strconv.Itoa(rf.me) + ": failed to contact majority of peers. Stepping down")
		rf.setState(Follower)
	}
	// else - we are still the leader
	printOut("[INFO] " + strconv.Itoa(rf.me) + ": got majority")
	return maxDiff
}

func (rf *Raft) startReplication(peer int) {
	lastIdx :=rf.getLastLogIndex()
	s := &followerReplication{
		peer:		strconv.Itoa(peer),
		inflight:	rf.leaderState.inflight,
		currentTerm: 	rf.getCurrentTerm(),
		lastContact:	time.Now(),
		stepDown:	rf.leaderState.stepDown,
		nextIndex:	lastIdx + 1,
		matchIndex:	0,
		triggerCh:	make(chan struct{}),
	}

	rf.leaderState.replState[strconv.Itoa(peer)] = s
	rf.goFunc(func() { rf.replicate(s) })
	//asyncNotifyCh(s.triggerCh)
}

func (rf *Raft) replicate(s *followerReplication) {
	// Starting async heartbeating
	rf.goFunc(func(){rf.heartbeat(s)})

	shouldStop := false
	for !shouldStop {
		select {
		case <-s.triggerCh:
			shouldStop = rf.replicateTo(s, rf.getLastLogIndex())
		case <-randomTimeout(CommitTimeout):
			shouldStop = rf.replicateTo(s, rf.getLastLogIndex())
		}
		//printOut("[INFO] " + strconv.Itoa(rf.me) + ": shouldStop = " + strconv.FormatBool(shouldStop))
	}

	printOut("[INFO] " + strconv.Itoa(rf.me) + ": shouldStop = " + strconv.FormatBool(shouldStop))
}

// TODO snapshotting
func (rf *Raft) replicateTo(s *followerReplication, lastIndex uint64) bool {
	var args AppendEntriesArgs
	var reply AppendEntriesReply

START:
	if s.failures > 0 {
		<-time.After(backoff(failureWait, s.failures, maxFailureScale))
	}

	if ok := rf.setupAppendEntries(s, &args, s.nextIndex, lastIndex); !ok {
		printOut("[INFO] " + strconv.Itoa(rf.me) + ": could not setup AppendEntries")
		return true
	}

	//printOut("[INFO] " + strconv.Itoa(rf.me) + ": finished setting up Appendentries for " + s.peer)
	peer, _ := strconv.Atoi(s.peer)

	//printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: Waiting for AppendEntries")
	if ok := rf.sendAppendEntries(peer, args, &reply); !ok {
		printOut("[ERR] " + strconv.Itoa(rf.me) + " Raft: Failed to send AppendEntries to peer = " + s.peer)
		s.failures++
		return true
	}
	//printOut("[INFO] " + strconv.Itoa(rf.me) + ": received AppendentriesReply from " + s.peer)

	// Stop replication if the Raft has a stale term
	if args.Term > reply.Term {
		printOut("[ERR] " + strconv.Itoa(rf.me) + " Raft: Term in AppendEntriesArgs is stale. Revert to follower")
		rf.setState(Follower)
		return true
	}

	// Update the last contact time
	s.setLastContact()

	if reply.Success {
		//printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries returned true")
		rf.updateLastAppend(s, &args)

		s.failures = 0
	} else {
		//printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: AppendEntries returned false")
		s.nextIndex = max(min(s.nextIndex-1, reply.LastLog+1), 1)
		s.matchIndex = s.nextIndex - 1
		if reply.NoRetryBackoff {
			s.failures = 0
		} else {
			s.failures++
		}

		printOut("[ERR] " + strconv.Itoa(rf.me) + " Raft: AppendEntries to " +s.peer + " rejected. Sending older logs")

	}
//CHECK_MORE:

	if s.nextIndex <=lastIndex {
		//printOut("[INFO] " + strconv.Itoa(rf.me) + ": s.nextIndex <=lastIndex ")
		goto START
	}

	return false
}

func (rf *Raft) setupAppendEntries(s *followerReplication, args *AppendEntriesArgs, nextIndex, lastIndex uint64) bool {
	args.Term = s.currentTerm
	args.Leader = rf.Leader()
	args.LeaderCommitIndex = rf.getCommitIndex()

	if ok := rf.setPreviousLog(args, nextIndex); !ok {
		return false
	}

	if ok := rf.setNewLogs(args, nextIndex, lastIndex); !ok {
		return false
	}

	return true
}

func (rf *Raft) setPreviousLog(args *AppendEntriesArgs, nextIndex uint64) bool {
	if nextIndex == 1 {
		args.PrevLogEntry = 0
		args.PrevLogTerm = 0
	} else {
		var l Log
		if ok := rf.store.GetLog(nextIndex - 1, &l); !ok {
			printOut("[ERR] " + strconv.Itoa(rf.me) + ": Raft: Failed to ge log at index = " + strconv.FormatUint(nextIndex - 1, 10))
			return false
		}

		args.PrevLogEntry = l.Index
		args.PrevLogTerm = l.Term
	}

	//printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: PrevLogTerm = " + strconv.FormatUint(args.PrevLogTerm, 10) + " | PrevLogIndex = " + strconv.FormatUint(args.PrevLogEntry, 10))

	return true
}

func (rf *Raft) setNewLogs(args *AppendEntriesArgs, nextIndex, lastIndex uint64) bool {
	args.Entries = make([]*Log, 0)
	maxIndex := min(nextIndex + uint64(MaxAppendEntries - 1), lastIndex)
	//printOut("[INFO] " + strconv.Itoa(rf.me) + ": Raft: nextIndex =" + strconv.FormatUint(nextIndex, 10) + " | maxIndex = " + strconv.FormatUint(maxIndex, 10))
	for i := nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if ok := rf.store.GetLog(i, oldLog); !ok {
			printOut("[ERR] " + strconv.Itoa(rf.me) + ": Raft: Failed to ge log at index = " + strconv.FormatUint(i, 10))
			return false
		}
		args.Entries = append(args.Entries, oldLog)
	}

	return true
}

func (rf *Raft) updateLastAppend(s *followerReplication, args *AppendEntriesArgs) {
	if logs := args.Entries; len(logs) > 0 {
		first := logs[0]
		last := logs[len(logs) - 1]
		s.inflight.CommitRange(first.Index, last.Index)

		s.matchIndex = last.Index
		s.nextIndex = last.Index + 1
	}
}

func (rf *Raft) heartbeat(s *followerReplication) {
	var failures uint64
	args := AppendEntriesArgs{
		Term: 	rf.getCurrentTerm(),
		Leader: rf.Leader(),
	}
	var reply AppendEntriesReply
	for {
		// Wait for the next heartbeat interval
		select {
		case <-randomTimeout(HeartbeatTimeout/10):
		}

		peer,_ := strconv.Atoi(s.peer)
		if ok:= rf.sendAppendEntries(peer, args, &reply); !ok {
			printOut("[ERR] " + strconv.Itoa(rf.me) + "failed to send heartbet to " + s.peer)
			failures++
			<-time.After(backoff(failureWait, failures, maxFailureScale))
		} else {
			s.setLastContact()
			//printOut("[INFO] " + s.peer + " received heartbeat")
			failures = 0
		}
	}
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.persister = persister
	rf.me = me
	//peers = append(peers[:me], peers[me+1:]...)
	rf.peers = peers

	// Your initialization code here.

	rf.setState(Follower)
	rf.setCurrentTerm(0)
	rf.store = NewLogStore()
	rf.goFunc(rf.run)
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
