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

	// Track the number of running routines
	runningRoutines int32
	runningRoutinesLock sync.RWMutex

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

func (rs *raftState) setState(s RaftState) {
	rs.stateLock.Lock()
	rs.state = s
	rs.stateLock.Unlock()
}

func (rs *raftState) getState() RaftState {
	rs.stateLock.Lock()
	state := rs.state
	rs.stateLock.Unlock()
	return state
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
	currentTerm uint64

	// Save the last time this Raft was contacted by the leader
	lastContact time.Time
	lastContactLock sync.RWMutex

	failures uint64
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

type leaderState struct {
	replState map[string]*followerReplication
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

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
	// TODO add lastLoginIndext and lastLogTerm
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


// TODO finish implementing
type AppendEntriesArgs struct {
	Term uint64
	Leader string
}

// TODO finish implementing
type AppendEntriesReply struct {
	Term uint64
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.getCurrentTerm()
	reply.Success = false

	// Reject entries from stale servers
	if args.Term < rf.getCurrentTerm() {
		return
	}

	// If the term in the arguments is bigger or a leader has been already
	// established - revert to Follower status
	if args.Term > rf.getCurrentTerm() || rf.getState() != Follower {
		rf.setState(Follower)
		rf.setCurrentTerm(args.Term)
		reply.Term = args.Term
	}

	// Update the leader
	rf.setLeader(args.Leader)

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
	isLeader := true


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
		// TODO deal with the apply channel redirections
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
		askPeerToVote(i)
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

	defer func() {
		rf.setLastContact()

		// Clear state
		rf.leaderState.replState = nil

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

// TODO finish implementing
func (rf *Raft) startReplication(peer int) {
	s := &followerReplication{
		peer:		strconv.Itoa(peer),
		currentTerm: 	rf.getCurrentTerm(),
		lastContact:	time.Now(),
	}

	rf.leaderState.replState[strconv.Itoa(peer)] = s
	rf.goFunc(func() { rf.replicate(s) })
}

// TODO finish implementing
func (rf *Raft) replicate(s *followerReplication) {
	// Starting async heartbeating
	rf.goFunc(func(){rf.heartbeat(s)})
}

func (rf *Raft) heartbeat(s *followerReplication) {
	//var failures uint64
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
			//failures++
			//<-time.After(backoff(failureWait, failures, maxFailureScale))
		} else {
			s.setLastContact()
			//printOut("[INFO] " + s.peer + " received heartbeat")
			//failures = 0
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	rf.setState(Follower)
	rf.setCurrentTerm(0)
	rf.goFunc(rf.run)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
