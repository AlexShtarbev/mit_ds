package raft
//
////
//// this is an outline of the API that raft must expose to
//// the service (or tester). see comments below for
//// each of these functions for more details.
////
//// rf = Make(...)
////   create a new Raft server.
//// rf.Start(command interface{}) (index, term, isleader)
////   start agreement on a new log entry
//// rf.GetState() (term, isLeader)
////   ask a Raft for its current term, and whether it thinks it is leader
//// ApplyMsg
////   each time a new entry is committed to the log, each Raft peer
////   should send an ApplyMsg to the service (or tester)
////   in the same server.
////
//
//import "sync"
//import (
//	"github.com/AlexShtarbev/mit_ds/labrpc"
//	"strconv"
//	"time"
//)
//
//// import "bytes"
//// import "encoding/gob"
//
//type Log struct {
//	Term int
//	Command interface{}
//}
//
//var testOut = false
//
//const (
//	Leader = "leader"
//	Follower = "follower"
//	Candidate = "candidate"
//)
//
////
//// as each Raft peer becomes aware that successive log entries are
//// committed, the peer should send an ApplyMsg to the service (or
//// tester) on the same server, via the applyCh passed to Make().
////
//type ApplyMsg struct {
//	Index       int
//	Command     interface{}
//	UseSnapshot bool   // ignore for lab2; only used in lab3
//	Snapshot    []byte // ignore for lab2; only used in lab3
//}
//
////
//// A Go object implementing a single Raft peer.
////
//type Raft struct {
//	mu        sync.Mutex
//	peers     []*labrpc.ClientEnd
//	persister *Persister
//	me        int // index into peers[]
//
//	// Your data here.
//	// Look at the paper's Figure 2 for a description of what
//	// state a Raft server must maintain.
//
//	currentTerm int
//	votedFor int
//	votedForTerm int
//	votesChan chan int
//
//	log []Log
//	commitIndex int
//	lastApplied int
//
//	status string
//
//	timer Timer
//}
//
//// return currentTerm and whether this server
//// believes it is the leader.
//func (rf *Raft) GetState() (int, bool) {
//
//	var term int
//	var isleader bool
//	// Your code here.
//	term = rf.currentTerm
//	isleader = (rf.status == Leader)
//	return term, isleader
//}
//
////
//// save Raft's persistent state to stable storage,
//// where it can later be retrieved after a crash and restart.
//// see paper's Figure 2 for a description of what should be persistent.
////
//func (rf *Raft) persist() {
//	// Your code here.
//	// Example:
//	// w := new(bytes.Buffer)
//	// e := gob.NewEncoder(w)
//	// e.Encode(rf.xxx)
//	// e.Encode(rf.yyy)
//	// data := w.Bytes()
//	// rf.persister.SaveRaftState(data)
//}
//
////
//// restore previously persisted state.
////
//func (rf *Raft) readPersist(data []byte) {
//	// Your code here.
//	// Example:
//	// r := bytes.NewBuffer(data)
//	// d := gob.NewDecoder(r)
//	// d.Decode(&rf.xxx)
//	// d.Decode(&rf.yyy)
//}
//
//
//
//
////
//// example RequestVote RPC arguments structure.
////
//type RequestVoteArgs struct {
//	Term int
//	CandidateId int
//	LastLogIndex int
//	LastLogTerm int
//}
//
////
//// example RequestVote RPC reply structure.
////
//type RequestVoteReply struct {
//	Term int
//	VoteGranted bool
//}
//
////
//// example RequestVote RPC handler.
////
//func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
//
//	if(rf.votedForTerm < args.Term) {
//		rf.votedFor = -1
//	}
//
//	//end of 5.4.1 explains "at least as up to date" voting rule
//	//to be implemented in RequestVote RPC handler
//	//compare last entry
//	//higher term wins
//	//if equal terms, longer log wins
//
//	if(rf.currentTerm < args.Term ) {
//		if(rf.votedFor == -1/*|| rf.log[rf.commitIndex].Term < args.LastLogTerm || rf.commitIndex < args.LastLogIndex*/) {
//			rf.votedFor = args.CandidateId
//			rf.votedForTerm = args.Term
//			reply.VoteGranted = true
//			printOut(strconv.Itoa(rf.me) + " votes true")
//		}
//	} else if rf.currentTerm > args.Term{
//		reply.Term = rf.currentTerm
//		reply.VoteGranted = false
//		printOut(strconv.Itoa(rf.me) + " votes false")
//	}
//}
//
////
//// example code to send a RequestVote RPC to a server.
//// server is the index of the target server in rf.peers[].
//// expects RPC arguments in args.
//// fills in *reply with RPC reply, so caller should probably
//// pass &reply.
////
//// returns true if labrpc says the RPC was delivered.
////
//func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}
//
//type AppendEntriesArgs struct {
//	Term int
//	//PrevLogIndex int
//	//PrevLogTerm int
//	Entries []Log
//	//LeaderCommit int
//}
//
//type AppendEntriesReply struct {
//	Term int
//	Success bool
//}
//
//func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
//	// heartbeat
//	if(len(args.Entries) == 0) {
//		if r.status == Candidate {
//			if(r.currentTerm <= args.Term) {
//				reply.Success = true
//				r.status = Follower
//				r.currentTerm = args.Term
//				r.timer.HeartbeatReceived <- true
//				printOut("at: " + strconv.Itoa(r.me) + " --> reverted to follower")
//			} else {
//				reply.Success = false
//			}
//
//		} else if r.currentTerm < args.Term {
//			reply.Success = true
//			r.status = Follower
//			r.currentTerm = args.Term
//			r.timer.HeartbeatReceived <- true
//
//		} else if r.currentTerm == args.Term {
//			reply.Success = true
//			r.timer.HeartbeatReceived <- true
//
//		} else {
//			reply.Success = false
//			reply.Term = r.currentTerm
//		}
//	} else if r.currentTerm > args.Term {
//		reply.Success = false
//	}
//
//}
//
//func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
//	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
//	return ok
//}
//
////
//// the service using Raft (e.g. a k/v server) wants to start
//// agreement on the next command to be appended to Raft's log. if this
//// server isn't the leader, returns false. otherwise start the
//// agreement and return immediately. there is no guarantee that this
//// command will ever be committed to the Raft log, since the leader
//// may fail.
////
//// the first return value is the index that the command will appear at
//// if it's ever committed. the second return value is the current
//// term. the third return value is true if this server believes it is
//// the leader.
////
//func (rf *Raft) Start(command interface{}) (int, int, bool) {
//	index := -1
//	term := -1
//	isLeader := true
//
//	return index, term, isLeader
//}
//
////
//// the tester calls Kill() when a Raft instance won't
//// be needed again. you are not required to do anything
//// in Kill(), but it might be convenient to (for example)
//// turn off debug output from this instance.
////
//func (rf *Raft) Kill() {
//	// Your code here, if desired.
//}
//
//
////
//// the service or tester wants to create a Raft server. the ports
//// of all the Raft servers (including this one) are in peers[]. this
//// server's port is peers[me]. persister is a place for this server to
//// save its persistent state, and also initially holds the most
//// recent saved state, if any. applyCh is a channel on which the
//// tester or service expects Raft to send ApplyMsg messages.
////
//func Make(peers []*labrpc.ClientEnd, me int,
//	persister *Persister, applyCh chan ApplyMsg) *Raft {
//	rf := &Raft{}
//	rf.peers = peers
//	rf.persister = persister
//	rf.me = me
//
//	// Your initialization code here.
//	//rf.log = make([]Log, 0)
//	rf.currentTerm = 0
//	rf.status = Follower
//	rf.timer.NewTimer(me)
//	go func(rf *Raft) {
//		for {
//			if _, isLeader := rf.GetState(); !isLeader {
//				var heartbeat bool
//				//printOut("at: " + strconv.Itoa(rf.me) + " --> waiting for hratbeat or election")
//				select {
//				case <- rf.timer.StartElectionChan:
//					heartbeat = false
//				case <- rf.timer.HeartbeatReceived:
//					heartbeat = true
//				}
//
//				//printOut(strconv.Itoa(rf.me) + "-->  heartbeat received: " + strconv.FormatBool(heartbeat))
//				if (!heartbeat) {
//					rf.status = Candidate
//					rf.votesChan = make(chan int)
//
//					printOut("at: " + strconv.Itoa(rf.me) + " --> choosing candidate | is now " + rf.status)
//					if(rf.status == Follower) {
//						continue
//					}
//					// increment term
//					rf.currentTerm++;
//					printOut("at: " + strconv.Itoa(rf.me) + " --> term = " + strconv.Itoa(rf.currentTerm))
//					// vote for self
//					rf.votedFor = rf.me
//					rf.votedForTerm = rf.currentTerm
//
//					for i := 0; i < len(peers); i++ {
//						if (i != rf.me) {
//							go rf.requestVote(i)
//						}
//					}
//
//					var positiveVotes int = 1
//					var receivedVotes int = 1
//
//					for i := range rf.votesChan {
//						receivedVotes++
//						positiveVotes += i;
//
//						// A leader was asserted
//						if (rf.status == Follower) {
//							rf.timer.ResetTimer()
//							printOut("at: " + strconv.Itoa(rf.me) + " --> has reverted to follower")
//							break
//						} else {
//							majority := len(rf.peers) / 2
//							if positiveVotes > majority{
//								rf.status = Leader;
//								rf.votesChan = nil
//								printOut(strconv.Itoa(rf.me) + " is now leader")
//								for i := 0; i < len(rf.peers); i++ {
//									if(i != rf.me) {
//										go func(i int) {
//											rf.sendAppendEntries(i, AppendEntriesArgs{rf.currentTerm, make([]Log, 0)}, &AppendEntriesReply{})
//										}(i)
//									}
//								}
//								go func(rf *Raft) {
//									for _ = range rf.votesChan {}
//								}(rf)
//
//								break
//							}
//
//							if receivedVotes > majority {
//								break
//							}
//						}
//					}
//					// Leader election failed
//					if(rf.status == Candidate) {
//						rf.timer.ResetTimer()
//						printOut("at: " + strconv.Itoa(rf.me) + " --> no majority")
//					}
//				} else {
//					rf.timer.HeartbeatReceived <- true
//				}
//			} else {
//
//				time.Sleep(time.Duration(10)*time.Millisecond)
//
//				for i := 0; i < len(peers); i++ {
//					if(i != rf.me) {
//						go func(i int, rf *Raft) {
//							var reply AppendEntriesReply
//							if(rf.status == Leader) {
//								ok := rf.sendAppendEntries(i, AppendEntriesArgs{rf.currentTerm, make([]Log, 0)}, &reply)
//								if ok {
//									if (!reply.Success && rf.status == Leader) {
//										rf.status = Follower
//										rf.timer.ResetTimer()
//									}
//
//								}
//							}
//						}(i, rf)
//					}
//				}
//				if(testOut) {
//					printOut("at: " + strconv.Itoa(rf.me) + " --> status = " + rf.status)
//				}
//			}
//			//printOut("at: " + strconv.Itoa(rf.me) + " --> runnning")
//		}
//	}(rf)
//
//	// initialize from state persisted before a crash
//	rf.readPersist(persister.ReadRaftState())
//
//
//	return rf
//}
//
//func (rf *Raft) requestVote(i int) {
//	var reply RequestVoteReply
//	// TODO after adding logs, add the log index and term
//	printOut("at: " + strconv.Itoa(rf.me) + " --> send request vote to: " + strconv.Itoa(i))
//	for j:= 5; j >= 0; j-- {
//		ok := rf.sendRequestVote(i, RequestVoteArgs{rf.currentTerm, rf.me, 0, 0}, &reply)
//		if ok {
//			if reply.VoteGranted == true {
//				if (rf.votesChan != nil) {
//					rf.votesChan <- 1
//				}
//			} else {
//				if (rf.currentTerm < reply.Term) {
//					rf.status = Follower
//					rf.currentTerm = reply.Term
//				}
//			}
//			return
//		} else {
//			printOut("at: " + strconv.Itoa(rf.me) + " --> " + strconv.Itoa(i) + "is disconnected")
//		}
//	}
//
//	rf.votesChan <- 0
//}