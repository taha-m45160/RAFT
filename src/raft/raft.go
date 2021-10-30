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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
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
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// persistent state
	currentTerm int    // latest term that this server has seen
	votedFor    int    // candidate id of the server that this server voted for in current term
	state       string // whether the node is a follower, candidate, or leader

	voteCount      int
	voteRequested  chan int
	isLeader       chan int
	heartBeat      chan int
	closeHeartBeat chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	state := rf.state
	term = rf.currentTerm
	rf.mu.Unlock()

	if state == "leader" {
		isleader = true

	} else {
		isleader = false
	}

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

type AppendEntriesArgs struct {
	Term     int // leader term
	LeaderID int // leader id
}

type AppendEntriesReply struct {
	Term int // receiver term
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term        int // candidate term
	CandidateID int // candidate id
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int  // receiver term
	VoteGranted bool // true if vote granted false otherwise
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
	isLeader := false

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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = "follower"
	rf.currentTerm = 0
	rf.votedFor = 0
	rf.voteRequested = make(chan int)
	rf.isLeader = make(chan int)
	rf.heartBeat = make(chan int)
	rf.closeHeartBeat = make(chan int)

	// spawn necessary goroutines
	go nodeHandler(rf, me)
	// go foo(ch)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func nodeHandler(rf *Raft, me int) {
	rand.Seed(time.Now().UnixNano())
	min := 250
	max := 450
	loop := true

	for loop {
		select {
		case <-time.After(time.Duration(rand.Intn(max-min)+min) * time.Millisecond):
			// milord is no more
			go election(rf, rf.isLeader)

		case <-rf.heartBeat:
			// thou art alive milord, reset timer
			continue
		case <-rf.voteRequested:
			// election progressing
			continue
		case <-rf.isLeader:
			// exit if made leader
			// loop = false
		}
	}
}

func election(rf *Raft, isLeader chan int) {
	fmt.Println(rf.me, "ELECTION")
	rf.mu.Lock()
	rf.currentTerm += 1
	term := rf.currentTerm
	rf.state = "candidate"
	rf.voteCount += 1
	totalPeers := len(rf.peers)
	rf.mu.Unlock()

	storeReplies := make(map[int]*RequestVoteReply)
	Args := RequestVoteArgs{term, rf.me}

	// request votes
	for i := 0; i < totalPeers; i++ {
		if i != rf.me {
			storeReplies[i] = new(RequestVoteReply)
			go rf.sendRequestVote(i, Args, storeReplies[i])
		}
	}

	// wait for election to complete or timeout
	loop := true
	ch := make(chan bool)
	go timer(ch)

	for loop {
		select {
		case loop = <-ch:
			continue
		default:
			rf.mu.Lock()
			status := rf.voteCount
			rf.mu.Unlock()

			if status > (totalPeers / 2) {
				// become Leader
				rf.mu.Lock()
				rf.state = "leader"
				rf.mu.Unlock()
				isLeader <- 1
				return
			}
		}
	}
}

//
// example RequestVote RPC handler. SV end
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.voteRequested <- -1
	term := args.Term
	candidateID := args.CandidateID

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	// votedFor := rf.votedFor
	rf.mu.Unlock()

	if currentTerm >= term {
		reply.Term = currentTerm
		reply.VoteGranted = false

	} else {
		reply.VoteGranted = true

		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = candidateID
		rf.mu.Unlock()
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	if currentTerm > args.Term {
		reply.Term = currentTerm

	} else {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()

		rf.heartBeat <- 1
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		rf.mu.Lock()
		if reply.VoteGranted {
			rf.voteCount += 1

		} else {
			rf.currentTerm = reply.Term
		}
		rf.mu.Unlock()
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok {
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = "follower"
		}
		rf.mu.Unlock()
	}

	return ok
}

func timer(timeout chan bool) {
	rand.Seed(time.Now().UnixNano())
	min := 250
	max := 450
	dur := rand.Intn(max-min) + min

	time.Sleep(time.Duration(dur) * time.Millisecond)

	timeout <- false
}

func heartBeat(rf *Raft) {
	loop := true

	for loop {
		select {
		case <-rf.closeHeartBeat:
			loop = false
			continue
		default:
			rf.mu.Lock()
			totalPeers := len(rf.peers)
			term := rf.currentTerm
			rf.mu.Unlock()

			storeReplies := make(map[int]*AppendEntriesReply)
			Args := AppendEntriesArgs{term, rf.me}

			// send heartbeats
			for i := 0; i < totalPeers; i++ {
				if i != rf.me {
					storeReplies[i] = new(AppendEntriesReply)
					go rf.sendAppendEntries(i, Args, storeReplies[i])
				}
			}
		}
	}
}
