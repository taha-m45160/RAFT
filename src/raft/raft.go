package raft

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

/*a single RAFT peer object*/
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// persistent state
	currentTerm int    // latest term that this server has seen
	votedFor    int    // candidate id of the server that this server voted for in current term
	state       string // whether the node is a follower, candidate, or leader

	voteCount     int
	voteRequested chan int
	heartBeat     chan int
}

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

func (rf *Raft) persist() {
}

func (rf *Raft) readPersist(data []byte) {
}

type AppendEntriesArgs struct {
	Term     int // leader term
	LeaderID int // leader id
}

type AppendEntriesReply struct {
	Term int // receiver term
}

type RequestVoteArgs struct {
	Term        int // candidate term
	CandidateID int // candidate id
}

type RequestVoteReply struct {
	Term        int  // receiver term
	VoteGranted bool // true if vote granted false otherwise
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	return index, term, isLeader
}

func (rf *Raft) Kill() {
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = "follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteRequested = make(chan int)
	rf.heartBeat = make(chan int)

	// spawn necessary goroutines
	go nodeMain(rf, me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

/*election process conducted upon leader failure*/
func election(rf *Raft) {
	rf.mu.Lock()
	rf.currentTerm += 1
	term := rf.currentTerm
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
	h := false
	go timer(ch, h)

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

				loop = false
				continue
			}
		}
	}

	rf.mu.Lock()
	rf.voteCount = 0
	rf.mu.Unlock()
}

/*request vote rpc received and reply dispatched*/
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.voteRequested <- -1

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	// votedFor := rf.votedFor
	rf.mu.Unlock()

	// candidate requesting vote has a stale term
	// or the same term in which case
	// this node is the candidate itself or
	// has already voted for another candidate
	if currentTerm >= args.Term {
		reply.Term = currentTerm
		reply.VoteGranted = false

	} else {
		reply.VoteGranted = true

		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.mu.Unlock()
	}
}

/*append entries rpc received and reply dispatched*/
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	if currentTerm > args.Term {
		// received appendEntries from old leader
		reply.Term = currentTerm

	} else {
		// heartBeat received
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()

		rf.heartBeat <- 1
	}
}

/*request vote rpc sent and reply handled*/
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// handle reply for the requested vote
	if ok {
		rf.mu.Lock()
		if reply.VoteGranted {
			rf.voteCount += 1

		} else {
			// consider converting candidate to follower
			if rf.currentTerm < reply.Term {
				rf.state = "follower"
			}
			rf.currentTerm = reply.Term
		}
		rf.mu.Unlock()
	}

	return ok
}

/*append entries rpc sent and reply handled*/
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	// handle appendEntries reply from a node
	if ok {
		rf.mu.Lock()
		// leader returns to follower state if its currentTerm is stale
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = "follower"
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		rf.state = "follower"
		rf.mu.Unlock()
	}

	return ok
}

/*return false when the timer runs out*/
func timer(timeout chan bool, heartBeat bool) bool {
	if !heartBeat {
		rand.Seed(time.Now().UnixNano())
		min := 350
		max := 600
		dur := rand.Intn(max-min) + min

		time.Sleep(time.Duration(dur) * time.Millisecond)

		timeout <- false
		return false

	} else {
		rand.Seed(time.Now().UnixNano())
		time.Sleep(40 * time.Millisecond)

		return false
	}
}

/*sends empty AppendEntries RPC as heartbeat message*/
func heartBeat(rf *Raft) {
	// get state
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

/*RAFT peer main process*/
func nodeMain(rf *Raft, me int) {
	rand.Seed(time.Now().UnixNano())
	min := 350
	max := 600

	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case "follower":
			select {
			case <-time.After(time.Duration(rand.Intn(max-min)+min) * time.Millisecond):
				// leader timed out
				rf.mu.Lock()
				rf.state = "candidate"
				rf.mu.Unlock()
			case <-rf.heartBeat:
				// timeout reset
			case <-rf.voteRequested:
				// timeout reset
			}
		case "candidate":
			election(rf)
		case "leader":
			heartBeat(rf)
			t := make(chan bool)
			h := true
			timer(t, h)
		}
	}
}
