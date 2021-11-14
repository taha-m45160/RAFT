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

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

type LogEntry struct {
	Command interface{} // received from client
	Term    int         // term when entry was received
}

type AppendEntriesArgs struct {
	Term         int        // leader term
	LeaderID     int        // leader id
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; consider sending more than one for efficiency)
	LeaderCommit int        // highest log index known to be committed by the leader
}

type AppendEntriesReply struct {
	Term    int  // receiver term
	Success bool // does follower contain entry matching prevLogIndex and prevLogTerm
}

type RequestVoteArgs struct {
	Term         int // candidate term
	CandidateID  int // candidate id
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // receiver term
	VoteGranted bool // true if vote granted false otherwise
}

/*a single RAFT peer object*/
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// persistent state
	currentTerm int        // latest term that this server has seen
	votedFor    int        // candidate id of the server that this server voted for in current term
	state       string     // whether the node is a follower, candidate, or leader
	log         []LogEntry // stores log entries

	// volatile state
	commitIndex int // highest log entry known to be committed
	lastApplied int // highest log entry applied to state machine

	// volatile state if leader
	nextIndex  map[int]int // index of the next entry to send to each server
	matchIndex map[int]int // index of highest log entry to send to each server

	// utility
	voteCount      int           // count of total votes in each election for a node
	voteRequested  chan int      // channel to inform main process if requestVote RPC received
	heartBeat      chan int      // channel to inform main process if heartbeat received
	legitLeader    chan bool     // channel to inform main process of a heartbeat from a legitimate leader
	requestQueue   chan Request  // queue for client requests
	followerCommit chan ApplyMsg // forwards commit requests for follower

	prevLogTerm  int
	prevLogIndex int
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	c := command

	rf.mu.Lock()
	s := rf.state
	rf.mu.Unlock()

	if s == "leader" {
		// add entry to log
		rf.mu.Lock()
		term := rf.currentTerm
		index := len(rf.log)
		entry := LogEntry{c, term}
		rf.log = append(rf.log, entry)
		rf.mu.Unlock()

		rf.requestQueue <- Request{c, index}

		return index, term, true

	} else {
		return -1, -1, false
	}
}

func (rf *Raft) Kill() {
}

type Request struct {
	command interface{}
	index   int
}

func handleCommits(rf *Raft, applyCh chan ApplyMsg) {
	for {
		select {
		case newReq := <-rf.requestQueue:
			// leader commit processing
			rf.mu.Lock()
			term := rf.currentTerm
			log := rf.log
			totalPeers := len(rf.peers)

			fmt.Println(rf.lastApplied)
			Args := AppendEntriesArgs{term, rf.me, rf.prevLogIndex, rf.prevLogTerm, log, rf.lastApplied}
			rf.mu.Unlock()

			storeReplies := make([]*AppendEntriesReply, totalPeers)
			successCount := 0

			// send append entries
			for i := 0; i < totalPeers; i++ {
				if i != rf.me {
					storeReplies[i] = new(AppendEntriesReply)
					rf.sendAppendEntries(i, Args, storeReplies[i]) // blocks

					if storeReplies[i].Success {
						successCount++
					}
				}
			}

			if successCount > (totalPeers / 2) {
				// commit entry
				applyCh <- ApplyMsg{newReq.index, newReq.command, false, make([]byte, 0)}
			}

			rf.mu.Lock()
			rf.lastApplied++
			rf.prevLogIndex = len(log) - 1
			rf.prevLogTerm = term
			rf.mu.Unlock()

		case newCommit := <-rf.followerCommit:
			// follower commit processing
			applyCh <- newCommit
		}
	}
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
	rf.prevLogIndex = -1
	rf.prevLogTerm = rf.currentTerm
	rf.voteRequested = make(chan int)
	rf.heartBeat = make(chan int)
	rf.legitLeader = make(chan bool)
	rf.requestQueue = make(chan Request, 500)
	rf.followerCommit = make(chan ApplyMsg)
	rf.lastApplied = -1
	rf.commitIndex = -1

	// spawn necessary goroutines
	go nodeMain(rf, me)
	go handleCommits(rf, applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
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
		rf.state = "follower"
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
		// return newer term
		reply.Term = currentTerm

	} else {
		// heartBeat received
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.state = "follower"

		// if candidate and heartbeat received from legitLeader leader
		// then cancel election and revert to follower
		if rf.state == "candidate" {
			rf.legitLeader <- false
		}

		rf.mu.Unlock()

		rf.heartBeat <- 1

		// append new entries
		rf.mu.Lock()
		for i := args.PrevLogIndex + 1; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}

		// if any new entries have been committed by the leader
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}

		//fmt.Println(rf.me, rf.commitIndex, rf.lastApplied)
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.followerCommit <- ApplyMsg{rf.lastApplied, rf.log[rf.lastApplied].Command, false, make([]byte, 0)}
		}
		rf.mu.Unlock()

		reply.Success = true
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

/*return false when timer runs out*/
func timer(timeout chan bool, heartBeat bool) bool {
	if !heartBeat {
		rand.Seed(time.Now().UnixNano())
		min := 450
		max := 600
		dur := rand.Intn(max-min) + min

		time.Sleep(time.Duration(dur) * time.Millisecond)

		timeout <- false
		return false

	} else {
		rand.Seed(time.Now().UnixNano())
		time.Sleep(100 * time.Millisecond)

		return false
	}
}

/*sends empty AppendEntries RPC as heartbeat message*/
func heartBeat(rf *Raft) {
	// get state
	rf.mu.Lock()
	totalPeers := len(rf.peers)
	term := rf.currentTerm
	log := rf.log
	rf.mu.Unlock()

	storeReplies := make(map[int]*AppendEntriesReply)
	rf.mu.Lock()
	Args := AppendEntriesArgs{term, rf.me, rf.prevLogIndex, rf.prevLogTerm, []LogEntry{}, rf.lastApplied}
	rf.mu.Unlock()

	// send heartbeats
	for i := 0; i < totalPeers; i++ {
		if i != rf.me {
			storeReplies[i] = new(AppendEntriesReply)
			go rf.sendAppendEntries(i, Args, storeReplies[i])
		}
	}

	rf.mu.Lock()
	rf.prevLogIndex = len(log) - 1
	rf.prevLogTerm = term
	rf.mu.Unlock()
}

/*main RAFT peer process*/
func nodeMain(rf *Raft, me int) {
	rand.Seed(time.Now().UnixNano())
	min := 450
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

/*election process conducted upon leader failure*/
func election(rf *Raft) {
	fmt.Println("ELECTION YAY")
	rf.mu.Lock()
	rf.currentTerm += 1
	term := rf.currentTerm
	rf.voteCount += 1
	totalPeers := len(rf.peers)
	rf.mu.Unlock()

	storeReplies := make(map[int]*RequestVoteReply)
	Args := RequestVoteArgs{term, rf.me, -1, -1}

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
		case loop = <-rf.legitLeader:
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

/*utility function*/
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
