package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type Request struct {
	command interface{}
	index   int
}

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
	Term           int  // receiver term
	Success        bool // does follower contain entry matching prevLogIndex and prevLogTerm
	AgreementIndex int
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
	matchIndex map[int]int // index of highest log entry known to be replicated on each server

	// utility
	voteCount     int          // count of total votes in each election for a node
	voteRequested chan int     // channel to inform main process if requestVote RPC received
	heartBeat     chan int     // channel to inform main process if heartbeat received
	legitLeader   chan bool    // channel to inform main process of a heartbeat from a legitimate leader
	requestQueue  chan Request // queue for client requests
	commitCh      chan int
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
	rf.legitLeader = make(chan bool)
	rf.requestQueue = make(chan Request, 500)
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = 0 // garbage value at index 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.commitCh = make(chan int)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = 1
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i] = 0
	}

	// spawn necessary goroutines
	go handleElection(rf, me)
	go rf.requestHandler(applyCh)
	// go rf.applyHandler(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// 	// Your code here.
	// 	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// //
// // restore previously persisted state.
// //
func (rf *Raft) readPersist(data []byte) {
	// 	// Your code here.
	// 	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == "leader" {
		// add entry to log
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})

		return len(rf.log) - 1, rf.currentTerm, true

	} else {
		return -1, -1, false
	}
}

func (rf *Raft) Kill() {
}

// /*periodically checks if a value is committed and in which case dispatches it through applyCh*/
// func (rf *Raft) applyHandler(applyCh chan ApplyMsg) {
// 	for {

// 	}
// }

/*handles client request (agreement on new log entry)*/
func (rf *Raft) requestHandler(applyCh chan ApplyMsg) {
	for {
		// apply
		<-rf.commitCh

		rf.mu.Lock()
		lastAppl := rf.lastApplied
		commitIdx := rf.commitIndex
		log := rf.log
		rf.mu.Unlock()

		// apply log entry if it has been committed
		for commitIdx > lastAppl {
			rf.mu.Lock()
			rf.lastApplied++
			lastAppl = rf.lastApplied
			rf.mu.Unlock()

			applyCh <- ApplyMsg{lastAppl, log[lastAppl].Command, false, make([]byte, 0)}
		}
	}
}

func (rf *Raft) commitEntries() {
	rf.mu.Lock()
	log := copySlice(rf.log)
	matchIdx := copyMap(rf.matchIndex)
	commitIdx := rf.commitIndex
	totalPeers := len(rf.peers)
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for N := commitIdx + 1; N < len(log); N++ {
		count := 1

		for i := 0; i < totalPeers; i++ {
			if i != rf.me && matchIdx[i] >= N {
				count++
			}
		}

		if (count > totalPeers/2) && (log[N].Term == currentTerm) {
			rf.mu.Lock()
			rf.commitIndex = N
			rf.mu.Unlock()
		}
	}

	rf.persist()
}

func validateVote(lastLogIndex1, lastLogIndex2, lastLogTerm1, lastLogTerm2 int) bool {
	/*
		returns true if candidate's log is at least
		as up-to-date as the follower's log
		otherwise returns false

		Input:
		1: Candidate
		2: Follower
	*/

	if lastLogTerm1 > lastLogTerm2 {
		return true

	} else if lastLogTerm1 < lastLogTerm2 {
		return false

	} else if lastLogIndex1 >= lastLogIndex2 {
		return true

	} else {
		return false
	}
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	defer rf.persist()

	// candidate requesting vote has a stale term
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false

	} else {
		reply.VoteGranted = false

		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.mu.Unlock()

		// check if log atleast up-to-date as its own
		validate := validateVote(args.LastLogIndex, lastLogIndex, args.LastLogTerm, lastLogTerm)

		if (votedFor == -1 || votedFor == args.CandidateID) && validate {
			rf.voteRequested <- -1
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.votedFor = args.CandidateID
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	state := rf.state
	log := copySlice(rf.log)
	rf.mu.Unlock()

	defer rf.persist()

	// received appendEntries from old leader
	// return newer term
	if currentTerm > args.Term {
		reply.Term = currentTerm
		reply.Success = false

		return
	}

	// if candidate and heartbeat received from legitLeader leader
	// then cancel election and revert to follower
	if state == "candidate" {
		rf.legitLeader <- false
	}

	rf.mu.Lock()
	rf.currentTerm = args.Term
	rf.state = "follower"
	rf.mu.Unlock()

	// reset timer
	rf.heartBeat <- 1

	// checks if log is consistent with leader's log
	// if not return last non-conflicting index to the leader
	lastLogIndex := len(log) - 1
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm

	consistentAt := consistencyCheck(log, args.Entries, prevLogIndex, lastLogIndex, prevLogTerm)
	if consistentAt != args.PrevLogIndex {
		reply.Success = false
		reply.AgreementIndex = consistentAt

		return
	}

	// checks for conflicting values
	// truncates follower log if required and adds entries
	rf.mu.Lock()
	rf.log = modifyLog(log, args.Entries, prevLogIndex)
	log = copySlice(rf.log)

	// commit entries
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(log)-1)
	}

	rf.mu.Unlock()

	rf.commitCh <- 1

	reply.Success = true
}

/*gets the latest index on which there is agreement*/
func consistencyCheck(followerLog, leaderLog []LogEntry, prevLogIndex, lastLogIndex, prevLogTerm int) int {
	for (prevLogIndex > lastLogIndex) || (followerLog[prevLogIndex].Term != prevLogTerm) {
		prevLogIndex--
		prevLogTerm = leaderLog[prevLogIndex].Term
	}

	return prevLogIndex
}

/*truncates follower log if required and adds entries*/
func modifyLog(followerLog, leaderLog []LogEntry, prevLogIndex int) []LogEntry {
	llogSize := len(leaderLog)
	flogSize := len(followerLog)

	i, j := prevLogIndex+1, prevLogIndex+1
	for i < flogSize && j < llogSize {
		if followerLog[i].Term != leaderLog[j].Term {
			break
		}

		i++
		j++
	}

	return append(followerLog[:i], leaderLog[j:]...)
}

/*request vote rpc sent and reply handled*/
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	defer rf.persist()

	// handle reply for the requested vote
	if ok {
		rf.mu.Lock()
		if reply.VoteGranted {
			rf.voteCount += 1

		} else {
			// consider converting candidate to follower
			if rf.currentTerm < reply.Term {
				rf.state = "follower"
				//rf.legitLeader <- false
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

	defer rf.persist()

	// handle appendEntries reply from a node
	if ok {
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		me := rf.me
		log := copySlice(rf.log)
		commitIdx := rf.commitIndex
		rf.mu.Unlock()

		if !reply.Success {
			// failure due to term inconsistency
			if currentTerm < reply.Term {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.state = "follower"
				rf.mu.Unlock()

				return ok
			}

			// faiure due to log inconsistency
			latestMatchingIndex := reply.AgreementIndex
			Args := AppendEntriesArgs{currentTerm, me, latestMatchingIndex, log[latestMatchingIndex].Term, log, commitIdx}
			go rf.sendAppendEntries(server, Args, new(AppendEntriesReply))

			return ok
		}

		// update matchIndex and nextIndex on success
		rf.mu.Lock()

		if rf.matchIndex[server] < len(args.Entries)-1 {
			rf.matchIndex[server] = len(args.Entries) - 1
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.mu.Unlock()

		// commit entries
		rf.commitEntries()

		rf.commitCh <- 1
	}

	return ok
}

/*return false when timer runs out*/
func timer(timeout chan bool, heartBeat bool) bool {
	if !heartBeat {
		rand.Seed(time.Now().UnixNano())
		min := 300
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
	log := copySlice(rf.log)
	rf.mu.Unlock()

	// send heartbeats
	for i := 0; i < totalPeers; i++ {
		if i != rf.me {
			rf.mu.Lock()
			prevLogIndex := rf.matchIndex[i]
			Args := AppendEntriesArgs{term, rf.me, prevLogIndex, log[prevLogIndex].Term, log, rf.commitIndex}
			rf.mu.Unlock()

			go rf.sendAppendEntries(i, Args, &AppendEntriesReply{})
		}
	}
}

/*monitors node state and calls election on detecting leader failure*/
func handleElection(rf *Raft, me int) {
	rand.Seed(time.Now().UnixNano())
	min := 300
	max := 600
	prevState := "follower"
	heartbeatCount := 0

	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		// reset vote
		if (prevState == "candidate" && state == "follower") || heartbeatCount == 0 {
			rf.mu.Lock()
			rf.votedFor = -1
			rf.mu.Unlock()
		}

		prevState = state

		switch state {
		case "follower":
			select {
			case <-time.After(time.Duration(rand.Intn(max-min)+min) * time.Millisecond):
				// leader timed out
				heartbeatCount = 0
				rf.mu.Lock()
				rf.state = "candidate"
				rf.mu.Unlock()
			case <-rf.heartBeat:
				// timeout reset
				heartbeatCount++
			case <-rf.voteRequested:
				// timeout reset
				heartbeatCount = 0
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
	rf.mu.Lock()
	rf.voteCount = 1
	rf.currentTerm += 1
	term := rf.currentTerm
	totalPeers := len(rf.peers)
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	Args := RequestVoteArgs{term, rf.me, lastLogIndex, lastLogTerm}

	// request votes
	for i := 0; i < totalPeers; i++ {
		if i != rf.me {
			go rf.sendRequestVote(i, Args, new(RequestVoteReply))
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
				drainQueue(rf.requestQueue)

				rf.mu.Lock()
				// reinitialize after election
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					rf.nextIndex[i] = 1
				}

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					rf.matchIndex[i] = 0
				}

				rf.state = "leader"
				rf.votedFor = -1
				rf.mu.Unlock()

				loop = false
				continue
			}
		}
	}
}

/*utility functions*/
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func drainQueue(ch chan Request) {
	/*drains client request queue channel*/
	for len(ch) > 0 {
		<-ch
	}
}

func copySlice(arr []LogEntry) []LogEntry {
	/*create a deepcopy of a slice*/
	return append(make([]LogEntry, 0, len(arr)), arr...)
}

func copyMap(arr map[int]int) map[int]int {
	newMap := make(map[int]int, len(arr))
	for k, v := range arr {
		newMap[k] = v
	}

	return newMap
}
