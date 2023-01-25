package raft

import (
	"github.com/bttger/url-shortener/internal/utils"
	"math/rand"
	"sync"
	"time"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// electionTimeout is the time a follower waits for a heartbeat from the leader before starting an election.
const electionTimeout = 150 * time.Millisecond
const electionTimeoutRandomAddition = 150
const heartbeatInterval = 50 * time.Millisecond

type LogEntry struct {
	Term       int
	FsmCommand interface{}
}

type ConsensusModule struct {
	sync.Mutex
	node *Node

	// Persistent state on all servers (must be persisted to stable storage before responding to RPCs)
	currentTerm int
	votedFor    int
	log         []LogEntry // index of first entry is 1

	// Volatile state on all servers
	state              NodeState
	commitIndex        int
	lastApplied        int
	electionResetEvent time.Time

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[int]int
	matchIndex map[int]int
}

func newConsensusModule(node *Node) *ConsensusModule {
	// TODO read persistent state from disk
	return &ConsensusModule{
		Mutex:       sync.Mutex{},
		node:        node,
		state:       Follower,
		currentTerm: 0,
		votedFor:    0,
		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   nil,
		matchIndex:  nil,
	}
}

func (cm *ConsensusModule) start() {
	utils.Logf("Starting consensus module")
	cm.becomeFollower(0)
}

func (cm *ConsensusModule) becomeFollower(term int) {
	utils.Logf("Becoming follower with term %d", term)
	cm.Lock()
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = 0
	cm.Unlock()

	go cm.startElectionTimer()
}

func (cm *ConsensusModule) becomeCandidate() {
	utils.Logf("Becoming candidate")
	cm.Lock()
	cm.state = Candidate
	cm.currentTerm++
	cm.votedFor = cm.node.id

	// Save current values for RPC closures
	args := RequestVoteArgs{
		Term:         cm.currentTerm,
		CandidateId:  cm.node.id,
		LastLogIndex: len(cm.log),
		LastLogTerm:  cm.getLastLogTerm(),
	}
	cm.Unlock()

	votesReceived := 1

	for peerId := 1; peerId <= cm.node.clusterSize; peerId++ {
		if peerId == cm.node.id {
			continue
		}
		go func(pId int) {
			var reply RequestVoteReply
			utils.Logf("Sending RequestVote to node %v", pId)
			err := cm.node.callRemoteProcedure("RequestVote", pId, args, &reply)
			if err == nil {
				utils.Logf("Received RequestVote reply from node %d: %+v", pId, reply)

				cm.Lock()
				currentTerm := cm.currentTerm
				state := cm.state
				clusterSize := cm.node.clusterSize
				cm.Unlock()

				if reply.Term > currentTerm {
					utils.Logf("Received RequestVote reply from node %d with higher term; becoming follower", pId)
					cm.becomeFollower(reply.Term)
					return
				}

				if state != Candidate {
					utils.Logf("Received RequestVote reply from node %d, but node is no longer candidate; ignoring reply", pId)
					return
				}

				if reply.VoteGranted {
					votesReceived++
					if votesReceived > clusterSize/2 {
						utils.Logf("Received majority of votes")
						cm.becomeLeader()
					}
				}
			}
		}(peerId)
	}

	// Start another election timer in case of unsuccessful election (e.g. split vote)
	go cm.startElectionTimer()
}

func (cm *ConsensusModule) becomeLeader() {
	utils.Logf("Becoming leader")
	cm.Lock()
	defer cm.Unlock() // TODO attention: defer might lead to deadlock
	cm.state = Leader
	cm.nextIndex = make(map[int]int, cm.node.clusterSize)
	cm.matchIndex = make(map[int]int, cm.node.clusterSize)

	for i := 1; i <= cm.node.clusterSize; i++ {
		cm.nextIndex[i] = len(cm.log) + 1
		cm.matchIndex[i] = 0
	}

	// go cm.startHeartbeatSending()
}

// startElectionTimer starts a timer that repeatedly checks if the electionTimeout has expired to initiate an election.
// Should be called as a goroutine.
func (cm *ConsensusModule) startElectionTimer() {
	cm.Lock()
	termStarted := cm.currentTerm
	cm.resetElectionTimer("new election timer about to start")
	cm.Unlock()

	rand.Seed(time.Now().UnixNano())
	randomAddition := time.Duration(rand.Intn(electionTimeoutRandomAddition)) * time.Millisecond
	timeout := electionTimeout + randomAddition
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	utils.Logf("Started election timer (%d) with timeout of %vms", termStarted, timeout)

	for {
		<-ticker.C

		cm.Lock()
		state := cm.state
		currentTerm := cm.currentTerm
		electionResetEvent := cm.electionResetEvent
		cm.Unlock()

		if state == Leader {
			utils.Logf("Received election timer (%d) tick, but node won election already; stopping election timer", termStarted)
			return
		}
		if termStarted < currentTerm {
			utils.Logf("Received election timer (%d) tick, but term changed in mean time; stopping election timer", termStarted)
			return
		}
		if electionResetEvent.Add(timeout).Before(time.Now()) {
			utils.Logf("Election timer expired")
			cm.becomeCandidate()
			return
		}
	}
}

// resetElectionTimer updates the electionResetEvent. Must be called when a heartbeat is received from the leader,
// a vote is granted, or the state changed.
// Expects the lock of the consensus module to be held.
func (cm *ConsensusModule) resetElectionTimer(reason string) {
	utils.Logf("Resetting election timer; reason: %s", reason)
	cm.electionResetEvent = time.Now()
}

// getLastLogTerm returns the term of the last log entry. Must be called with the lock of the consensus module held.
func (cm *ConsensusModule) getLastLogTerm() int {
	if len(cm.log) == 0 {
		return 0
	}
	return cm.log[len(cm.log)-1].Term
}

func (cm *ConsensusModule) GetState() NodeState {
	cm.Lock()
	defer cm.Unlock()
	return cm.state
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	utils.Logf("Received RequestVote from node %d with term %d", args.CandidateId, args.Term)
	cm.Lock()
	currentTerm := cm.currentTerm
	votedFor := cm.votedFor
	lastLogTerm := cm.getLastLogTerm()
	lastLogIndex := len(cm.log)
	cm.Unlock()

	if args.Term < currentTerm {
		utils.Logf("Received RequestVote from node %d with term %d, but current term is %d; denying vote", args.CandidateId, args.Term, currentTerm)
		reply.Term = currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > currentTerm {
		utils.Logf("Received RequestVote from node %d with higher term (%d); becoming follower", args.Term, args.CandidateId)
		cm.becomeFollower(args.Term)
	}

	if votedFor != 0 && votedFor != args.CandidateId {
		utils.Logf("Received RequestVote from node %d, but node already voted for node %d; denying vote", args.CandidateId, votedFor)
		reply.Term = currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		utils.Logf("Received RequestVote from node %d, but this node has newer log entries; denying vote", args.CandidateId)
		reply.Term = currentTerm
		reply.VoteGranted = false
		return nil
	}

	utils.Logf("Received RequestVote from node %d, granting vote", args.CandidateId)

	cm.Lock()
	cm.votedFor = args.CandidateId
	cm.resetElectionTimer("granted vote")
	cm.Unlock()

	reply.Term = cm.currentTerm
	reply.VoteGranted = true
	return nil
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	utils.Logf("AppendEntries: %+v", args)
	reply.Success = true
	reply.Term = cm.currentTerm
	return nil
}
