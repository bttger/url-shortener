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
	log         []LogEntry

	// Volatile state on all servers
	state              NodeState
	commitIndex        int // -1 at initialization; deviates from Raft paper since we use 0-based indexing for the log
	lastApplied        int // -1 at initialization; deviates from Raft paper since we use 0-based indexing for the log
	electionResetEvent time.Time

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[int]int
	matchIndex map[int]int // -1 at initialization; deviates from Raft paper since we use 0-based indexing for the log
}

func newConsensusModule(node *Node) *ConsensusModule {
	// TODO read persistent state from disk
	return &ConsensusModule{
		Mutex:              sync.Mutex{},
		node:               node,
		currentTerm:        0,
		votedFor:           0,
		log:                make([]LogEntry, 0),
		state:              Follower,
		commitIndex:        -1,
		lastApplied:        -1,
		electionResetEvent: time.Time{},
		nextIndex:          nil,
		matchIndex:         nil,
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
		LastLogIndex: len(cm.log) - 1,
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
					if votesReceived*2 > clusterSize {
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
	cm.state = Leader
	cm.nextIndex = make(map[int]int, cm.node.clusterSize)
	cm.matchIndex = make(map[int]int, cm.node.clusterSize)

	for i := 1; i <= cm.node.clusterSize; i++ {
		cm.nextIndex[i] = len(cm.log)
		cm.matchIndex[i] = 0
	}
	cm.Unlock()

	go cm.startHeartbeatSending()
}

// startHeartbeatSending starts a ticker that fires every heartbeatInterval to call the AppendEntries RPC on all
// peers. Automatically stops when the state changes. Should be called as a goroutine.
func (cm *ConsensusModule) startHeartbeatSending() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		cm.sendHeartbeat()

		<-ticker.C

		cm.Lock()
		if cm.state != Leader {
			utils.Logf("Stopping heartbeat sending since node is no longer leader")
			cm.Unlock()
			return
		}
		cm.Unlock()
	}
}

// sendHeartbeat concurrently and asynchronously sends an AppendEntries RPC to all peers.
func (cm *ConsensusModule) sendHeartbeat() {
	cm.Lock()
	currentTerm := cm.currentTerm
	cm.Unlock()

	for peerId := 1; peerId <= cm.node.clusterSize; peerId++ {
		if peerId == cm.node.id {
			continue
		}
		go func(pId int) {
			cm.Lock()
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     cm.node.id,
				PrevLogIndex: cm.nextIndex[pId] - 1,
				PrevLogTerm:  cm.getPreviousLogTermOf(pId),
				Entries:      cm.log[cm.nextIndex[pId]:],
				CommitIndex:  cm.commitIndex,
			}
			lastSent := len(cm.log) - 1
			cm.Unlock()

			if len(args.Entries) > 0 {
				utils.Logf("Found %d new entries to send to node %d", len(args.Entries), pId)
			}

			var reply AppendEntriesReply
			utils.Logf("Sending AppendEntries to node %v", pId)
			err := cm.node.callRemoteProcedure("AppendEntries", pId, args, &reply)
			if err == nil {
				utils.Logf("Received AppendEntries reply from node %d: %+v", pId, reply)

				if reply.Term > currentTerm {
					utils.Logf("Received AppendEntries reply from node %d with higher term; becoming follower", pId)
					cm.becomeFollower(reply.Term)
					return
				}

				cm.Lock()
				if cm.state != Leader {
					utils.Logf("Received AppendEntries reply from node %d, but node is no longer leader; ignoring reply", pId)
					cm.Unlock()
					return
				}

				if reply.Success {
					utils.Logf("AppendEntries RPC to node %d was successful: nextIndex=%d, matchIndex=%d", pId, lastSent+1, lastSent)
					cm.nextIndex[pId] = lastSent + 1
					cm.matchIndex[pId] = lastSent
				} else {
					utils.Logf("AppendEntries RPC to node %d was not successful; decreased nextIndex to %d", pId, cm.nextIndex[pId])
					cm.nextIndex[pId] = cm.nextIndex[pId] - 1
				}

				// Check if the leader received a majority for a log entry after the last committed entry and commit it
				for i := cm.commitIndex + 1; i < len(cm.log); i++ {
					if cm.log[i].Term == cm.currentTerm {
						matchCount := 1
						for _, match := range cm.matchIndex {
							if match >= i {
								matchCount++
							}
						}
						if matchCount*2 > cm.node.clusterSize {
							cm.commitIndex = i
						}
					}
				}
				// TODO: apply committed entries to state machine, set lastApplied, and notify client
				cm.Unlock()
			}
		}(peerId)
	}
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
// a vote is granted, or the state changed. Expects the lock of the consensus module to be held.
func (cm *ConsensusModule) resetElectionTimer(reason string) {
	utils.Logf("Resetting election timer; reason: %s", reason)
	cm.electionResetEvent = time.Now()
}

// getLastLogTerm returns the term of the last log entry. Expects the lock of the consensus module to be held.
func (cm *ConsensusModule) getLastLogTerm() int {
	if len(cm.log) == 0 {
		return 0
	}
	return cm.log[len(cm.log)-1].Term
}

// getPreviousLogTermOf returns the term of the log entry immediately preceding new ones that are about to be sent to
// the peer. Expects the lock of the consensus module to be held.
func (cm *ConsensusModule) getPreviousLogTermOf(peerId int) int {
	if cm.nextIndex[peerId] == 0 {
		return 0
	}
	return cm.log[cm.nextIndex[peerId]-1].Term
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
	lastLogIndex := len(cm.log) - 1
	cm.Unlock()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > currentTerm {
		utils.Logf("Received RequestVote from node %d with higher term (%d); becoming follower", args.Term, args.CandidateId)
		cm.becomeFollower(args.Term)
	}

	// Reply false if term < currentTerm (§5.1)
	if args.Term < currentTerm {
		utils.Logf("Received RequestVote from node %d with term %d, but current term is %d; denying vote", args.CandidateId, args.Term, currentTerm)
		reply.Term = currentTerm
		reply.VoteGranted = false
		return nil
	}

	// Make sure this node hasn't voted for another candidate in this term
	if votedFor != 0 && votedFor != args.CandidateId {
		utils.Logf("Received RequestVote from node %d, but node already voted for node %d; denying vote", args.CandidateId, votedFor)
		reply.Term = currentTerm
		reply.VoteGranted = false
		return nil
	}

	// Make sure that the candidate's log is at least as up-to-date as the receiver's log
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
	utils.Logf("Received AppendEntries RPC: %+v", args)
	cm.Lock()
	currentTerm := cm.currentTerm
	cm.Unlock()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1).
	if args.Term > currentTerm {
		utils.Logf("Received AppendEntries RPC from node %d with higher term (%d); becoming follower", args.LeaderId, args.Term)
		cm.becomeFollower(args.Term)
	}

	// Reply false if term < currentTerm (§5.1)
	if args.Term < currentTerm {
		utils.Logf("Received AppendEntries RPC with term %d, but current term is %d; denying AppendEntries", args.Term, cm.currentTerm)
		reply.Term = currentTerm
		reply.Success = false
		return nil
	}

	cm.Lock()
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3).
	if args.PrevLogIndex >= 0 && len(cm.log) > args.PrevLogIndex && cm.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		utils.Logf("Received AppendEntries RPC with PrevLogIndex %d, but PrevLogTerm %d does not match; denying AppendEntries", args.PrevLogIndex, args.PrevLogTerm)
		reply.Term = cm.currentTerm
		reply.Success = false
		cm.Unlock()
		return nil
	}

	// After we made sure all conditions are met and that there is a valid leader, we can reset the election timer.
	cm.resetElectionTimer("received AppendEntries RPC")

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and
	// all that follow it (§5.3). This can happen if the leader has been partitioned away from the rest of the cluster
	// for some time, and then rejoins the cluster with log entries that were inserted during the partition.
	if len(cm.log)-1 > args.PrevLogIndex {
		cm.log = cm.log[:args.PrevLogIndex+1]
	}

	// Append any new entries not already in the log
	cm.log = append(cm.log, args.Entries...)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.CommitIndex > cm.commitIndex {
		cm.commitIndex = utils.IntMin(args.CommitIndex, len(cm.log)-1)
		// TODO: apply committed entries to state machine, set lastApplied
	}

	reply.Success = true
	reply.Term = cm.currentTerm
	cm.Unlock()
	return nil
}
