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

const (
	timeoutDebugAddend            = 5 * time.Second
	electionTimeout               = 150*time.Millisecond + timeoutDebugAddend
	electionTimeoutRandomAddition = 150
	heartbeatInterval             = 50*time.Millisecond + timeoutDebugAddend
)

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
