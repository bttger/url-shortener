package raft

import (
	"github.com/bttger/url-shortener/internal/utils"
	"sync"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term       int
	FsmCommand interface{}
}

type ConsensusModule struct {
	sync.Mutex
	node  *Node
	state NodeState

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry
}

func NewConsensusModule(node *Node) *ConsensusModule {
	return &ConsensusModule{
		node: node,
	}
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
	panic("not implemented")
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
