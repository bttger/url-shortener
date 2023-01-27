package raft

import "github.com/bttger/url-shortener/internal/utils"

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
	cm.leaderId = args.LeaderId

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

		for i := cm.lastApplied + 1; i <= cm.commitIndex; i++ {
			cm.commitAndApplyLogEntry(i, false)
		}
	}

	reply.Success = true
	reply.Term = cm.currentTerm
	cm.Unlock()
	return nil
}
