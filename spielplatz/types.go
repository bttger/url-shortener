package main

type LogEntry struct {
	Term       int
	FsmCommand interface{}
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
type ConsensusModule struct {
	id          int
	currentTerm int
}
