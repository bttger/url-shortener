package main

import (
	"github.com/bttger/url-shortener/internal/utils"
	"net/rpc"
)

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

func main() {
	client, err := rpc.Dial("tcp", ":9007")
	if err != nil {
		panic(err)
	}
	reply := &AppendEntriesReply{}
	req := AppendEntriesArgs{
		Term:         0,
		LeaderId:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      make([]LogEntry, 0),
		CommitIndex:  0,
	}
	err = client.Call("ConsensusModule.AppendEntries", req, reply)
	if err != nil {
		panic(err)
	}
	utils.Logf("%v", reply.Success)
}
