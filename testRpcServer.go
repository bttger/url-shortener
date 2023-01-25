package main

import (
	"github.com/bttger/url-shortener/internal/utils"
	"net"
	"net/rpc"
	"time"
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
type ConsensusModule struct {
	id          int
	currentTerm int
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	utils.Logf("AppendEntries: %+v", args)
	reply.Success = true
	reply.Term = cm.currentTerm
	return nil
}

func main() {
	listener, err := net.Listen("tcp", ":9019")
	if err != nil {
		panic(err)
	}
	cm := &ConsensusModule{
		id:          0,
		currentTerm: 0,
	}
	err = rpc.Register(cm)
	if err != nil {
		panic(err)
	}
	go rpc.Accept(listener)
	time.Sleep(40 * time.Second)

	//for {
	//	conn, err := listener.Accept()
	//	if err != nil {
	//		panic(err)
	//	}
	//	go cm.rpcServer.ServeConn(conn)
	//}

}
