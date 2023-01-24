package raft

import (
	"net/rpc"
	"time"
)

type Node struct {
	id           int
	clusterSize  int
	usePortsFrom int
	commitChan   chan *FSMInput
	rpcServer    *rpc.Server
}

func NewNode(id, clusterSize, usePortsFrom int, commitChan chan *FSMInput) *Node {
	return &Node{
		id:           id,
		clusterSize:  clusterSize,
		usePortsFrom: usePortsFrom,
		commitChan:   commitChan,
		rpcServer:    rpc.NewServer(),
	}
}

func (n *Node) JoinCluster() {
	// TODO
}

func (n *Node) Submit(input *FSMInput) {
	time.Sleep(200 * time.Millisecond)
	// TODO send input to leader's appendEntries backlog which in turn will send it to the commitChan
	n.commitChan <- input
}
