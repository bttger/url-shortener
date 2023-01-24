package raft

import (
	"github.com/bttger/url-shortener/internal/utils"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

const (
	dialRetryInterval = 100 * time.Millisecond
	dialRetryCount    = 5
)

// TODO mutex needed for local methods?
type Node struct {
	id           int
	clusterSize  int
	usePortsFrom int
	commitChan   chan *FSMInput
	rpcServer    *rpc.Server
	cm           *ConsensusModule
	peers        map[int]*rpc.Client
}

func NewNode(id, clusterSize, usePortsFrom int, commitChan chan *FSMInput) *Node {
	utils.Logf("Initializing Raft node")
	node := &Node{
		id:           id,
		clusterSize:  clusterSize,
		usePortsFrom: usePortsFrom,
		commitChan:   commitChan,
		rpcServer:    rpc.NewServer(),
		cm:           nil,
		peers:        make(map[int]*rpc.Client),
	}
	node.cm = NewConsensusModule(node)
	return node
}

func (n *Node) JoinCluster() {
	utils.Logf("Joining cluster with %d nodes", n.clusterSize)
	port := n.usePortsFrom + n.clusterSize + n.id - 1
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		utils.Logf("Error listening: %v", err)
	}

	err = n.rpcServer.RegisterName("CM", n.cm)
	if err != nil {
		utils.Logf("Error registering RPC service: %v", err)
	}
	go rpc.Accept(listener)
	utils.Logf("RPC service listening on port %d", n.usePortsFrom+n.clusterSize+n.id-1)

	utils.Logf("RPC: connecting to other nodes")
	for i := 1; i <= n.clusterSize; i++ {
		if i == n.id {
			continue
		}
		utils.Logf("RPC: connecting to node %d", i)
		n.connectToPeer(i)
	}
}

func (n *Node) connectToPeer(id int) {
	var client *rpc.Client
	var err error
	for i := 0; i < dialRetryCount; i++ {
		port := n.usePortsFrom + n.clusterSize + id - 1
		client, err = rpc.Dial("tcp", ":"+strconv.Itoa(port))
		if err == nil {
			break
		}
		time.Sleep(dialRetryInterval)
	}
	if err != nil {
		utils.Logf("RPC: error dialing node %d: %v", id, err)
		os.Exit(1)
	}
	n.peers[id] = client
	utils.Logf("RPC: connected to node %d", id)
}

func (n *Node) Submit(input *FSMInput) {
	go func() {
		// TODO send input to leader's appendEntries backlog which in turn will send it to the commitChan
		n.commitChan <- input
	}()
}
