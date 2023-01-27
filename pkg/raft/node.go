package raft

import (
	"fmt"
	"github.com/bttger/url-shortener/pkg/utils"
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

type Commit struct {
	FsmCommand interface{}
	ResultChan chan interface{}
}

type Node struct {
	id           int
	clusterSize  int
	usePortsFrom int
	commitChan   chan Commit
	cm           *ConsensusModule
	peers        map[int]*rpc.Client
}

func NewNode(id, clusterSize, usePortsFrom int, commitChan chan Commit) *Node {
	utils.Logf("Initializing Raft node")
	node := &Node{
		id:           id,
		clusterSize:  clusterSize,
		usePortsFrom: usePortsFrom,
		commitChan:   commitChan,
		cm:           nil,
		peers:        make(map[int]*rpc.Client),
	}
	node.cm = newConsensusModule(node)
	return node
}

func (n *Node) JoinCluster() {
	utils.Logf("Joining cluster with %d nodes", n.clusterSize)
	port := n.usePortsFrom + n.clusterSize + n.id - 1
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		utils.Logf("Error listening: %v", err)
	}

	err = rpc.Register(n.cm)
	if err != nil {
		utils.Logf("Error registering RPC service: %v", err)
	}
	go rpc.Accept(listener)
	utils.Logf("RPC: listening on port %d", n.usePortsFrom+n.clusterSize+n.id-1)

	for i := 1; i <= n.clusterSize; i++ {
		if i == n.id {
			continue
		}
		utils.Logf("RPC: start connecting to node %d", i)
		err := n.connectToPeer(i)
		if err != nil {
			os.Exit(1)
		}
	}
	n.cm.start() // TODO I think I should be able to start the consensus module before connecting to all peers
}

// connectToPeer connects to the given peer and adds it to the list of peers. If the connection fails, it will retry
// until for a maximum of dialRetryCount times with a delay of dialRetryInterval.
func (n *Node) connectToPeer(id int) error {
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
		return err
	}
	n.peers[id] = client
	utils.Logf("RPC: connected to node %d", id)
	return nil
}

// Submit a new FsmCommand to the Raft cluster. Must be called only on the leader node and yields an error if not.
// The FsmCommand will be replicated to all other nodes and eventually committed.
func (n *Node) Submit(clientRequest *ClientRequest) error {
	if n.cm.GetState() != Leader {
		return fmt.Errorf("not leader")
	}
	n.cm.Lock()
	defer n.cm.Unlock()
	n.cm.log = append(n.cm.log, LogEntry{
		Term:       n.cm.currentTerm,
		FsmCommand: clientRequest.fsmCommand,
	})
	n.cm.clientRequests[len(n.cm.log)-1] = clientRequest
	utils.Logf("Client request submitted to local log: %v", clientRequest.fsmCommand)
	return nil
}

// callRemoteProcedure calls the given procedure on the given node, waits, and returns the result.
// Possible methods:
// - AppendEntries
// - RequestVote
func (n *Node) callRemoteProcedure(method string, peerId int, args any, reply any) error {
	client := n.peers[peerId]
	serviceMethod := "ConsensusModule." + method
	err := client.Call(serviceMethod, args, reply)
	if err != nil {
		utils.Logf("RPC: error calling %s on node %d: %v", method, peerId, err)
		return err
	}
	return nil
}

// GetLeaderId returns the id of the current leader. If there is no leader, it returns 0.
func (n *Node) GetLeaderId() int {
	n.cm.Lock()
	defer n.cm.Unlock()
	return n.cm.leaderId
}
