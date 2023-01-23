package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
)

type Node struct {
	mu sync.Mutex

	id      int
	peerIds []int // TODO needed? bc if not, we can have a random node id

	cm *ConsensusModule

	rpcServer *rpc.Server
	listener  net.Listener

	peerClients map[int]*rpc.Client
	commitChan  chan<- CommitEntry

	ready <-chan interface{}
	quit  chan interface{}

	wg sync.WaitGroup
}

func NewNode(nodeId int, peerIds []int, ready <-chan interface{}, commitChan chan<- CommitEntry) *Node {
	n := &Node{
		id:          nodeId,
		peerIds:     peerIds,
		peerClients: make(map[int]*rpc.Client),
		commitChan:  commitChan,
		ready:       ready,
		quit:        make(chan interface{}),
	}
	n.cm = NewConsensusModule(n.id, n.peerIds, n, n.ready, commitChan)
	n.rpcServer = rpc.NewServer()
	err := n.rpcServer.Register(n.cm)
	if err != nil {
		log.Fatal("rpcServer.Register:", err)
	}
	return n
}

func (n *Node) JoinCluster(port int) {
	n.mu.Lock()
	var err error
	n.listener, err = net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Node %v listening at %s", n.id, n.listener.Addr())
	n.mu.Unlock()

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()

		for {
			conn, err := n.listener.Accept()
			if err != nil {
				select {
				case <-n.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			n.wg.Add(1)
			go func() {
				n.rpcServer.ServeConn(conn)
				n.wg.Done()
			}()
		}
	}()
}
func (n *Node) DisconnectAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.peerClients {
		if n.peerClients[id] != nil {
			err := n.peerClients[id].Close()
			if err != nil {
				log.Fatal(err)
			}
			n.peerClients[id] = nil
		}
	}
}

func (n *Node) Shutdown() {
	n.cm.Stop()
	close(n.quit)
	err := n.listener.Close()
	if err != nil {
		log.Fatal(err)
	}
	n.wg.Wait()
}

func (n *Node) ConnectToPeer(peerId int, addr net.Addr) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		n.peerClients[peerId] = client
	}
	return nil
}

func (n *Node) DisconnectPeer(peerId int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.peerClients[peerId] != nil {
		err := n.peerClients[peerId].Close()
		n.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (n *Node) CallRemoteProcedure(id int, serviceMethod string, args interface{}, reply interface{}) error {
	n.mu.Lock()
	peer := n.peerClients[id]
	n.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}
