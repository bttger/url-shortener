package main

import (
	"fmt"
	"github.com/bttger/url-shortener/internal/raft"
	"github.com/bttger/url-shortener/internal/urlShortener"
	"os"
	"strconv"
)

// Required environment variables:
// NODE_ID=number (starting at 1)
// CLUSTER_SIZE=number
// USE_PORTS_FROM=number
func main() {
	nodeId, err := strconv.Atoi(os.Getenv("NODE_ID"))
	clusterSize, err := strconv.Atoi(os.Getenv("CLUSTER_SIZE"))
	usePortsFrom, err := strconv.Atoi(os.Getenv("USE_PORTS_FROM"))
	if err != nil {
		fmt.Println("Please set the environment variables NODE_ID, CLUSTER_SIZE and USE_PORTS_FROM")
		os.Exit(1)
	}

	// Spawn a new finite-state machine and let it listen for new committed inputs
	commitChan := make(chan *raft.FSMInput)
	store := urlShortener.NewURLStore()
	go store.ListenToNewCommits(commitChan)
	// Join a new node in a Raft cluster and publish committed log entries to the commit channel
	raftNode := raft.NewNode(nodeId, clusterSize, usePortsFrom, commitChan)
	raftNode.JoinCluster()
	// Start the HTTP server to handle client requests
	err = urlShortener.Start(usePortsFrom+nodeId-1, store, raftNode)
}
