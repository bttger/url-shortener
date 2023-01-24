package main

import (
	"fmt"
	"github.com/bttger/url-shortener/internal/raft"
	"github.com/bttger/url-shortener/internal/urlShortener"
	"os"
	"strconv"
)

func main() {
	usePortsFrom, err := strconv.Atoi(os.Getenv("USE_PORTS_FROM"))
	nodeId, err := strconv.Atoi(os.Getenv("NODE_ID"))
	clusterSize, err := strconv.Atoi(os.Getenv("CLUSTER_SIZE"))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Spawn a new finite-state machine
	commitChan := make(chan *urlShortener.FSMInputRequest)
	store := urlShortener.NewURLStore()
	// Let it listen for new committed inputs
	go store.ListenToNewCommits(commitChan)
	// Start a new Raft node
	raftNode := raft.NewNode(nodeId, clusterSize, usePortsFrom, commitChan)
	raftNode.Start()
	// Start the HTTP server to handle client requests
	err = urlShortener.Start(usePortsFrom+nodeId-1, store, raftNode)
}
