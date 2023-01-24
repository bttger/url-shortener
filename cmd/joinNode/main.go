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

	commitChan := make(chan *urlShortener.FSMInputRequest)
	store := urlShortener.NewURLStore()
	go store.ListenToNewCommits(commitChan)
	raftNode := raft.NewNode(nodeId, clusterSize, usePortsFrom, commitChan)
	raftNode.Start()
	err = urlShortener.Start(usePortsFrom+nodeId-1, store, raftNode)
}
