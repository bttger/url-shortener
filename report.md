# Cloud-based Data Processing - URL shortener project

## Build and run

> Requires Go to be installed

```bash
```sh
chmod u+x startRaftCluster.sh
./startRaftCluster.sh
```

## Describe the design of your system

The system consists of the following main components:

- Raft library (`/pkg/raft`; closely following the Raft paper)
- URL shortener service (`/internal/urlShortener`)
  - HTTP server (`/internal/urlShortener/server.go`; handles client requests)
  - Finite-state machine (`/internal/urlShortener/store.go`; listens to Raft commits, applies them to the state machine, and responds to read-only queries)
- Main executable (`/cmd/joinNode/main.go`; starts a new Raft node and the URL shortener service)

The URL shortener service can simply be swapped out with another service that implements the `ListenToNewCommits(commitChan chan raft.Commit)` method. This method indefinitely listens to new commits from the Raft library and applies them to the state machine. The state machine is responsible for responding to read-only queries by the server.

## How does your cluster handle leader crashes?
* How long does it take to elect a new leader?
* Measure the impact of election timeouts. Investigate what happens when it gets too short / too long.

## Analyze the load of your nodes:
* How many resources do your nodes use?
* Where do inserts create most load?
* Do lookups distribute the load evenly among replicas?

## How many nodes should you use for this system? What are their roles?

## Measure the latency to generate a new short URL
* Analyze where your system spends time during this operation

## Measure the lookup latency to get the URL from a short id

## How does your system scale?
* Measure the latency with increased data inserted, e.g., in 10% increments of inserted short URLs
* Measure the system performance with more nodes
