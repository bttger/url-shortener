# Cloud-based Data Processing - URL shortener project

## Build and run
```sh
chmod u+x startRaftCluster
./startRaftCluster
```

## Describe the design of your system

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
