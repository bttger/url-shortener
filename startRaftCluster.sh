#!/usr/bin/env bash

CLUSTER_SIZE=5
USE_PORTS_FROM=9000

for (( i=1; i<=CLUSTER_SIZE; i++ ));do
    DEBUG="true" USE_PORTS_FROM=$USE_PORTS_FROM NODE_ID=$i CLUSTER_SIZE=$CLUSTER_SIZE go run ./cmd/joinNode/main.go NODE_ID=$i &
done

wait
