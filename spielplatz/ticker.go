package main

import (
	"fmt"
	"math/rand"
	"time"
)

const electionTimeout = 200 * time.Millisecond
const electionTimeoutRandomness = 100

func main() {
	rand.Seed(time.Now().UnixNano())
	fmt.Println(rand.Intn(20))
	fmt.Println(rand.Intn(20))
	fmt.Println(rand.Intn(20))
	randomAddition := time.Duration(rand.Intn(electionTimeoutRandomness+1)-electionTimeoutRandomness/2) * time.Millisecond
	interval := electionTimeout + randomAddition
	println(interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			println(time.Now().String())
		}
	}
}
