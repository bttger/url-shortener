package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println(os.Getenv("NODE_ID"), os.Getenv("CLUSTER_SIZE"))
}
