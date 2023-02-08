package utils

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func Logf(format string, args ...interface{}) {
	if os.Getenv("DEBUG") == "true" {
		currentTime := time.Now().Format("2006-01-02 15:04:05.000")
		nodeId, err := strconv.Atoi(os.Getenv("NODE_ID"))
		if err != nil {
			panic(err)
		}
		indent := ""
		if nodeId > 1 {
			indent = strings.Repeat(" ", (nodeId-1)*4)
		}
		fmt.Printf(currentTime+indent+" ["+os.Getenv("NODE_ID")+"] "+format+"\n", args...)
	}
}

func IntMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
