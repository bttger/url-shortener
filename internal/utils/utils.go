package utils

import (
	"fmt"
	"os"
	"time"
)

func Logf(format string, args ...interface{}) {
	currentTime := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Printf(currentTime+" ["+os.Getenv("NODE_ID")+"] "+format+"\n", args...)
}

func IntMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
