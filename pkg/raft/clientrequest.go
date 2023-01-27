package raft

import (
	"github.com/bttger/url-shortener/pkg/utils"
	"sync"
)

// ClientRequest represents a client request to apply some FsmCommand to the finite-state machine.
type ClientRequest struct {
	mut             sync.Mutex
	fsmCommand      interface{}
	clientConnected bool
	// resultChan is a channel that the node will send the response to after the fsmCommand has been applied.
	resultChan chan interface{}
}

func NewClientRequest(command interface{}, resultChan chan interface{}) *ClientRequest {
	return &ClientRequest{
		mut:             sync.Mutex{},
		fsmCommand:      command,
		clientConnected: true,
		resultChan:      resultChan,
	}
}

func (c *ClientRequest) SetClientConnected(v bool) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.clientConnected = v
}

// Reply sends the FsmCommand result to the client iff the client is still connected.
func (c *ClientRequest) Reply(result interface{}) {
	c.mut.Lock()
	defer c.mut.Unlock()
	if c.clientConnected {
		c.resultChan <- result
		utils.Logf("ClientRequest: Sent result to client: %v", result)
	}
}
