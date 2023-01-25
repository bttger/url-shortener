package raft

import (
	"github.com/bttger/url-shortener/internal/utils"
	"sync"
)

// FSMCommand represents a client request to apply some command to the finite-state machine.
type FSMCommand struct {
	mut             sync.Mutex
	command         interface{}
	clientConnected bool
	// resultChan is a channel that the FSM will send the response to after the command has been applied.
	resultChan chan interface{}
}

func NewFSMCommand(command interface{}, resultChan chan interface{}) *FSMCommand {
	return &FSMCommand{
		mut:             sync.Mutex{},
		command:         command,
		clientConnected: true,
		resultChan:      resultChan,
	}
}

func (c *FSMCommand) GetCommand() interface{} {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.command
}

func (c *FSMCommand) SetCommand(command interface{}) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.command = command
}

func (c *FSMCommand) SetClientConnected(v bool) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.clientConnected = v
}

// Reply sends the command result to the client iff the client is still connected.
func (c *FSMCommand) Reply(result interface{}) {
	c.mut.Lock()
	defer c.mut.Unlock()
	if c.clientConnected {
		c.resultChan <- result
		utils.Logf("FSMCommand: Sent result to client: %v", result)
	}
}
