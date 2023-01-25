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
	// respChannel is a channel that the FSM will send the response to after the command has been applied.
	respChannel chan interface{}
}

func NewFSMCommand(command interface{}, respChannel chan interface{}) *FSMCommand {
	return &FSMCommand{
		mut:             sync.Mutex{},
		command:         command,
		clientConnected: true,
		respChannel:     respChannel,
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

// Reply sends the response to the client iff the client is still connected.
func (c *FSMCommand) Reply(response interface{}) {
	c.mut.Lock()
	defer c.mut.Unlock()
	if c.clientConnected {
		c.respChannel <- response
		utils.Logf("Sent response to client: %s", response)
	}
}
