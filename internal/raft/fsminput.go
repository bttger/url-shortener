package raft

import (
	"github.com/bttger/url-shortener/internal/utils"
	"sync"
)

// FSMInput represents a client request to apply some input to the URLStore state machine.
type FSMInput struct {
	mut             sync.Mutex
	input           interface{}
	clientConnected bool
	// respChannel is a channel that the FSM will send the response to after the input has been applied.
	respChannel chan interface{}
}

func NewFSMInput(input interface{}, respChannel chan interface{}) *FSMInput {
	return &FSMInput{
		mut:             sync.Mutex{},
		input:           input,
		clientConnected: true,
		respChannel:     respChannel,
	}
}

func (i *FSMInput) GetInput() interface{} {
	i.mut.Lock()
	defer i.mut.Unlock()
	return i.input
}

func (i *FSMInput) SetInput(input interface{}) {
	i.mut.Lock()
	defer i.mut.Unlock()
	i.input = input
}

func (i *FSMInput) SetClientConnected(v bool) {
	i.mut.Lock()
	defer i.mut.Unlock()
	i.clientConnected = v
}

// Reply sends the response to the client iff the client is still connected.
func (i *FSMInput) Reply(response interface{}) {
	i.mut.Lock()
	defer i.mut.Unlock()
	if i.clientConnected {
		i.respChannel <- response
		utils.Logf("Sent response to client: %s", response)
	}
}
