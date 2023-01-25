package urlShortener

import (
	"github.com/bttger/url-shortener/internal/raft"
	"github.com/bttger/url-shortener/internal/utils"
	"sync"
)

// URLStore represents the finite-state machine that stores the URL mappings.
type URLStore struct {
	sync.Mutex
	// Map from nanoid to long URL
	urls map[string]string
}

func NewURLStore() *URLStore {
	utils.Logf("Initializing URLStore")
	return &URLStore{
		urls: make(map[string]string),
	}
}

func (s *URLStore) GetURL(nanoid string) (string, bool) {
	s.Lock()
	defer s.Unlock()
	url, ok := s.urls[nanoid]
	return url, ok
}

type AddUrlCommand struct {
	url    string
	nanoid string
}

type AddUrlResult struct {
	success bool
}

// AddURL adds a new URL mapping to the store and returns the nanoid.
func (s *URLStore) AddURL(request AddUrlCommand) string {
	s.Lock()
	defer s.Unlock()
	s.urls[request.nanoid] = request.url
	return request.nanoid
}

func (s *URLStore) ListenToNewCommits(commitChan chan *raft.FSMCommand) {
	utils.Logf("URLStore: start listening to new commits")
	for {
		fsmCommand := <-commitChan
		utils.Logf("Received new committed command: %v", fsmCommand.GetCommand())
		command := fsmCommand.GetCommand().(AddUrlCommand)
		s.AddURL(command)
		utils.Logf("Added new URL mapping: %s -> %s", command.url, command.nanoid)
		fsmCommand.Reply(AddUrlResult{success: true})
	}
}
