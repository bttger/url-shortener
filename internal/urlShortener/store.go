package urlShortener

import (
	"github.com/bttger/url-shortener/pkg/raft"
	"github.com/bttger/url-shortener/pkg/utils"
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
	Url    string
	Nanoid string
}

type AddUrlResult struct {
	success bool
}

// AddURL adds a new URL mapping to the store and returns the nanoid.
func (s *URLStore) AddURL(request AddUrlCommand) string {
	s.Lock()
	defer s.Unlock()
	s.urls[request.Nanoid] = request.Url
	return request.Nanoid
}

// ListenToNewCommits listens to newly committed fsmCommands and applies them to the finite-state machine.
func (s *URLStore) ListenToNewCommits(commitChan chan raft.Commit) {
	utils.Logf("URLStore: start listening to new committed commands")
	for {
		commit := <-commitChan
		cmd := commit.FsmCommand.(AddUrlCommand)
		utils.Logf("FSM: Received new committed command: %v", cmd)
		s.AddURL(cmd)
		utils.Logf("FSM: Added new URL mapping: %s -> %s", cmd.Url, cmd.Nanoid)
		commit.ResultChan <- AddUrlResult{success: true}
	}
}
