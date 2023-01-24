package urlShortener

import (
	"github.com/bttger/url-shortener/internal/raft"
	"github.com/bttger/url-shortener/internal/utils"
	gonanoid "github.com/matoous/go-nanoid"
	"sync"
)

// URLStore represents the state machine that stores the URL mappings.
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

func (s *URLStore) AddURL(url string) string {
	s.Lock()
	defer s.Unlock()
	nanoid, ok := "", true
	for ok {
		nanoid = gonanoid.MustGenerate("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", 6)
		_, ok = s.urls[nanoid]
	}
	s.urls[nanoid] = url
	return nanoid
}

func (s *URLStore) ListenToNewCommits(commitChan chan *raft.FSMInput) {
	utils.Logf("URLStore: start listening to new commits")
	for {
		fsmInput := <-commitChan
		utils.Logf("Received new committed input: %s", fsmInput.GetInput())
		url := fsmInput.GetInput()
		nanoid := s.AddURL(url.(string))
		utils.Logf("Added new URL mapping: %s -> %s", nanoid, url)
		fsmInput.Reply(nanoid)
	}
}
