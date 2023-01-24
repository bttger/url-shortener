package urlShortener

import (
	gonanoid "github.com/matoous/go-nanoid"
	"sync"
)

type URLStore struct {
	sync.Mutex
	// Map from nanoid to long URL
	urls map[string]string
}

func NewURLStore() *URLStore {
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
