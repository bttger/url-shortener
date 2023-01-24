package urlShortener

import (
	"github.com/bttger/url-shortener/internal/raft"
	"github.com/bttger/url-shortener/internal/utils"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// PostTimeout is the timeout in seconds to wait for a leader reply on write requests
	PostTimeout = 5
)

type Server struct {
	store *URLStore
	rn    *raft.RaftNode
}

func Start(port int, store *URLStore, raftNode *raft.RaftNode) error {
	s := &Server{
		store: store,
		rn:    raftNode,
	}
	http.HandleFunc("/", s.get)
	http.HandleFunc("/add", s.post)
	err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	nanoid := strings.Split(r.URL.Path, "/")[1]
	url, ok := s.store.GetURL(nanoid)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	http.Redirect(w, r, url, http.StatusFound)
}

func (s *Server) post(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		url := string(body)
		respChannel := make(chan string)
		fsmInputReq := &FSMInputRequest{
			Mutex:       sync.Mutex{},
			Url:         url,
			TimedOut:    false,
			RespChannel: respChannel,
		}
		s.rn.Submit(fsmInputReq)

		// Wait for the response from the leader or timeout after PostTimeout seconds
		select {
		case res := <-respChannel:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(res))
			if err != nil {
				utils.Logf("Error writing response: %v", err)
			}
		case <-time.After(PostTimeout * time.Second):
			fsmInputReq.Lock()
			fsmInputReq.TimedOut = true
			fsmInputReq.Unlock()
			w.WriteHeader(http.StatusRequestTimeout)
			utils.Logf("Timeout on POST request for url %s", url)
		}
	}
}
