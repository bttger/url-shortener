package urlShortener

import (
	"github.com/bttger/url-shortener/internal/raft"
	"github.com/bttger/url-shortener/internal/utils"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// PostTimeout is the timeout in seconds to wait for a leader reply on write requests
	PostTimeout = 5
)

type Server struct {
	store *URLStore
	rn    *raft.Node
}

func Start(port int, store *URLStore, raftNode *raft.Node) {
	s := &Server{
		store: store,
		rn:    raftNode,
	}
	http.HandleFunc("/", s.get)
	http.HandleFunc("/add", s.post)
	portStr := strconv.Itoa(port)
	utils.Logf("Starting HTTP server on port %s", portStr)
	log.Fatal(http.ListenAndServe(":"+portStr, nil))
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
		respChannel := make(chan interface{})
		fsmInput := raft.NewFSMInput(url, respChannel)
		s.rn.Submit(fsmInput)

		// Wait for the response from the leader or timeout after PostTimeout seconds
		select {
		case res := <-respChannel:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(res.(string)))
			if err != nil {
				utils.Logf("Error writing response: %v", err)
			}
		case <-time.After(PostTimeout * time.Second):
			fsmInput.SetClientConnected(false)
			w.WriteHeader(http.StatusRequestTimeout)
			utils.Logf("Timeout on POST request for url %s", url)
		}
	}
}
