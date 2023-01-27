package urlShortener

import (
	"encoding/json"
	"github.com/bttger/url-shortener/internal/raft"
	"github.com/bttger/url-shortener/internal/utils"
	gonanoid "github.com/matoous/go-nanoid"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// PostTimeout is the timeout to wait for a leader reply on write requests
	PostTimeout = 5 * time.Second
)

type Server struct {
	store *URLStore
	rn    *raft.Node
}

type FoundLeader struct {
	Message  string `json:"message"`
	LeaderId int    `json:"leaderId"`
}

type ShortenedUrl struct {
	Url         string `json:"url"`
	RedirectsTo string `json:"redirectsTo"`
}

func Start(port int, store *URLStore, raftNode *raft.Node) {
	s := &Server{
		store: store,
		rn:    raftNode,
	}
	http.HandleFunc("/", s.handle)
	portStr := strconv.Itoa(port)
	utils.Logf("Starting HTTP server on port %s", portStr)
	log.Fatal(http.ListenAndServe(":"+portStr, nil))
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Generate a random ID for the URL which is not already in use
		nanoid, ok := "", true
		for ok {
			nanoid = gonanoid.MustGenerate("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", 6)
			_, ok = s.store.GetURL(nanoid)
		}
		resultChan := make(chan interface{})
		addUrlCommand := AddUrlCommand{
			Url:    string(body),
			Nanoid: nanoid,
		}

		// Create a new FSM command and submit it to the Raft node
		clientRequest := raft.NewClientRequest(addUrlCommand, resultChan)
		err = s.rn.Submit(clientRequest)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			jsonData, err := json.Marshal(FoundLeader{Message: "Not a leader node", LeaderId: s.rn.GetLeaderId()})
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, err = w.Write(jsonData)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			return
		}

		// Wait for the result from the leader or timeout after PostTimeout seconds
		select {
		case res := <-resultChan:
			result := res.(AddUrlResult)
			if result.success {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				jsonData, err := json.Marshal(ShortenedUrl{Url: "http://" + r.Host + "/" + addUrlCommand.Nanoid, RedirectsTo: addUrlCommand.Url})
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_, err = w.Write(jsonData)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		case <-time.After(PostTimeout):
			clientRequest.SetClientConnected(false)
			w.WriteHeader(http.StatusRequestTimeout)
			utils.Logf("Timeout on POST request for url %s", addUrlCommand.Url)
		}
	} else if r.Method == "GET" {
		nanoid := strings.Split(r.URL.Path, "/")[1]
		url, ok := s.store.GetURL(nanoid)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		http.Redirect(w, r, url, http.StatusFound)
	}
}
