package server

import (
	"bufio"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	maxClients     = 10
	requestTimeout = 30 * time.Second
)

// Server handles file and pub-sub requests on a TCP listener.
type Server struct {
	root        string
	broker      broker
	pollTimeout time.Duration
	// Readers hold this lock until their file is closed. In particular, Windows
	// must not replace an open file while another request is streaming it.
	files sync.RWMutex
}

// New creates a server for root. A nonpositive pollTimeout uses the default.
func New(root string, pollTimeout time.Duration) (*Server, error) {
	// Deprecated note: Function parsing root path and creating server object

	// Returning full path or joining with project's path if relative
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	// Cleaning up relative pathing
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}

	// Get info about directory
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, err
	}

	// Check if really directory
	if !info.IsDir() {
		return nil, errors.New("must be a directory")
	}
	if pollTimeout <= 0 {
		pollTimeout = defaultPollTimeout
	}
	return &Server{
		root:        resolved,
		broker:      broker{topics: make(map[string]*topic)},
		pollTimeout: pollTimeout,
	}, nil
}

// Serve accepts TCP connections and runs their handlers.
//
// Deprecated note: Idk if I like the sync part
func (s *Server) Serve(listener net.Listener) error {
	// Deprecated note: Create channel with 3 slots
	// maxClients sets the number of active handler slots.
	slots := make(chan struct{}, maxClients)
	stopping := make(chan struct{})
	var workers sync.WaitGroup // WaitGroup is used for waiting for several goroutines to finish working
	defer workers.Wait()       // method that waits for the goroutines to finish
	defer close(stopping)      // Wake idle polls before waiting for workers to finish.
	for {                      // For every incoming TCP connection
		conn, err := listener.Accept() // Accept TCP connection
		if err != nil {
			// Deprecated note: If connection closed don't return error
			// net.ErrClosed here refers to the listener.
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		// Acquire before spawning: excess clients wait without extra goroutines.
		slots <- struct{}{} // synchronization with channels, if there are no free slots goroutine is frozen
		// Deprecated note: Adding 1 to WaitGroup counter, if counter is 0 then all goroutines are released
		// A zero counter unblocks callers of Wait.
		workers.Add(1)
		go func() {
			defer workers.Done()       // Same as WaitGroup.Add(-1)
			defer func() { <-slots }() // reading from channel, freeing the spot

			// Deprecated note: call file operation logic
			// The handler now routes broker and file requests.
			s.handle(conn, slots, stopping)
		}()
	}
}

func (s *Server) handle(conn net.Conn, slots chan struct{}, stopping <-chan struct{}) {
	defer conn.Close()                                       // Closing connection after everything
	_ = conn.SetReadDeadline(time.Now().Add(requestTimeout)) // Timeout for stale connections
	req, err := readRequest(bufio.NewReader(conn))
	if err != nil {
		writeError(conn, 400)
		return
	}
	// Closing the connection releases the request body without draining an
	// untrusted body on rejected requests. Each connection serves one request.
	if req.ProtoMajor != 1 || (req.ProtoMinor != 0 && req.ProtoMinor != 1) ||
		(req.ProtoMinor == 1 && req.Host == "") || strings.ContainsAny(req.Host, " \t\r\n") {
		writeError(conn, 400)
		return
	}
	if req.Method != "GET" && req.Method != "POST" {
		writeError(conn, 501)
		return
	}
	if !strings.HasPrefix(req.RequestURI, "/") || !strings.HasPrefix(req.URL.Path, "/") || req.URL.Fragment != "" {
		writeError(conn, 400)
		return
	}
	if s.handleBroker(conn, req, slots, stopping) {
		return
	}
	// Deprecated note for the old urlPath assignment: Already percent-decoded by http.ReadRequest.
	filename, contentType, err := s.resolvePath(req.URL.Path)
	if err != nil {
		writeError(conn, 400)
		return
	}
	if req.Method == "GET" {
		s.get(conn, filename, contentType)
	} else {
		s.post(conn, req, filename)
	}
}
