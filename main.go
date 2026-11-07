package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxClients      = 10
	maxHeaderBytes  = 64 << 10
	requestTimeout  = 30 * time.Second
	responseTimeout = 30 * time.Second
)

var contentTypes = map[string]string{
	".html": "text/html",
	".txt":  "text/plain",
	".gif":  "image/gif",
	".jpeg": "image/jpeg",
	".jpg":  "image/jpeg",
	".css":  "text/css",
}

type server struct {
	root string
	// Readers hold this lock until their file is closed. In particular, Windows
	// must not replace an open file while another request is streaming it.
	files sync.RWMutex
}

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	// Input parsing
	if len(args) < 1 || len(args) > 2 {
		return errors.New("usage: http_server <port> [document-root]")
	}
	port, err := strconv.Atoi(args[0])
	if err != nil || port < 1 || port > 65535 {
		return errors.New("port must be a number between 1 and 65535")
	}
	root := "."
	if len(args) == 2 {
		root = args[1]
	}

	// Creating server object
	s, err := newServer(root)
	if err != nil {
		return fmt.Errorf("document root: %w", err)
	}

	// Creating TCP listener on host with specified port
	listener, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	// Closing listener after all o
	defer listener.Close()
	log.Printf("Serving %s on %s (at most %d clients)", s.root, listener.Addr(), maxClients)

	// Start working
	return s.serve(listener)
}

func newServer(root string) (*server, error) {
	// Function parsing root path and creating server object

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
	return &server{root: resolved}, nil
}

// Idk if I like the sync part
func (s *server) serve(listener net.Listener) error {
	slots := make(chan struct{}, maxClients)	// Create channel with 3 slots
	var workers sync.WaitGroup // WaitGroup is used for waiting for several goroutines to finish working
	defer workers.Wait() // method that waits for the goroutines to finish
	for { // For every incoming TCP connection
		conn, err := listener.Accept() // Accept TCP connection
		if err != nil {
			if errors.Is(err, net.ErrClosed) { // If connection closed don't return error
				return nil
			}
			return err
		}
		// Acquire before spawning: excess clients wait without extra goroutines.
		slots <- struct{}{}	// synchronization with channels, if there are no free slots goroutine is frozen
		workers.Add(1)	// Adding 1 to WaitGroup counter, if counter is 0 then all goroutines are released
		go func() {
			defer workers.Done() // Same as WaitGroup.Add(-1)
			defer func() { <-slots }()	// reading from channel, freeing the spot

			s.handle(conn) // call file operation logic
		}()
	}
}

func (s *server) handle(conn net.Conn) {
	defer conn.Close() // Closing connection after everything
	_ = conn.SetReadDeadline(time.Now().Add(requestTimeout))	// Timeout for stale connections
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
	filename, contentType, err := s.resolvePath(req)
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

// Buffer only the headers, then replay them into Go's HTTP parser. Limiting the
// header separately leaves bodies free to stream to disk without a size cap.
func readRequest(reader *bufio.Reader) (*http.Request, error) {
	var header bytes.Buffer
	lineStart := 0
	for {
		fragment, err := reader.ReadSlice('\n') // Reading one line

		// header size check
		if header.Len()+len(fragment) > maxHeaderBytes {
			return nil, errors.New("headers too large")
		}
		header.Write(fragment)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return nil, err
		}
		line := header.Bytes()[lineStart:]
		if len(line) < 2 || !bytes.HasSuffix(line, []byte("\r\n")) {
			return nil, errors.New("headers require CRLF line endings")
		}
		if len(line) == 2 {
			break
		}
		// ReadRequest is permissive about spaces in header names. Validate the
		// raw field name before it is canonicalized by the parser.
		if lineStart != 0 && line[0] != ' ' && line[0] != '\t' {
			colon := bytes.IndexByte(line, ':')
			if colon < 1 || !validToken(line[:colon]) {
				return nil, errors.New("invalid header name")
			}
		}
		lineStart = header.Len()
	}
	return http.ReadRequest(bufio.NewReader(io.MultiReader(bytes.NewReader(header.Bytes()), reader)))
}

func validToken(token []byte) bool {
	for _, b := range token {
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' ||
			strings.IndexByte("!#$%&'*+-.^_`|~", b) >= 0) {
			return false
		}
	}
	return true
}

func (s *server) resolvePath(req *http.Request) (string, string, error) {
	urlPath := req.URL.Path // Already percent-decoded by http.ReadRequest.
	if !strings.HasPrefix(req.RequestURI, "/") || !strings.HasPrefix(urlPath, "/") ||
		strings.ContainsAny(urlPath, "\\\x00:") || req.URL.Fragment != "" {
		return "", "", errors.New("invalid path")
	}
	for _, part := range strings.Split(urlPath, "/") {
		if part == ".." || strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
			return "", "", errors.New("invalid path component")
		}
	}
	contentType, ok := contentTypes[strings.ToLower(path.Ext(urlPath))]
	if !ok {
		return "", "", errors.New("unsupported extension")
	}
	relative := filepath.FromSlash(strings.TrimPrefix(path.Clean(urlPath), "/"))
	if !filepath.IsLocal(relative) {
		return "", "", errors.New("path must stay inside document root")
	}
	return filepath.Join(s.root, relative), contentType, nil
}

// Check existing symlinks as well as the nearest existing ancestor of a new
// upload. Requests must not escape the document root through a symlink.
func (s *server) checkPath(filename string) error {
	for candidate := filename; ; candidate = filepath.Dir(candidate) {
		resolved, err := filepath.EvalSymlinks(candidate)
		if err == nil {
			relative, err := filepath.Rel(s.root, resolved)
			if err != nil || (relative != "." && !filepath.IsLocal(relative)) {
				return errors.New("path escapes document root")
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if candidate == s.root {
			return err
		}
	}
}

func (s *server) get(conn net.Conn, filename, contentType string) {
	s.files.RLock()
	defer s.files.RUnlock()
	if err := s.checkPath(filename); err != nil {
		writeError(conn, 400)
		return
	}
	info, err := os.Stat(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeError(conn, 404)
		} else {
			writeError(conn, 400)
		}
		return
	}
	if !info.Mode().IsRegular() {
		writeError(conn, 400)
		return
	}
	file, err := os.Open(filename)
	if err != nil {
		writeError(conn, 400)
		return
	}
	defer file.Close()
	writeResponse(conn, 200, contentType, info.Size(), file)
}

func (s *server) post(conn net.Conn, req *http.Request, filename string) {
	if err := s.checkPath(filename); err != nil {
		writeError(conn, 400)
		return
	}
	// Stage alongside the destination so rename stays on the same filesystem.
	// Parent directories must already exist. Temporary names have no permitted
	// extension, so they cannot be retrieved through GET.
	temp, err := os.CreateTemp(filepath.Dir(filename), ".http-upload-*")
	if err != nil {
		writeError(conn, 400)
		return
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	if expect := req.Header.Get("Expect"); expect != "" {
		if !strings.EqualFold(expect, "100-continue") || req.ProtoMinor != 1 {
			writeError(conn, 400)
			return
		}
		_ = conn.SetWriteDeadline(time.Now().Add(responseTimeout))
		if _, err := io.WriteString(conn, "HTTP/1.1 100 Continue\r\n\r\n"); err != nil {
			return
		}
	}
	if _, err := io.Copy(temp, req.Body); err != nil {
		writeError(conn, 400)
		return
	}
	if err := temp.Close(); err != nil {
		writeError(conn, 400)
		return
	}

	s.files.Lock()
	status := s.commit(temp.Name(), filename)
	s.files.Unlock()
	if status == 400 {
		writeError(conn, status)
		return
	}
	body := statusText(status) + "\n"
	writeResponse(conn, status, "text/plain", int64(len(body)), strings.NewReader(body))
}

// Called with the write lock held. Failed or incomplete uploads never touch the
// previous version. Serializing rename and GET also provides atomic visibility
// on platforms where os.Rename itself does not promise atomicity.
func (s *server) commit(temp, filename string) int {
	if err := s.checkPath(filename); err != nil {
		return 400
	}
	status := 201
	info, err := os.Lstat(filename)
	if err == nil {
		if !info.Mode().IsRegular() {
			return 400
		}
		status = 200
	} else if !errors.Is(err, os.ErrNotExist) {
		return 400
	}
	if err := os.Rename(temp, filename); err != nil {
		return 400
	}
	return status
}

func statusText(status int) string {
	switch status {
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 404:
		return "Not Found"
	case 501:
		return "Not Implemented"
	default:
		return "Bad Request"
	}
}

func writeError(conn net.Conn, status int) {
	body := fmt.Sprintf("%d %s\n", status, statusText(status))
	writeResponse(conn, status, "text/plain", int64(len(body)), strings.NewReader(body))
}

func writeResponse(conn net.Conn, status int, contentType string, length int64, body io.Reader) {
	_ = conn.SetWriteDeadline(time.Now().Add(responseTimeout))
	// Networking and response serialization use net/io, never http.Serve or a
	// net/http server. Content-Length and CRLF framing work for binary files.
	writer := bufio.NewWriter(conn)
	_, err := fmt.Fprintf(writer, "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n",
		status, statusText(status), contentType, length)
	if err == nil {
		_, err = io.CopyN(writer, body, length)
	}
	if err == nil {
		_ = writer.Flush()
	}
}
