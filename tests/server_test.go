package tests

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"http_server/src/server"
)

type testServer struct {
	root string
	addr string
}

func startTestServer(t *testing.T) testServer {
	t.Helper()
	return startTestServerWithPollTimeout(t, 0)
}

func startTestServerWithPollTimeout(t *testing.T, timeout time.Duration) testServer {
	t.Helper()
	root := t.TempDir()
	s, err := server.New(root, timeout)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	t.Cleanup(func() {
		listener.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})
	return testServer{root: root, addr: listener.Addr().String()}
}

func exchange(addr, raw string) (*http.Response, []byte, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, raw); err != nil {
		return nil, nil, err
	}
	// A write-side EOF also lets malformed/truncated request tests finish.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
	return readResponse(bufio.NewReader(conn))
}

func readResponse(reader *bufio.Reader) (*http.Response, []byte, error) {
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return response, body, err
}

func assertResponse(t *testing.T, addr, raw string, wantStatus int, wantType string, wantBody []byte) {
	t.Helper()
	response, body, err := exchange(addr, raw)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("status = %s, want %d; body = %q", response.Status, wantStatus, body)
	}
	if got := response.Header.Get("Content-Type"); got != wantType {
		t.Errorf("Content-Type = %q, want %q", got, wantType)
	}
	if response.ContentLength != int64(len(body)) {
		t.Errorf("Content-Length = %d, received %d bytes", response.ContentLength, len(body))
	}
	if !response.Close {
		t.Error("response did not advertise Connection: close")
	}
	if wantBody != nil && !bytes.Equal(body, wantBody) {
		t.Errorf("body = %q, want %q", body, wantBody)
	}
}

func postRequest(filename string, body []byte) string {
	return fmt.Sprintf("POST /%s HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\n\r\n%s", filename, len(body), body)
}

func getRequest(filename string) string {
	return "GET /" + filename + " HTTP/1.1\r\nHost: localhost\r\n\r\n"
}

func TestGetContentTypesAndBinaryBytes(t *testing.T) {
	s := startTestServer(t)
	for _, test := range []struct{ extension, contentType string }{
		{"html", "text/html"}, {"txt", "text/plain"}, {"gif", "image/gif"},
		{"jpeg", "image/jpeg"}, {"jpg", "image/jpeg"}, {"css", "text/css"},
		{"JPG", "image/jpeg"},
	} {
		t.Run(test.extension, func(t *testing.T) {
			body := []byte{'a', '\r', '\n', 0, 0xff, 'z'}
			filename := "file." + test.extension
			if err := os.WriteFile(filepath.Join(s.root, filename), body, 0600); err != nil {
				t.Fatal(err)
			}
			assertResponse(t, s.addr, getRequest(filename+"?ignored=yes"), 200, test.contentType, body)
		})
	}
}

func TestErrors(t *testing.T) {
	s := startTestServer(t)
	if err := os.Mkdir(filepath.Join(s.root, "directory.txt"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, raw string
		status    int
	}{
		{"missing file", getRequest("missing.txt"), 404},
		{"missing directory", getRequest("absent/file.txt"), 404},
		{"unsupported extension", getRequest("image.png"), 400},
		{"missing extension", getRequest("file"), 400},
		{"directory", getRequest("directory.txt"), 400},
		{"invalid request line", "not HTTP\r\n\r\n", 400},
		{"invalid header", "GET /a.txt HTTP/1.1\r\nHost: localhost\r\nBroken header\r\n\r\n", 400},
		{"invalid header name", "GET /a.txt HTTP/1.1\r\nHost: localhost\r\nBad Header: value\r\n\r\n", 400},
		{"whitespace before colon", "GET /a.txt HTTP/1.1\r\nHost : localhost\r\n\r\n", 400},
		{"missing host", "GET /a.txt HTTP/1.1\r\n\r\n", 400},
		{"duplicate host", "GET /a.txt HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n", 400},
		{"invalid host", "GET /a.txt HTTP/1.1\r\nHost: bad host\r\n\r\n", 400},
		{"unsupported version", "GET /a.txt HTTP/2.0\r\nHost: localhost\r\n\r\n", 400},
		{"invalid content length", "POST /a.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: no\r\n\r\n", 400},
		{"conflicting lengths", "POST /a.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\nabc", 400},
		{"truncated headers", "GET /a.txt HTTP/1.1\r\nHost: localhost\r\n", 400},
		{"bare line feeds", "GET /a.txt HTTP/1.1\nHost: localhost\n\n", 400},
		{"large headers", "GET /a.txt HTTP/1.1\r\nHost: localhost\r\nX-Large: " + strings.Repeat("a", 64<<10) + "\r\n\r\n", 400},
		{"traversal", getRequest("../outside.txt"), 400},
		{"encoded traversal", getRequest("%2e%2e/outside.txt"), 400},
		{"backslash traversal", getRequest("..%5coutside.txt"), 400},
		{"nul", getRequest("bad%00.txt"), 400},
		{"alternate data stream", getRequest("file.txt:stream.txt"), 400},
		{"absolute target", "GET http://localhost/a.txt HTTP/1.1\r\nHost: localhost\r\n\r\n", 400},
		{"upload missing parent", postRequest("absent/a.txt", []byte("data")), 400},
		{"upload to directory", postRequest("directory.txt", []byte("data")), 400},
		{"upload forbidden extension", postRequest("a.png", []byte("data")), 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertResponse(t, s.addr, test.raw, test.status, "text/plain", nil)
		})
	}
	for _, method := range []string{"HEAD", "PUT", "DELETE", "OPTIONS", "PATCH", "CUSTOM"} {
		t.Run(method, func(t *testing.T) {
			assertResponse(t, s.addr, method+" /file.txt HTTP/1.1\r\nHost: localhost\r\n\r\n", 501, "text/plain", nil)
		})
	}
}

func TestPostCreateReplaceAndEmptyFile(t *testing.T) {
	s := startTestServer(t)
	for i, body := range [][]byte{[]byte("first version"), {0, 1, 0xff, '\r', '\n'}, {}} {
		status := 200
		if i == 0 {
			status = 201
		}
		assertResponse(t, s.addr, postRequest("saved.txt", body), status, "text/plain", nil)
		assertResponse(t, s.addr, getRequest("saved.txt"), 200, "text/plain", body)
	}
	assertNoTemporaryFiles(t, s.root)
}

func TestHTTP10AndNestedPaths(t *testing.T) {
	s := startTestServer(t)
	if err := os.Mkdir(filepath.Join(s.root, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	assertResponse(t, s.addr, "POST /nested/hello%20world.txt HTTP/1.0\r\nContent-Length: 5\r\n\r\nhello", 201, "text/plain", nil)
	assertResponse(t, s.addr, "GET /nested/hello%20world.txt HTTP/1.0\r\n\r\n", 200, "text/plain", []byte("hello"))
}

func TestChunkedUploadAndIncompleteUpload(t *testing.T) {
	s := startTestServer(t)
	assertResponse(t, s.addr, "POST /chunk.txt HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n", 201, "text/plain", nil)
	assertResponse(t, s.addr, getRequest("chunk.txt"), 200, "text/plain", []byte("abcde"))
	for _, raw := range []string{
		"POST /chunk.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 50\r\n\r\nincomplete",
		"POST /chunk.txt HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\nbad chunk\r\n",
	} {
		assertResponse(t, s.addr, raw, 400, "text/plain", nil)
		assertResponse(t, s.addr, getRequest("chunk.txt"), 200, "text/plain", []byte("abcde"))
	}
	assertNoTemporaryFiles(t, s.root)
}

func assertNoTemporaryFiles(t *testing.T, root string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(root, ".http-upload-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary uploads remain: %v (%v)", matches, err)
	}
}

func TestContinueUploadKeepsOldVersionVisibleUntilComplete(t *testing.T) {
	s := startTestServer(t)
	assertResponse(t, s.addr, postRequest("continue.txt", []byte("old")), 201, "text/plain", nil)
	conn, err := net.Dial("tcp", s.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = io.WriteString(conn, "POST /continue.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	interim, err := http.ReadResponse(reader, nil)
	if err != nil || interim.StatusCode != 100 {
		t.Fatalf("expected 100 Continue, got %v (%v)", interim, err)
	}
	if _, err := io.WriteString(conn, "hel"); err != nil {
		t.Fatal(err)
	}
	assertResponse(t, s.addr, getRequest("continue.txt"), 200, "text/plain", []byte("old"))
	if _, err := io.WriteString(conn, "lo"); err != nil {
		t.Fatal(err)
	}
	response, _, err := readResponse(reader)
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("upload response = %v (%v)", response, err)
	}
	assertResponse(t, s.addr, getRequest("continue.txt"), 200, "text/plain", []byte("hello"))
}

func TestConcurrentPostsAndGetsAreAtomic(t *testing.T) {
	s := startTestServer(t)
	versions := make([][]byte, 5)
	for i := range versions {
		versions[i] = bytes.Repeat([]byte{byte('A' + i)}, 128*1024+i)
	}
	assertResponse(t, s.addr, postRequest("shared.txt", versions[0]), 201, "text/plain", nil)
	var workers sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			<-start
			for iteration := 0; iteration < 12; iteration++ {
				if id%2 == 0 {
					response, _, err := exchange(s.addr, postRequest("shared.txt", versions[(id+iteration)%len(versions)]))
					if err != nil || response.StatusCode != 200 {
						t.Errorf("concurrent POST failed: %v (%v)", response, err)
						return
					}
				} else {
					response, body, err := exchange(s.addr, getRequest("shared.txt"))
					if err != nil || response.StatusCode != 200 {
						t.Errorf("concurrent GET failed: %v (%v)", response, err)
						return
					}
					complete := false
					for _, version := range versions {
						complete = complete || bytes.Equal(body, version)
					}
					if !complete {
						t.Errorf("GET returned a partial or mixed version (%d bytes)", len(body))
						return
					}
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	assertNoTemporaryFiles(t, s.root)
}

// Observe when a handler first reads a socket, without inspecting its semaphore.
type observedListener struct {
	net.Listener
	started chan struct{}
}

func (listener observedListener) Accept() (net.Conn, error) {
	conn, err := listener.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &observedConn{Conn: conn, started: listener.started}, nil
}

type observedConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (conn *observedConn) Read(buffer []byte) (int, error) {
	conn.once.Do(func() { conn.started <- struct{}{} })
	return conn.Conn.Read(buffer)
}

func TestAtMostTenActiveClients(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("hello"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := server.New(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 20)
	done := make(chan error, 1)
	go func() { done <- s.Serve(observedListener{Listener: listener, started: started}) }()
	clients := make([]net.Conn, 0, 11)
	defer func() {
		for _, conn := range clients {
			conn.Close()
		}
		listener.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	}()
	for i := 0; i < 11; i++ {
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, conn)
		if i < 10 {
			if _, err := io.WriteString(conn, "GET /file.txt HTTP/1.1\r\nHost: localhost\r\n"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not start")
			}
		} else if _, err := io.WriteString(conn, getRequest("file.txt")); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-started:
		t.Fatal("an eleventh handler started while ten were busy")
	case <-time.After(150 * time.Millisecond):
	}
	// Completing one request must immediately allow the queued client to run.
	if _, err := io.WriteString(clients[0], "\r\n"); err != nil {
		t.Fatal(err)
	}
	_ = clients[10].SetReadDeadline(time.Now().Add(5 * time.Second))
	response, body, err := readResponse(bufio.NewReader(clients[10]))
	if err != nil || response.StatusCode != 200 || string(body) != "hello" {
		t.Fatalf("queued request failed: %v, %q (%v)", response, body, err)
	}
}

func TestSymlinkCannotEscapeRoot(t *testing.T) {
	s := startTestServer(t)
	outside := t.TempDir()
	filename := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(filename, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(s.root, "escape")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	assertResponse(t, s.addr, getRequest("escape/secret.txt"), 400, "text/plain", nil)
	assertResponse(t, s.addr, postRequest("escape/secret.txt", []byte("changed")), 400, "text/plain", nil)
	body, err := os.ReadFile(filename)
	if err != nil || string(body) != "secret" {
		t.Fatalf("outside file changed: %q (%v)", body, err)
	}
}

func TestInvalidArguments(t *testing.T) {
	for _, args := range [][]string{nil, {"0"}, {"65536"}, {"-1"}, {"abc"}, {"8080", ".", "extra"}} {
		if err := server.Run(args); err == nil {
			t.Errorf("run(%q) should fail", args)
		}
	}
	if _, err := server.New(filepath.Join(t.TempDir(), "missing"), 0); err == nil {
		t.Error("nonexistent document root should fail")
	}
}
