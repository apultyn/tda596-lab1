package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	maxHeaderBytes  = 64 << 10
	responseTimeout = 30 * time.Second
)

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

func acceptBody(conn net.Conn, req *http.Request) bool {
	expect := req.Header.Get("Expect")
	if expect == "" {
		return true
	}
	if !strings.EqualFold(expect, "100-continue") || req.ProtoMinor != 1 {
		writeError(conn, 400)
		return false
	}
	_ = conn.SetWriteDeadline(time.Now().Add(responseTimeout))
	_, err := io.WriteString(conn, "HTTP/1.1 100 Continue\r\n\r\n")
	return err == nil
}

func statusText(status int) string {
	switch status {
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 204:
		return "No Content"
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

func writeResponse(conn net.Conn, status int, contentType string, length int64, body io.Reader, headers ...string) {
	_ = conn.SetWriteDeadline(time.Now().Add(responseTimeout))
	// Networking and response serialization use net/io, never http.Serve or a
	// net/http server. Content-Length and CRLF framing work for binary files.
	writer := bufio.NewWriter(conn)
	fmt.Fprintf(writer, "HTTP/1.1 %d %s\r\nConnection: close\r\n", status, statusText(status))
	if status != 204 {
		fmt.Fprintf(writer, "Content-Type: %s\r\nContent-Length: %d\r\n", contentType, length)
	}
	for _, header := range headers {
		fmt.Fprint(writer, header, "\r\n")
	}
	fmt.Fprint(writer, "\r\n")
	if length > 0 {
		if _, err := io.CopyN(writer, body, length); err != nil {
			return
		}
	}
	_ = writer.Flush()
}
