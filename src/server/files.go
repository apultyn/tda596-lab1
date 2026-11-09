package server

import (
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var contentTypes = map[string]string{
	".html": "text/html",
	".txt":  "text/plain",
	".gif":  "image/gif",
	".jpeg": "image/jpeg",
	".jpg":  "image/jpeg",
	".css":  "text/css",
}

func (s *Server) resolvePath(urlPath string) (string, string, error) {
	if strings.ContainsAny(urlPath, "\\\x00:") {
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
func (s *Server) checkPath(filename string) error {
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

func (s *Server) get(conn net.Conn, filename, contentType string) {
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

func (s *Server) post(conn net.Conn, req *http.Request, filename string) {
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
	if !acceptBody(conn, req) {
		return
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
func (s *Server) commit(temp, filename string) int {
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
