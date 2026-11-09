package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
)

// Run starts the server with the port and optional document root from args.
func Run(args []string) error {
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

	// Deprecated note: Creating server object
	s, err := New(root, 0)
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
	log.Printf("Serving %s on %s (at most %d active handlers)", s.root, listener.Addr(), maxClients)

	// Deprecated note: Start working
	return s.Serve(listener)
}
