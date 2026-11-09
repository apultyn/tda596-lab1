package server

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	subscriberQueueSize = 32
	defaultPollTimeout  = 15 * time.Second
)

type message struct {
	sequence uint64
	body     []byte
}

type subscriber struct {
	queue   chan message
	done    chan struct{}
	polling bool // Protected by broker.mu.
}

type topic struct {
	sequence    uint64
	subscribers map[uint64]*subscriber
}

type broker struct {
	mu     sync.Mutex
	topics map[string]*topic
	nextID uint64
}

// topicLocked is called with mu held. Keep a topic's sequence across changes
// to its subscribers, including when the last subscriber leaves.
func (b *broker) topicLocked(name string) *topic {
	t := b.topics[name]
	if t == nil {
		t = &topic{subscribers: make(map[uint64]*subscriber)}
		b.topics[name] = t
	}
	return t
}

func (b *broker) subscribe(name string) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	t := b.topicLocked(name)
	b.nextID++
	t.subscribers[b.nextID] = &subscriber{
		queue: make(chan message, subscriberQueueSize),
		done:  make(chan struct{}),
	}
	return b.nextID
}

func (b *broker) unsubscribe(name string, id uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	t := b.topics[name]
	if t == nil || t.subscribers[id] == nil {
		return false
	}
	close(t.subscribers[id].done)
	delete(t.subscribers, id)
	return true
}

func (b *broker) publish(name string, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	t := b.topicLocked(name)
	t.sequence++
	// Share the immutable payload. Holding mu through fan-out keeps enqueue
	// order identical to sequence order for concurrent publishers.
	msg := message{sequence: t.sequence, body: body}
	for _, sub := range t.subscribers {
		select {
		case sub.queue <- msg:
		default: // Skip only this subscriber if its queue is full.
		}
	}
}

func (b *broker) beginPoll(name string, id uint64) (*subscriber, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	t := b.topics[name]
	if t == nil || t.subscribers[id] == nil {
		return nil, 404
	}
	sub := t.subscribers[id]
	if sub.polling {
		return nil, 400
	}
	sub.polling = true
	return sub, 200
}

func (b *broker) endPoll(sub *subscriber) {
	b.mu.Lock()
	sub.polling = false
	b.mu.Unlock()
}

// handleBroker reports whether the request matched a broker route.
func (s *Server) handleBroker(conn net.Conn, req *http.Request, slots chan struct{}, stopping <-chan struct{}) bool {
	route := req.URL.Path
	var method string
	switch route {
	case "/subscribe", "/unsubscribe", "/publish":
		method = "POST"
	case "/events":
		method = "GET"
	default:
		return false
	}
	if req.Method != method {
		writeError(conn, 501)
		return true
	}
	query, err := url.ParseQuery(req.URL.RawQuery)
	names := query["topic"]
	if err != nil || len(names) != 1 || strings.TrimSpace(names[0]) == "" {
		writeError(conn, 400)
		return true
	}
	name := names[0]
	var id uint64
	if route == "/unsubscribe" || route == "/events" {
		if len(query["id"]) != 1 {
			writeError(conn, 400)
			return true
		}
		id, err = strconv.ParseUint(query.Get("id"), 10, 64)
		if err != nil || id == 0 {
			writeError(conn, 400)
			return true
		}
	}

	switch route {
	case "/subscribe":
		body := fmt.Sprintf(`{"id":%d}`, s.broker.subscribe(name))
		writeResponse(conn, 201, "application/json", int64(len(body)), strings.NewReader(body))
	case "/unsubscribe":
		if s.broker.unsubscribe(name, id) {
			writeResponse(conn, 204, "", 0, nil)
		} else {
			writeError(conn, 404)
		}
	case "/publish":
		if !acceptBody(conn, req) {
			break
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			writeError(conn, 400)
			break
		}
		s.broker.publish(name, body)
		writeResponse(conn, 200, "text/plain", 0, nil)
	case "/events":
		s.events(conn, name, id, slots, stopping)
	}
	return true
}

func (s *Server) events(conn net.Conn, name string, id uint64, slots chan struct{}, stopping <-chan struct{}) {
	sub, status := s.broker.beginPoll(name, id)
	if sub == nil {
		writeError(conn, status)
		return
	}
	defer s.broker.endPoll(sub)
	msg, received := waitForMessage(sub, s.pollTimeout, slots, stopping)
	select {
	case <-sub.done:
		writeError(conn, 404)
		return
	default:
	}
	if !received {
		writeResponse(conn, 204, "", 0, nil)
		return
	}
	sequenceHeader := "X-Sequence-Number: " + strconv.FormatUint(msg.sequence, 10)
	writeResponse(conn, 200, "application/octet-stream", int64(len(msg.body)), bytes.NewReader(msg.body), sequenceHeader)
}

func waitForMessage(sub *subscriber, timeout time.Duration, slots chan struct{}, stopping <-chan struct{}) (message, bool) {
	select {
	case msg := <-sub.queue:
		return msg, true
	default:
	}

	// A waiting poll owns no slot. Reacquire one before returning so that
	// responses still count toward the active handler limit.
	<-slots
	defer func() { slots <- struct{}{} }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg := <-sub.queue:
		return msg, true
	case <-sub.done:
	case <-stopping:
	case <-timer.C:
	}
	return message{}, false
}
