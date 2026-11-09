package tests

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"
)

func brokerResponse(t *testing.T, s testServer, request string, status int) (*http.Response, []byte) {
	t.Helper()
	res, body, err := exchange(s.addr, request)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != status {
		t.Fatalf("response = %s, want %d, body = %q", res.Status, status, body)
	}
	if status == 204 && (len(body) != 0 || res.Header.Get("Content-Length") != "") {
		t.Fatal("204 must have no body or Content-Length")
	}
	return res, body
}

func subscribeForTest(t *testing.T, s testServer, topic string) uint64 {
	t.Helper()
	res, body := brokerResponse(t, s, postRequest("subscribe?topic="+topic, nil), 201)
	var result struct{ ID uint64 }
	if err := json.Unmarshal(body, &result); err != nil || result.ID == 0 || res.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("invalid subscription response: %q (%v)", body, err)
	}
	return result.ID
}

func subscriberTarget(route, topic string, id uint64) string {
	return fmt.Sprintf("%s?topic=%s&id=%d", route, topic, id)
}

func checkEvent(t *testing.T, res *http.Response, body []byte, sequence uint64, want []byte) {
	t.Helper()
	if res.StatusCode != 200 || res.Header.Get("X-Sequence-Number") != strconv.FormatUint(sequence, 10) ||
		res.Header.Get("Content-Type") != "application/octet-stream" || !bytes.Equal(body, want) {
		t.Fatalf("event = %s, headers %v, body %q, want sequence %d and body %q", res.Status, res.Header, body, sequence, want)
	}
}

func TestBrokerValidation(t *testing.T) {
	s := startTestServer(t)
	for _, test := range []struct {
		name    string
		request string
		status  int
	}{
		{"missing topic", postRequest("subscribe", nil), 400},
		{"blank topic", postRequest("publish?topic=%20", nil), 400},
		{"malformed query", postRequest("publish?topic=%xx", nil), 400},
		{"duplicate topic", postRequest("subscribe?topic=a&topic=b", nil), 400},
		{"missing id", getRequest("events?topic=a"), 400},
		{"zero id", getRequest("events?topic=a&id=0"), 400},
		{"invalid id", postRequest("unsubscribe?topic=a&id=-1", nil), 400},
		{"duplicate id", getRequest("events?topic=a&id=1&id=2"), 400},
		{"unknown subscriber", getRequest("events?topic=a&id=1"), 404},
		{"unknown unsubscribe", postRequest("unsubscribe?topic=a&id=1", nil), 404},
		{"GET on publish", getRequest("publish?topic=a"), 501},
		{"POST on events", postRequest("events?topic=a&id=1", nil), 501},
	} {
		t.Run(test.name, func(t *testing.T) {
			brokerResponse(t, s, test.request, test.status)
		})
	}
}

func TestBrokerFanOutAndPollTimeout(t *testing.T) {
	const timeout = 30 * time.Millisecond
	s := startTestServerWithPollTimeout(t, timeout)
	a := subscribeForTest(t, s, "news")
	b := subscribeForTest(t, s, "news")
	other := subscribeForTest(t, s, "other")
	if a == b || b == other || a == other {
		t.Fatal("duplicate subscriber IDs")
	}
	brokerResponse(t, s, "POST /publish?topic=news HTTP/1.1\r\nHost: localhost\r\nContent-Length: 12\r\n\r\nshort", 400)
	payload := []byte{0, 255, '\r', '\n', 'a'}
	brokerResponse(t, s, postRequest("publish?topic=news", payload), 200)
	late := subscribeForTest(t, s, "news")
	for _, id := range []uint64{a, b} {
		res, body := brokerResponse(t, s, getRequest(subscriberTarget("events", "news", id)), 200)
		checkEvent(t, res, body, 1, payload)
	}
	start := time.Now()
	brokerResponse(t, s, getRequest(subscriberTarget("events", "news", late)), 204)
	if time.Since(start) < timeout {
		t.Fatal("empty poll returned before the timeout")
	}
	brokerResponse(t, s, getRequest(subscriberTarget("events", "other", other)), 204)
	brokerResponse(t, s, getRequest(subscriberTarget("events", "other", a)), 404)
}

func TestBrokerConcurrentPublishOrder(t *testing.T) {
	s := startTestServer(t)
	a := subscriberTarget("events", "ordered", subscribeForTest(t, s, "ordered"))
	b := subscriberTarget("events", "ordered", subscribeForTest(t, s, "ordered"))
	const count = 16
	var publishers sync.WaitGroup
	for i := 0; i < count; i++ {
		publishers.Add(1)
		go func(i int) {
			defer publishers.Done()
			res, _, err := exchange(s.addr, postRequest("publish?topic=ordered", []byte(strconv.Itoa(i))))
			if err != nil || res.StatusCode != 200 {
				t.Errorf("publisher %d failed: %v (%v)", i, res, err)
			}
		}(i)
	}
	publishers.Wait()
	seen := make(map[string]bool)
	for sequence := uint64(1); sequence <= count; sequence++ {
		first, body := brokerResponse(t, s, getRequest(a), 200)
		checkEvent(t, first, body, sequence, body)
		second, otherBody := brokerResponse(t, s, getRequest(b), 200)
		checkEvent(t, second, otherBody, sequence, body)
		if seen[string(body)] {
			t.Fatalf("message %q delivered twice", body)
		}
		seen[string(body)] = true
	}
}

func TestBrokerFullQueueSkipsOnlySlowSubscriber(t *testing.T) {
	s := startTestServer(t)
	const subscriberQueueSize = 32
	slow := subscriberTarget("events", "full", subscribeForTest(t, s, "full"))
	for i := 0; i < subscriberQueueSize; i++ {
		brokerResponse(t, s, postRequest("publish?topic=full", []byte("queued")), 200)
	}
	fast := subscriberTarget("events", "full", subscribeForTest(t, s, "full"))
	brokerResponse(t, s, postRequest("publish?topic=full", []byte("skipped by slow")), 200)
	res, body := brokerResponse(t, s, getRequest(fast), 200)
	checkEvent(t, res, body, subscriberQueueSize+1, []byte("skipped by slow"))
	for sequence := uint64(1); sequence <= subscriberQueueSize; sequence++ {
		res, body := brokerResponse(t, s, getRequest(slow), 200)
		checkEvent(t, res, body, sequence, []byte("queued"))
	}
	brokerResponse(t, s, postRequest("publish?topic=full", []byte("next")), 200)
	res, body = brokerResponse(t, s, getRequest(slow), 200)
	checkEvent(t, res, body, subscriberQueueSize+2, []byte("next"))
}

func openPoll(t *testing.T, s testServer, target string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", s.addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, getRequest(target)); err != nil {
		t.Fatal(err)
	}
	return conn
}

type pollResult struct {
	response *http.Response
	body     []byte
	err      error
}

// One of two concurrent polls is rejected. That response confirms the other
// poll is outstanding without reaching into broker state or using a sleep.
func startWaitingPoll(t *testing.T, s testServer, target string) <-chan pollResult {
	t.Helper()
	results := make(chan pollResult, 2)
	for i := 0; i < 2; i++ {
		conn := openPoll(t, s, target)
		go func() {
			res, body, err := readResponse(bufio.NewReader(conn))
			results <- pollResult{response: res, body: body, err: err}
		}()
	}
	first := <-results
	if first.err != nil || first.response.StatusCode != 400 {
		t.Fatalf("overlapping poll response = %v (%v), want 400", first.response, first.err)
	}
	return results
}

func TestWaitingPollsLeaveCapacityForFileRequestsAndPublish(t *testing.T) {
	s := startTestServer(t)
	var polls []<-chan pollResult
	for i := 0; i < 12; i++ {
		id := subscribeForTest(t, s, "waiting")
		polls = append(polls, startWaitingPoll(t, s, subscriberTarget("events", "waiting", id)))
	}
	assertResponse(t, s.addr, postRequest("still-works.txt", []byte("file")), 201, "text/plain", nil)
	assertResponse(t, s.addr, getRequest("still-works.txt"), 200, "text/plain", []byte("file"))
	brokerResponse(t, s, postRequest("publish?topic=waiting", []byte("wake")), 200)
	for _, poll := range polls {
		result := <-poll
		if result.err != nil {
			t.Fatal(result.err)
		}
		checkEvent(t, result.response, result.body, 1, []byte("wake"))
	}
}

func TestUnsubscribeWakesWaitingPoll(t *testing.T) {
	s := startTestServer(t)
	id := subscribeForTest(t, s, "cancel")
	target := subscriberTarget("events", "cancel", id)
	poll := startWaitingPoll(t, s, target)
	brokerResponse(t, s, postRequest(subscriberTarget("unsubscribe", "cancel", id), nil), 204)
	result := <-poll
	if result.err != nil || result.response.StatusCode != 404 {
		t.Fatalf("waiting poll response = %v (%v), want 404", result.response, result.err)
	}
	brokerResponse(t, s, getRequest(target), 404)
}
