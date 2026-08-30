package server

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestTCPBlockingDisconnectAndWakeup(t *testing.T) {
	s := New("127.0.0.1:0")
	startErr := make(chan error, 1)
	go func() { startErr <- s.Start() }()
	var addr string
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		addr = s.Addr()
		if _, port, err := net.SplitHostPort(addr); err == nil && port != "0" {
			break
		}
		select {
		case err := <-startErr:
			t.Skipf("TCP listener unavailable in this environment: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if _, port, _ := net.SplitHostPort(addr); port == "0" {
		t.Fatal("server did not become ready")
	}

	// A blocked BLPOP is woken by a producer and returns the key/value pair.
	listWatcher, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := listWatcher.Write([]byte(encodeCommand("BLPOP", "wakejobs", "1"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	listProducer, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	listProducerReader := bufio.NewReader(listProducer)
	if got := sendAndRead(t, listProducer, listProducerReader, "RPUSH", "wakejobs", "task"); got != ":1\r\n" {
		t.Fatalf("RPUSH wakeup = %q", got)
	}
	listWatcherReader := bufio.NewReader(listWatcher)
	got, err := readReply(listWatcherReader)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "task") {
		t.Fatalf("BLPOP wakeup = %q", got)
	}
	_ = listWatcher.Close()
	_ = listProducer.Close()

	// A blocking timeout returns a null array when no producer arrives.
	timeoutClient, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	timeoutReader := bufio.NewReader(timeoutClient)
	if got := sendAndRead(t, timeoutClient, timeoutReader, "BLPOP", "never", "0.02"); got != "*-1\r\n" {
		t.Fatalf("BLPOP timeout = %q", got)
	}
	_ = timeoutClient.Close()

	// XREAD BLOCK wakes when a producer appends to the requested stream.
	streamWatcher, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := streamWatcher.Write([]byte(encodeCommand("XREAD", "BLOCK", "1000", "STREAMS", "wakeevents", "0-0"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	streamProducer, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	streamProducerReader := bufio.NewReader(streamProducer)
	if got := sendAndRead(t, streamProducer, streamProducerReader, "XADD", "wakeevents", "1-0", "f", "v"); got != "$3\r\n1-0\r\n" {
		t.Fatalf("XADD wakeup = %q", got)
	}
	streamWatcherReader := bufio.NewReader(streamWatcher)
	got, err = readReply(streamWatcherReader)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "1-0") {
		t.Fatalf("XREAD wakeup = %q", got)
	}
	_ = streamWatcher.Close()
	_ = streamProducer.Close()

	blocked, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blocked.Write([]byte(encodeCommand("BLPOP", "jobs", "0"))); err != nil {
		t.Fatal(err)
	}
	_ = blocked.Close()
	// Allow the monitor's bounded read poll to observe EOF and cancel the waiter.
	time.Sleep(150 * time.Millisecond)

	producer, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(producer)
	if got := sendAndRead(t, producer, r, "RPUSH", "jobs", "task"); got != ":1\r\n" {
		t.Fatalf("RPUSH after disconnect = %q", got)
	}
	if got := sendAndRead(t, producer, r, "LLEN", "jobs"); got != ":1\r\n" {
		t.Fatalf("LLEN after disconnect = %q", got)
	}
	_ = producer.Close()

	streamClient, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := streamClient.Write([]byte(encodeCommand("XREAD", "BLOCK", "0", "STREAMS", "events", "0-0"))); err != nil {
		t.Fatal(err)
	}
	_ = streamClient.Close()
	time.Sleep(150 * time.Millisecond)

	if _, err := s.store.XAdd("events", "1-0", []string{"f", "v"}); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-startErr; err != nil {
		t.Fatal(err)
	}
}
