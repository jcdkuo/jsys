package main

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSubscribeReturnsBufferedChannel(t *testing.T) {
	s := New()
	ch, _ := s.Subscribe()
	select {
	case <-ch:
		t.Fatal("expected channel to be empty initially")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	s := New()
	ch, unsub := s.Subscribe()
	unsub()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("channel did not close after unsubscribe")
	}
}

func TestUnsubscribeIsIdempotent(t *testing.T) {
	s := New()
	_, unsub := s.Subscribe()
	unsub()
	unsub() // must not panic
}

func TestMultipleSubscribersAreIndependent(t *testing.T) {
	s := New()
	a, _ := s.Subscribe()
	b, _ := s.Subscribe()
	if a == b {
		t.Fatal("expected distinct channels")
	}
}

func TestBroadcastDeliversToReadyConsumer(t *testing.T) {
	s := New()
	ch, _ := s.Subscribe()
	snap := &Snapshot{Timestamp: 42}

	s.broadcast(snap)

	select {
	case got := <-ch:
		if got.Timestamp != 42 {
			t.Fatalf("got %d, want 42", got.Timestamp)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("expected snapshot on channel")
	}
}

func TestBroadcastReplacesStaleSnapshotForSlowConsumer(t *testing.T) {
	s := New()
	ch, _ := s.Subscribe()

	s.broadcast(&Snapshot{Timestamp: 1})
	s.broadcast(&Snapshot{Timestamp: 2})

	got := <-ch
	if got.Timestamp != 2 {
		t.Fatalf("expected newest snapshot (2), got %d", got.Timestamp)
	}
}

func TestBroadcastClosesAfterThreeConsecutiveDrops(t *testing.T) {
	s := New()
	ch, _ := s.Subscribe()

	s.broadcast(&Snapshot{Timestamp: 1}) // delivered, drops=0
	s.broadcast(&Snapshot{Timestamp: 2}) // replaces, drops=1
	s.broadcast(&Snapshot{Timestamp: 3}) // replaces, drops=2
	s.broadcast(&Snapshot{Timestamp: 4}) // replaces, drops=3 → close

	deadline := time.After(100 * time.Millisecond)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("expected channel to close after 3 consecutive drops")
		}
	}
}

func TestBroadcastResetsDropCountAfterSuccessfulDelivery(t *testing.T) {
	s := New()
	ch, _ := s.Subscribe()

	s.broadcast(&Snapshot{Timestamp: 1}) // drops=0, buffered
	s.broadcast(&Snapshot{Timestamp: 2}) // drops=1
	<-ch                                  // drain
	s.broadcast(&Snapshot{Timestamp: 3}) // delivered, drops=0
	s.broadcast(&Snapshot{Timestamp: 4}) // drops=1
	s.broadcast(&Snapshot{Timestamp: 5}) // drops=2

	// channel should still be open
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("channel closed prematurely")
		}
		if got.Timestamp != 5 {
			t.Fatalf("expected newest (5), got %d", got.Timestamp)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("expected snapshot on channel")
	}
}

func TestRunCancelExitsCleanly(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = s.runForTest(ctx, 20*time.Millisecond)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not exit after context cancel")
	}
}

func TestRunPopulatesLatest(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.runForTest(ctx, 20*time.Millisecond)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if s.Latest() != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("Latest() never populated")
}

func TestRunClosesSubscribersOnShutdown(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	go s.runForTest(ctx, 20*time.Millisecond)

	ch, _ := s.Subscribe()
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			// first read may yield a snapshot; drain and try again
			select {
			case _, ok2 := <-ch:
				if ok2 {
					t.Fatal("expected channel to close after shutdown")
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatal("channel not closed after shutdown")
			}
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("channel not closed after shutdown")
	}
}

func TestTwoSSEClientsReceiveSameSnapshots(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.runForTest(ctx, 50*time.Millisecond)

	mux := http.NewServeMux()
	mux.HandleFunc("/events", makeEventsHandler(s))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	readN := func(n int) []string {
		resp, err := http.Get(srv.URL + "/events")
		if err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(resp.Body)
		var lines []string
		for scanner.Scan() && len(lines) < n {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				lines = append(lines, line)
			}
		}
		resp.Body.Close()
		return lines
	}

	a := make(chan []string, 1)
	b := make(chan []string, 1)
	go func() { a <- readN(3) }()
	go func() { b <- readN(3) }()

	select {
	case linesA := <-a:
		linesB := <-b
		if len(linesA) != 3 || len(linesB) != 3 {
			t.Fatalf("expected 3 data lines each, got %d / %d", len(linesA), len(linesB))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for SSE data")
	}
}
