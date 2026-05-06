package main

import (
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
