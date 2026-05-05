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
