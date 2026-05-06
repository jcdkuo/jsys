package main

import (
	"sync"
	"sync/atomic"
	"time"
)

type Sampler struct {
	mu          sync.Mutex
	latest      atomic.Pointer[Snapshot]
	subscribers map[chan *Snapshot]*subState
	previousCPU []cpuTimes
	previousNet map[string]netCounters
	previousAt  time.Time
	eventID     int64
	events      []Event
	darwinCPU   atomic.Pointer[darwinCPU]
}

type subState struct {
	drops int
}

type darwinCPU struct {
	total float64
	at    time.Time
}

func New() *Sampler {
	return &Sampler{
		subscribers: map[chan *Snapshot]*subState{},
		previousNet: map[string]netCounters{},
		previousAt:  time.Now(),
	}
}

func (s *Sampler) Subscribe() (<-chan *Snapshot, func()) {
	ch := make(chan *Snapshot, 1)
	s.mu.Lock()
	s.subscribers[ch] = &subState{}
	s.mu.Unlock()
	return ch, func() { s.unsubscribe(ch) }
}

func (s *Sampler) unsubscribe(ch chan *Snapshot) {
	s.mu.Lock()
	if _, ok := s.subscribers[ch]; ok {
		delete(s.subscribers, ch)
		close(ch)
	}
	s.mu.Unlock()
}

func (s *Sampler) Latest() *Snapshot {
	return s.latest.Load()
}

func (s *Sampler) broadcast(snap *Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch, st := range s.subscribers {
		select {
		case ch <- snap:
			st.drops = 0
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- snap:
			default:
			}
			st.drops++
			if st.drops >= 3 {
				delete(s.subscribers, ch)
				close(ch)
			}
		}
	}
}
