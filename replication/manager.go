package replication

import (
	"encoding/json"
	"sync"
	"sync/atomic"
)

type Event struct {
	Collection string          `json:"collection"`
	Command    json.RawMessage `json:"command"`
}

type Manager struct {
	incoming    chan *Event
	subscribers map[int64]chan *Event
	mu          sync.RWMutex
	nextID      int64
}

func NewManager() *Manager {
	m := &Manager{
		incoming:    make(chan *Event, 1024),
		subscribers: map[int64]chan *Event{},
	}
	go m.loop()
	return m
}

func (m *Manager) loop() {
	for event := range m.incoming {
		var drop []int64

		m.mu.RLock()
		for id, ch := range m.subscribers {
			select {
			case ch <- event:
			default:
				drop = append(drop, id)
			}
		}
		m.mu.RUnlock()

		if len(drop) == 0 {
			continue
		}

		m.mu.Lock()
		for _, id := range drop {
			ch, ok := m.subscribers[id]
			if !ok {
				continue
			}
			delete(m.subscribers, id)
			close(ch)
		}
		m.mu.Unlock()
	}

	m.mu.Lock()
	for id, ch := range m.subscribers {
		delete(m.subscribers, id)
		close(ch)
	}
	m.mu.Unlock()
}

func (m *Manager) PublishCommand(collection string, command []byte) {
	if m == nil {
		return
	}

	if m.incoming == nil {
		return
	}

	payload := json.RawMessage(append([]byte(nil), command...))
	event := &Event{Collection: collection, Command: payload}
	m.incoming <- event
}

func (m *Manager) Subscribe() (<-chan *Event, func()) {
	if m == nil {
		ch := make(chan *Event)
		close(ch)
		return ch, func() {}
	}

	ch := make(chan *Event, 1024)
	id := atomic.AddInt64(&m.nextID, 1)

	m.mu.Lock()
	m.subscribers[id] = ch
	m.mu.Unlock()

	cancel := func() {
		m.mu.Lock()
		subscriber, ok := m.subscribers[id]
		if ok {
			delete(m.subscribers, id)
			close(subscriber)
		}
		m.mu.Unlock()
	}

	return ch, cancel
}

func (m *Manager) Close() {
	if m == nil {
		return
	}
	if m.incoming == nil {
		return
	}
	close(m.incoming)
	m.incoming = nil
}
