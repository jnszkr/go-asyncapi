// Package membroker is an in-process broker.Broker, useful for tests.
//
// Messages are delivered synchronously on the publishing goroutine. Subjects
// match token by token, with "*" matching any single token. Subscribers
// sharing a queue name receive messages round-robin.
package membroker

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/jnszkr/go-asyncapi/broker"
)

// Broker is an in-memory broker.Broker.
type Broker struct {
	mu     sync.Mutex
	subs   []*sub
	next   map[string]int // round-robin cursor per (pattern, queue)
	inboxN int
}

var _ broker.Broker = (*Broker)(nil)

type sub struct {
	b       *Broker
	pattern string
	queue   string
	h       func(*broker.Message)
}

func (s *sub) Unsubscribe() error {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	for i, x := range s.b.subs {
		if x == s {
			s.b.subs = append(s.b.subs[:i], s.b.subs[i+1:]...)
			break
		}
	}
	return nil
}

// New returns an empty Broker.
func New() *Broker { return &Broker{next: map[string]int{}} }

func (b *Broker) Subscribe(subject, queue string, h func(*broker.Message)) (broker.Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := &sub{b: b, pattern: subject, queue: queue, h: h}
	b.subs = append(b.subs, s)
	return s, nil
}

func (b *Broker) Publish(_ context.Context, m *broker.Message) error {
	for _, s := range b.targets(m.Subject) {
		s.h(clone(m))
	}
	return nil
}

func (b *Broker) Request(ctx context.Context, m *broker.Message) (*broker.Message, error) {
	b.mu.Lock()
	b.inboxN++
	inbox := "_INBOX." + strconv.Itoa(b.inboxN)
	b.mu.Unlock()

	replies := make(chan *broker.Message, 1)
	s, _ := b.Subscribe(inbox, "", func(r *broker.Message) {
		select {
		case replies <- r:
		default:
		}
	})
	defer s.Unsubscribe()

	req := clone(m)
	req.Reply = inbox
	if len(b.targets(req.Subject)) == 0 {
		return nil, errNoResponders
	}
	go b.Publish(ctx, req)
	select {
	case r := <-replies:
		return r, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type brokerError string

func (e brokerError) Error() string { return string(e) }

const errNoResponders brokerError = "membroker: no responders"

// targets picks the subscribers that receive a message on subject: every
// plain subscriber, and one subscriber per queue group.
func (b *Broker) targets(subject string) []*sub {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*sub
	groups := map[string][]*sub{}
	var order []string
	for _, s := range b.subs {
		if !match(s.pattern, subject) {
			continue
		}
		if s.queue == "" {
			out = append(out, s)
			continue
		}
		key := s.pattern + "\x00" + s.queue
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], s)
	}
	for _, key := range order {
		g := groups[key]
		out = append(out, g[b.next[key]%len(g)])
		b.next[key]++
	}
	return out
}

func match(pattern, subject string) bool {
	p, s := strings.Split(pattern, "."), strings.Split(subject, ".")
	if len(p) != len(s) {
		return false
	}
	for i := range p {
		if p[i] != "*" && p[i] != s[i] {
			return false
		}
	}
	return true
}

func clone(m *broker.Message) *broker.Message {
	c := *m
	c.Header = broker.Header{}
	for k, v := range m.Header {
		c.Header[k] = append([]string(nil), v...)
	}
	return &c
}
