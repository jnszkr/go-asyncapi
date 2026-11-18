// Package membroker is an in-process transport for generated code, useful
// for tests. It implements every transport interface of package broker.
//
// Messages are delivered synchronously on the publishing goroutine. Subjects
// match token by token, with "*" matching any single token. Subscribers
// sharing a queue name receive messages round-robin. As on core NATS, the
// Settlement a Consumer returns is ignored, and only the message reaches a
// handler: its context is a background one, not the sender's.
package membroker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/jnszkr/go-asyncapi/broker"
)

// Broker is an in-memory transport.
type Broker struct {
	onError func(error)

	mu   sync.Mutex
	subs []*sub
	next map[string]int // round-robin cursor per (pattern, queue)
}

var (
	_ broker.Publisher  = (*Broker)(nil)
	_ broker.Subscriber = (*Broker)(nil)
	_ broker.Requester  = (*Broker)(nil)
	_ broker.Replier    = (*Broker)(nil)
)

// Option configures a Broker.
type Option func(*Broker)

// WithErrorHandler receives the errors handlers return. By default they are
// discarded.
func WithErrorHandler(fn func(error)) Option { return func(b *Broker) { b.onError = fn } }

// New returns an empty Broker.
func New(opts ...Option) *Broker {
	b := &Broker{onError: func(error) {}, next: map[string]int{}}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// ErrNoResponders is returned by Request when no Reply subscription matches.
var ErrNoResponders = errors.New("membroker: no responders")

type sub struct {
	b       *Broker
	pattern string
	queue   string
	handle  func(context.Context, broker.Inbox) (broker.Settlement, error) // Subscribe
	reply   func(context.Context, broker.Inbox) ([]byte, error)            // Reply
}

func (s *sub) Unsubscribe() error {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	s.b.subs = slices.DeleteFunc(s.b.subs, func(x *sub) bool { return x == s })
	return nil
}

// Subscribe registers h.
func (b *Broker) Subscribe(filter, queue string, h func(context.Context, broker.Inbox) (broker.Settlement, error)) (broker.Subscription, error) {
	return b.add(&sub{b: b, pattern: filter, queue: queue, handle: h}), nil
}

// Reply registers h to answer requests.
func (b *Broker) Reply(subject, queue string, h func(context.Context, broker.Inbox) ([]byte, error)) (broker.Subscription, error) {
	return b.add(&sub{b: b, pattern: subject, queue: queue, reply: h}), nil
}

func (b *Broker) add(s *sub) *sub {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = append(b.subs, s)
	return s
}

// Publish delivers a message to the matching subscribers.
func (b *Broker) Publish(_ context.Context, subject string, headers map[string][]string, body []byte) error {
	for _, s := range b.targets(subject, false) {
		if _, err := s.handle(context.Background(), newMessage(subject, headers, body)); err != nil {
			b.onError(err)
		}
	}
	return nil
}

// Request calls one matching Reply handler and returns its answer. Its error
// travels as it would over NATS and comes back as *broker.RemoteError.
func (b *Broker) Request(ctx context.Context, subject string, headers map[string][]string, body []byte) ([]byte, map[string][]string, error) {
	targets := b.targets(subject, true)
	if len(targets) == 0 {
		return nil, nil, ErrNoResponders
	}
	type answer struct {
		body []byte
		err  error
	}
	done := make(chan answer, 1)
	go func() {
		body, err := targets[0].reply(context.Background(), newMessage(subject, headers, body))
		done <- answer{body, err}
	}()
	select {
	case a := <-done:
		if a.err != nil {
			b.onError(a.err)
			h := broker.ErrorHeaders(a.err)
			return nil, h, broker.ReplyError(h)
		}
		return a.body, map[string][]string{}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// targets picks the subscriptions of one kind that receive a message on
// subject: every one without a queue, and one per queue group.
func (b *Broker) targets(subject string, replies bool) []*sub {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*sub
	groups := map[string][]*sub{}
	var order []string
	for _, s := range b.subs {
		if (s.reply != nil) != replies || !match(s.pattern, subject) {
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
	for {
		p, prest, pmore := strings.Cut(pattern, ".")
		s, srest, smore := strings.Cut(subject, ".")
		if p != "*" && p != s || pmore != smore {
			return false
		}
		if !pmore {
			return true
		}
		pattern, subject = prest, srest
	}
}

// message is a received copy of a message, so that receivers cannot change
// what the sender or other receivers see.
type message struct {
	subject string
	headers map[string][]string
	data    []byte
}

func newMessage(subject string, headers map[string][]string, data []byte) *message {
	m := &message{subject: subject, headers: make(map[string][]string, len(headers)), data: slices.Clone(data)}
	for k, v := range headers {
		m.headers[k] = slices.Clone(v)
	}
	return m
}

func (m *message) Subject() string              { return m.subject }
func (m *message) Headers() map[string][]string { return m.headers }
func (m *message) Data() []byte                 { return m.data }
