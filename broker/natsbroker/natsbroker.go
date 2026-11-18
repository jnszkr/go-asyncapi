// Package natsbroker is a transport for generated code on a core NATS
// connection. It implements every transport interface of package broker.
//
// Core NATS has no acknowledgements, so the Settlement a Consumer returns is
// ignored; errors go to the handler set with WithErrorHandler. Failures to
// answer a request travel in the headers of the NATS micro protocol and
// reach the requester as *broker.RemoteError.
package natsbroker

import (
	"context"
	"fmt"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/nats-io/nats.go"
)

// Broker is a transport backed by a NATS connection.
type Broker struct {
	nc             *nats.Conn
	onError        func(error)
	requestTimeout time.Duration
}

var (
	_ broker.Publisher  = (*Broker)(nil)
	_ broker.Subscriber = (*Broker)(nil)
	_ broker.Requester  = (*Broker)(nil)
	_ broker.Replier    = (*Broker)(nil)
)

// Option configures a Broker.
type Option func(*Broker)

// WithErrorHandler receives the errors of handling received messages and
// requests, e.g. payloads that cannot be decoded. By default they are
// discarded.
func WithErrorHandler(fn func(error)) Option { return func(b *Broker) { b.onError = fn } }

// WithRequestTimeout applies to requests whose context has no deadline,
// which NATS requires. The default is 5 seconds.
func WithRequestTimeout(d time.Duration) Option { return func(b *Broker) { b.requestTimeout = d } }

// New returns a Broker using nc.
func New(nc *nats.Conn, opts ...Option) *Broker {
	b := &Broker{nc: nc, onError: func(error) {}, requestTimeout: 5 * time.Second}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Conn returns the underlying connection.
func (b *Broker) Conn() *nats.Conn { return b.nc }

// Publish sends a message.
func (b *Broker) Publish(_ context.Context, subject string, headers map[string][]string, body []byte) error {
	return b.nc.PublishMsg(&nats.Msg{Subject: subject, Header: headers, Data: body})
}

// Request sends a request and waits for its reply. A reply reporting a
// failure is returned as *broker.RemoteError.
func (b *Broker) Request(ctx context.Context, subject string, headers map[string][]string, body []byte) ([]byte, map[string][]string, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, b.requestTimeout)
		defer cancel()
	}
	rm, err := b.nc.RequestMsgWithContext(ctx, &nats.Msg{Subject: subject, Header: headers, Data: body})
	if err != nil {
		return nil, nil, err
	}
	if err := broker.ReplyError(rm.Header); err != nil {
		return nil, rm.Header, err
	}
	return rm.Data, rm.Header, nil
}

// Subscribe uses a core NATS subscription, a queue subscription when queue
// is set. h gets a background context; wrap the Broker to derive one from
// the message.
func (b *Broker) Subscribe(filter, queue string, h func(context.Context, broker.Inbox) (broker.Settlement, error)) (broker.Subscription, error) {
	return b.subscribe(filter, queue, func(m *nats.Msg) {
		if _, err := h(context.Background(), inbox{m}); err != nil {
			b.onError(err)
		}
	})
}

// Reply answers the requests on subject with what h returns, or with its
// error: a 400 when it is malformed (see broker.IsMalformed), a 500 when not.
// Replies are sent even when h returns an error, which is also reported.
func (b *Broker) Reply(subject, queue string, h func(context.Context, broker.Inbox) ([]byte, error)) (broker.Subscription, error) {
	return b.subscribe(subject, queue, func(m *nats.Msg) {
		if m.Reply == "" {
			b.onError(fmt.Errorf("%s: request without reply subject", m.Subject))
			return
		}
		reply := &nats.Msg{Subject: m.Reply}
		body, err := h(context.Background(), inbox{m})
		if err != nil {
			b.onError(err)
			reply.Header = broker.ErrorHeaders(err)
		} else {
			reply.Data = body
		}
		if err := b.nc.PublishMsg(reply); err != nil {
			b.onError(fmt.Errorf("%s: reply: %w", m.Subject, err))
		}
	})
}

func (b *Broker) subscribe(subject, queue string, cb nats.MsgHandler) (broker.Subscription, error) {
	if queue != "" {
		return b.nc.QueueSubscribe(subject, queue, cb)
	}
	return b.nc.Subscribe(subject, cb)
}

type inbox struct{ m *nats.Msg }

func (i inbox) Subject() string { return i.m.Subject }
func (i inbox) Data() []byte    { return i.m.Data }

// Headers returns the message's headers, never nil.
func (i inbox) Headers() map[string][]string {
	if i.m.Header == nil {
		return map[string][]string{}
	}
	return i.m.Header
}
