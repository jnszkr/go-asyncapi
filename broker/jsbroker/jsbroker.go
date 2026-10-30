// Package jsbroker implements broker.Broker on NATS JetStream.
//
// The Broker does not create streams or consumers: their configuration
// (retention, replicas, delivery policy, ack wait, ...) belongs to the
// application. Create them with the jetstream package and bind each consumer
// to the endpoint of a receive operation; generated code declares those
// endpoints, and ConsumerConfig turns one into a starting configuration.
//
//	cons, _ := js.CreateOrUpdateConsumer(ctx, "ORDERS", jsbroker.ConsumerConfig(orders.OnOrderCreatedEndpoint))
//	b := jsbroker.New(js)
//	b.Bind(orders.OnOrderCreatedEndpoint, cons)
//	c := orders.NewClient(b)
//	c.OnOrderCreated(handle) // consumes from cons
//
// Messages are acked when the handler returns nil, nakked (redelivered) when
// it returns an error, and terminated when the error wraps
// broker.ErrMalformed. Publish waits for the stream's acknowledgement.
// Request goes over core NATS, since replies are not stored; responders
// should use natsbroker.
package jsbroker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Broker is a broker.Broker backed by JetStream.
type Broker struct {
	js      jetstream.JetStream
	onError func(error)

	mu    sync.Mutex
	bound map[broker.Endpoint]binding
}

type binding struct {
	consumer jetstream.Consumer
	opts     []jetstream.PullConsumeOpt
}

var _ broker.Broker = (*Broker)(nil)

// Option configures a Broker.
type Option func(*Broker)

// WithErrorHandler receives errors the Broker cannot return to a caller:
// failed acks and consume errors. By default they are discarded.
func WithErrorHandler(fn func(error)) Option { return func(b *Broker) { b.onError = fn } }

// New returns a Broker using js.
func New(js jetstream.JetStream, opts ...Option) *Broker {
	b := &Broker{js: js, onError: func(error) {}, bound: map[broker.Endpoint]binding{}}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// JetStream returns the underlying JetStream context.
func (b *Broker) JetStream() jetstream.JetStream { return b.js }

// Bind makes Subscribe calls for ep consume from c, with opts passed to
// c.Consume. Binding an endpoint again replaces the earlier consumer for
// later subscriptions.
func (b *Broker) Bind(ep broker.Endpoint, c jetstream.Consumer, opts ...jetstream.PullConsumeOpt) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bound[ep] = binding{consumer: c, opts: opts}
}

// ConsumerConfig is a starting point for a consumer serving ep: it filters
// on ep.Subject, acks explicitly and, when ep has a queue, is durable with
// the queue as its name, so that every subscriber sharing the queue shares
// the consumer. Adjust the result as needed before creating the consumer.
func ConsumerConfig(ep broker.Endpoint) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       ep.Queue,
		FilterSubject: ep.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}
}

// Publish stores m in the stream that captures m.Subject and waits for the
// acknowledgement. It fails when no stream captures the subject.
func (b *Broker) Publish(ctx context.Context, m *broker.Message) error {
	_, err := b.js.PublishMsg(ctx, toNATS(m))
	return err
}

// Request sends m over core NATS and waits for the reply.
func (b *Broker) Request(ctx context.Context, m *broker.Message) (*broker.Message, error) {
	rm, err := b.js.Conn().RequestMsgWithContext(ctx, toNATS(m))
	if err != nil {
		return nil, err
	}
	return &broker.Message{Subject: rm.Subject, Header: broker.Header(rm.Header), Data: rm.Data}, nil
}

// ErrNotBound is returned by Subscribe for an endpoint without a consumer.
var ErrNotBound = errors.New("jsbroker: no consumer bound")

// Subscribe consumes from the consumer bound to (subject, queue).
func (b *Broker) Subscribe(subject, queue string, h func(*broker.Message) error) (broker.Subscription, error) {
	ep := broker.Endpoint{Subject: subject, Queue: queue}
	b.mu.Lock()
	bd, ok := b.bound[ep]
	b.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w for subject %q, queue %q", ErrNotBound, subject, queue)
	}
	opts := append([]jetstream.PullConsumeOpt{
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			b.onError(fmt.Errorf("jsbroker: consume %s: %w", subject, err))
		}),
	}, bd.opts...)
	cc, err := bd.consumer.Consume(func(msg jetstream.Msg) {
		herr := h(&broker.Message{Subject: msg.Subject(), Header: broker.Header(msg.Headers()), Data: msg.Data()})
		var aerr error
		switch {
		case herr == nil:
			aerr = msg.Ack()
		case errors.Is(herr, broker.ErrMalformed):
			aerr = msg.Term()
		default:
			aerr = msg.Nak()
		}
		if aerr != nil {
			b.onError(fmt.Errorf("jsbroker: ack %s: %w", msg.Subject(), aerr))
		}
	}, opts...)
	if err != nil {
		return nil, err
	}
	return subscription{cc}, nil
}

type subscription struct{ cc jetstream.ConsumeContext }

// Unsubscribe stops consuming. The consumer itself is left in place.
func (s subscription) Unsubscribe() error {
	s.cc.Stop()
	return nil
}

func toNATS(m *broker.Message) *nats.Msg {
	return &nats.Msg{Subject: m.Subject, Reply: m.Reply, Header: nats.Header(m.Header), Data: m.Data}
}
