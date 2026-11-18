// Package jsbroker is a transport for generated code on NATS JetStream. It
// implements the broker.Publisher, broker.Subscriber and broker.Requester
// interfaces.
//
// The Broker does not create or configure streams and consumers: retention,
// replicas, delivery policy, ack wait and the rest belong to the
// application, which creates them with the jetstream package and tells the
// Broker which consumer each subscription reads from with WithConsumers:
//
//	cons, _ := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
//		Durable:       orders.OnOrderCreatedEndpoint.Queue,
//		FilterSubject: orders.OnOrderCreatedEndpoint.Subject,
//		AckPolicy:     jetstream.AckExplicitPolicy,
//	})
//	b := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons, jetstream.PullMaxMessages(10))))
//	orders.NewConsumer(b).OnOrderCreated(handle)
//
// To create consumers when subscribing, pass a ConsumerFunc instead, which
// gets the filter and queue of each subscription.
//
// Each message is settled the way the handler says: acked on broker.Ack,
// terminated on broker.Term, and nakked (redelivered) otherwise. An
// Interceptor can change that or settle the message itself, e.g. to move it
// to a dead letter queue. A failed ack is not reported, and the server
// redelivers the message after its ack wait. Consume errors go to the
// jetstream.ConsumeErrHandler among the consume options, if any. Publish waits
// for the stream's acknowledgement.
//
// Request goes over core NATS, since replies are not stored; responders use
// natsbroker.
package jsbroker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Broker is a transport backed by JetStream.
type Broker struct {
	js             jetstream.JetStream
	consumers      ConsumerFactory
	interceptors   []Interceptor
	onError        func(error)
	requestTimeout time.Duration
}

var (
	_ broker.Publisher  = (*Broker)(nil)
	_ broker.Subscriber = (*Broker)(nil)
	_ broker.Requester  = (*Broker)(nil)
)

// Option configures a Broker.
type Option func(*Broker)

// WithConsumers sets where subscriptions consume from. Without it, Subscribe
// fails with ErrNoConsumer.
func WithConsumers(f ConsumerFactory) Option { return func(b *Broker) { b.consumers = f } }

// WithInterceptors wraps the delivery of each consumed message, the first
// being the outermost.
func WithInterceptors(i ...Interceptor) Option {
	return func(b *Broker) { b.interceptors = append(b.interceptors, i...) }
}

// WithErrorHandler receives the errors handlers return with their
// settlement. By default they are discarded.
func WithErrorHandler(fn func(error)) Option { return func(b *Broker) { b.onError = fn } }

// WithRequestTimeout applies to requests whose context has no deadline,
// which NATS requires. The default is 5 seconds.
func WithRequestTimeout(d time.Duration) Option { return func(b *Broker) { b.requestTimeout = d } }

// New returns a Broker using js.
func New(js jetstream.JetStream, opts ...Option) *Broker {
	b := &Broker{js: js, onError: func(error) {}, requestTimeout: 5 * time.Second}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// JetStream returns the underlying JetStream context.
func (b *Broker) JetStream() jetstream.JetStream { return b.js }

// ConsumerFactory gives the consumer of a subscription, along with the
// options for its Consume call. Both are used as they are.
type ConsumerFactory interface {
	// Consumer is called once by every Subscribe, with its filter and queue,
	// e.g. to fill in FilterSubject and Durable.
	Consumer(filter, queue string) (jetstream.Consumer, []jetstream.PullConsumeOpt, error)
}

// ConsumerFunc is a function implementing ConsumerFactory.
type ConsumerFunc func(filter, queue string) (jetstream.Consumer, []jetstream.PullConsumeOpt, error)

// Consumer calls f.
func (f ConsumerFunc) Consumer(filter, queue string) (jetstream.Consumer, []jetstream.PullConsumeOpt, error) {
	return f(filter, queue)
}

// Existing returns a ConsumerFactory giving cons, consumed with opts, to
// every subscription.
func Existing(cons jetstream.Consumer, opts ...jetstream.PullConsumeOpt) ConsumerFactory {
	return ConsumerFunc(func(string, string) (jetstream.Consumer, []jetstream.PullConsumeOpt, error) {
		return cons, opts, nil
	})
}

// Deliver handles a consumed message. The Settlement it returns decides how
// the message is settled, unless it has been settled already.
type Deliver func(ctx context.Context, msg jetstream.Msg) (broker.Settlement, error)

// Interceptor wraps the delivery of each message. The next Deliver decodes
// the message and runs the application's handler, with the context it is
// given. An Interceptor can return a different Settlement, such as Ack after
// moving the message to a dead letter queue, or settle msg itself, such as
// with NakWithDelay, after which the Settlement no longer affects it. The msg
// gives access to Metadata, e.g. the number of deliveries.
type Interceptor func(next Deliver) Deliver

// Publish stores a message in the stream that captures subject and waits for
// the acknowledgement. It fails when no stream captures the subject.
func (b *Broker) Publish(ctx context.Context, subject string, headers map[string][]string, body []byte) error {
	_, err := b.js.PublishMsg(ctx, &nats.Msg{Subject: subject, Header: headers, Data: body})
	return err
}

// Request sends a request over core NATS and waits for the reply. A reply
// reporting a failure is returned as *broker.RemoteError.
func (b *Broker) Request(ctx context.Context, subject string, headers map[string][]string, body []byte) ([]byte, map[string][]string, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, b.requestTimeout)
		defer cancel()
	}
	rm, err := b.js.Conn().RequestMsgWithContext(ctx, &nats.Msg{Subject: subject, Header: headers, Data: body})
	if err != nil {
		return nil, nil, err
	}
	if err := broker.ReplyError(rm.Header); err != nil {
		return nil, rm.Header, err
	}
	return rm.Data, rm.Header, nil
}

// ErrNoConsumer is returned by Subscribe when the Broker has no
// ConsumerFactory.
var ErrNoConsumer = errors.New("jsbroker: no consumer (use jsbroker.WithConsumers)")

// Subscribe consumes from the consumer the ConsumerFactory gives for filter
// and queue. Its filter subjects must lie within filter, since messages the
// generated code cannot parse are terminated. h gets a background context;
// use an Interceptor to derive one from the message.
func (b *Broker) Subscribe(filter, queue string, h func(context.Context, broker.Inbox) (broker.Settlement, error)) (broker.Subscription, error) {
	if b.consumers == nil {
		return nil, fmt.Errorf("%w for subject %q", ErrNoConsumer, filter)
	}
	cons, opts, err := b.consumers.Consumer(filter, queue)
	if err != nil {
		return nil, fmt.Errorf("jsbroker: consumer for subject %q: %w", filter, err)
	}
	if err := checkFilters(cons, filter); err != nil {
		return nil, err
	}
	deliver := Deliver(func(ctx context.Context, msg jetstream.Msg) (broker.Settlement, error) {
		return h(ctx, inbox{msg})
	})
	for _, i := range slices.Backward(b.interceptors) {
		deliver = i(deliver)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		s, err := deliver(context.Background(), msg)
		if err != nil {
			b.onError(err)
		}
		// A message an Interceptor settled refuses further settling.
		switch s {
		case broker.Ack:
			_ = msg.Ack()
		case broker.Term:
			_ = msg.Term()
		default:
			_ = msg.Nak()
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

type inbox struct{ m jetstream.Msg }

func (i inbox) Subject() string { return i.m.Subject() }
func (i inbox) Data() []byte    { return i.m.Data() }

// Headers returns the message's headers, never nil.
func (i inbox) Headers() map[string][]string {
	if h := i.m.Headers(); h != nil {
		return h
	}
	return map[string][]string{}
}

// checkFilters fails when the consumer filters on subjects outside subject,
// whose messages the subscription could only terminate. Consumers without a
// filter get whatever their stream captures and are not checked.
func checkFilters(c jetstream.Consumer, subject string) error {
	info := c.CachedInfo()
	if info == nil {
		return nil
	}
	filters := info.Config.FilterSubjects
	if f := info.Config.FilterSubject; f != "" {
		filters = []string{f}
	}
	for _, f := range filters {
		if !within(f, subject) {
			return fmt.Errorf("jsbroker: consumer %q filters %q, outside subject %q", info.Name, f, subject)
		}
	}
	return nil
}

// within reports whether every subject matching filter matches pattern.
// pattern uses only "*" wildcards, as generated endpoints do.
func within(filter, pattern string) bool {
	f, p := strings.Split(filter, "."), strings.Split(pattern, ".")
	if len(f) != len(p) {
		return false
	}
	for i := range p {
		if f[i] == ">" || (p[i] != "*" && p[i] != f[i]) {
			return false
		}
	}
	return true
}
