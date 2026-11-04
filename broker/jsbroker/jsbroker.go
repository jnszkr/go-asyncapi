// Package jsbroker implements broker.Broker on NATS JetStream.
//
// The Broker has no configuration and does not create or configure streams
// and consumers: retention, replicas, delivery policy, ack wait, filters,
// consume options and the rest belong to the application, which creates them
// with the jetstream package and passes the consumer to each subscription
// with From:
//
//	cons, _ := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
//		Durable:       "order-processors",
//		FilterSubject: orders.OrderCreatedChannel.Pattern(),
//		AckPolicy:     jetstream.AckExplicitPolicy,
//		AckWait:       30 * time.Second,
//	})
//	c := orders.NewConsumer(jsbroker.New(js))
//	c.OnOrderCreated(handle, jsbroker.From(cons, jetstream.PullMaxMessages(10)))
//
// To create the consumer when subscribing, pass a ConsumerFactory with
// FromFactory instead, which also takes Interceptors.
//
// Messages are acked when the handler returns nil, nakked (redelivered) when
// it returns an error, and terminated when the error wraps
// broker.ErrMalformed. An Interceptor can change that outcome or settle the
// message itself, e.g. to move it to a dead letter queue. A failed ack is not
// reported, and the server redelivers the message after its ack wait; an
// Interceptor settling the message itself gets the error. Interceptors here see
// the jetstream.Msg; those of broker.WithReceiveInterceptors, which work on every
// transport, see a broker.Message and run inside them. Consume errors go to
// the jetstream.ConsumeErrHandler among the consume options, if any. Publish
// waits for the stream's acknowledgement.
//
// Request goes over core NATS, since replies are not stored; responders
// should use natsbroker.
package jsbroker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Broker is a broker.Broker backed by JetStream.
type Broker struct {
	js jetstream.JetStream
}

var _ broker.Broker = (*Broker)(nil)

// New returns a Broker using js.
func New(js jetstream.JetStream) *Broker { return &Broker{js: js} }

// JetStream returns the underlying JetStream context.
func (b *Broker) JetStream() jetstream.JetStream { return b.js }

// ConsumerFactory creates the consumer of a subscription, along with the
// options for its Consume call. Both are used as they are.
type ConsumerFactory interface {
	// Consumer is called once by Subscribe. cfg holds the subject and queue
	// of the subscription, e.g. to fill in FilterSubject and Durable.
	Consumer(cfg broker.SubConfig) (jetstream.Consumer, []jetstream.PullConsumeOpt, error)
}

// ConsumerFunc is a function implementing ConsumerFactory.
type ConsumerFunc func(cfg broker.SubConfig) (jetstream.Consumer, []jetstream.PullConsumeOpt, error)

// Consumer calls f.
func (f ConsumerFunc) Consumer(cfg broker.SubConfig) (jetstream.Consumer, []jetstream.PullConsumeOpt, error) {
	return f(cfg)
}

// Existing returns a ConsumerFactory for a consumer that already exists,
// consumed with opts.
func Existing(cons jetstream.Consumer, opts ...jetstream.PullConsumeOpt) ConsumerFactory {
	return ConsumerFunc(func(broker.SubConfig) (jetstream.Consumer, []jetstream.PullConsumeOpt, error) {
		return cons, opts, nil
	})
}

// Deliver handles a consumed message. Its error decides how the message is
// settled, unless it has been settled already: nil acks it, an error wrapping
// broker.ErrMalformed terminates it, and any other error naks it.
type Deliver func(ctx context.Context, msg jetstream.Msg) error

// Interceptor wraps the delivery of each message of a subscription. The next
// Deliver decodes the message and runs the application's handler, with the
// context it is given. An Interceptor can return a different error to change
// the outcome, such as nil after moving the message to a dead letter queue, or
// settle msg itself, such as with NakWithDelay, in which case the returned
// error no longer affects it. The msg gives access to Metadata, e.g. the number
// of deliveries.
type Interceptor func(next Deliver) Deliver

type source struct {
	factory      ConsumerFactory
	interceptors []Interceptor
}

// From makes a subscription consume from cons, passing opts to cons.Consume.
// See FromFactory for the requirements on the consumer.
func From(cons jetstream.Consumer, opts ...jetstream.PullConsumeOpt) broker.SubOption {
	return FromFactory(Existing(cons, opts...))
}

// FromFactory makes a subscription consume from the consumer f creates. Its
// filter subjects must lie within the subscribed subject, since messages that
// don't match the channel are terminated. The subscription's queue only reaches
// f: every subscription on the same consumer shares its messages.
//
// Each message passes through interceptors, the first being the outermost. Use
// Existing to intercept a consumer that already exists.
func FromFactory(f ConsumerFactory, interceptors ...Interceptor) broker.SubOption {
	return broker.WithSource(source{factory: f, interceptors: interceptors})
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

// ErrNoConsumer is returned by Subscribe when no consumer was given with From or
// FromFactory.
var ErrNoConsumer = errors.New("jsbroker: no consumer (use jsbroker.From or jsbroker.FromFactory)")

// Subscribe consumes from the consumer set with From or FromFactory. h gets a
// background context; use an Interceptor to derive one from the message.
func (b *Broker) Subscribe(cfg broker.SubConfig, h func(context.Context, *broker.Message) error) (broker.Subscription, error) {
	src, ok := cfg.Source.(source)
	switch {
	case cfg.Source == nil:
		return nil, fmt.Errorf("%w for subject %q", ErrNoConsumer, cfg.Subject)
	case !ok:
		return nil, fmt.Errorf("jsbroker: unsupported source %T", cfg.Source)
	}
	cons, opts, err := src.factory.Consumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("jsbroker: consumer for subject %q: %w", cfg.Subject, err)
	}
	if err := checkFilters(cons, cfg.Subject); err != nil {
		return nil, err
	}
	deliver := Deliver(func(ctx context.Context, msg jetstream.Msg) error {
		return h(ctx, &broker.Message{Subject: msg.Subject(), Header: broker.Header(msg.Headers()), Data: msg.Data()})
	})
	for i := len(src.interceptors) - 1; i >= 0; i-- {
		deliver = src.interceptors[i](deliver)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		// A message an Interceptor settled refuses further settling.
		err := deliver(context.Background(), msg)
		switch {
		case err == nil:
			_ = msg.Ack()
		case errors.Is(err, broker.ErrMalformed):
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

func toNATS(m *broker.Message) *nats.Msg {
	return &nats.Msg{Subject: m.Subject, Reply: m.Reply, Header: nats.Header(m.Header), Data: m.Data}
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
// pattern uses only "*" wildcards, as channel patterns do.
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
