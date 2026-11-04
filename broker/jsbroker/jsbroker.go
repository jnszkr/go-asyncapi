// Package jsbroker implements broker.Broker on NATS JetStream.
//
// The Broker does not create or configure streams and consumers: retention,
// replicas, delivery policy, ack wait, filters and the rest belong to the
// application, which creates them with the jetstream package and passes the
// consumer to each subscription with From:
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
	"strings"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Broker is a broker.Broker backed by JetStream.
type Broker struct {
	js      jetstream.JetStream
	onError func(error)
}

var _ broker.Broker = (*Broker)(nil)

// Option configures a Broker.
type Option func(*Broker)

// WithErrorHandler receives errors the Broker cannot return to a caller:
// failed acks and consume errors. By default they are discarded.
func WithErrorHandler(fn func(error)) Option { return func(b *Broker) { b.onError = fn } }

// New returns a Broker using js.
func New(js jetstream.JetStream, opts ...Option) *Broker {
	b := &Broker{js: js, onError: func(error) {}}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// JetStream returns the underlying JetStream context.
func (b *Broker) JetStream() jetstream.JetStream { return b.js }

type source struct {
	consumer jetstream.Consumer
	opts     []jetstream.PullConsumeOpt
}

// From makes a subscription consume from cons, passing opts to
// cons.Consume. The consumer is used as configured; its filter subjects
// must lie within the subscribed subject, since messages that don't match
// the channel are terminated. The subscription's queue is ignored: every
// subscription on the same consumer shares its messages.
func From(cons jetstream.Consumer, opts ...jetstream.PullConsumeOpt) broker.SubOption {
	return broker.WithSource(source{consumer: cons, opts: opts})
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

// ErrNoConsumer is returned by Subscribe when no consumer was given with From.
var ErrNoConsumer = errors.New("jsbroker: no consumer (use jsbroker.From)")

// Subscribe consumes from the consumer set with From. h gets a background
// context; wrap the Broker to derive one from the message.
func (b *Broker) Subscribe(cfg broker.SubConfig, h func(context.Context, *broker.Message) error) (broker.Subscription, error) {
	src, ok := cfg.Source.(source)
	switch {
	case cfg.Source == nil:
		return nil, fmt.Errorf("%w for subject %q", ErrNoConsumer, cfg.Subject)
	case !ok:
		return nil, fmt.Errorf("jsbroker: unsupported source %T", cfg.Source)
	}
	if err := checkFilters(src.consumer, cfg.Subject); err != nil {
		return nil, err
	}
	opts := append([]jetstream.PullConsumeOpt{
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			b.onError(fmt.Errorf("jsbroker: consume %s: %w", cfg.Subject, err))
		}),
	}, src.opts...)
	cc, err := src.consumer.Consume(func(msg jetstream.Msg) {
		herr := h(context.Background(), &broker.Message{Subject: msg.Subject(), Header: broker.Header(msg.Headers()), Data: msg.Data()})
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
