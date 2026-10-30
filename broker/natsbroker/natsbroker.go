// Package natsbroker implements broker.Broker on a NATS connection.
package natsbroker

import (
	"context"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/nats-io/nats.go"
)

// Broker is a broker.Broker backed by a NATS connection.
type Broker struct {
	nc *nats.Conn
}

var _ broker.Broker = (*Broker)(nil)

// New returns a Broker using nc.
func New(nc *nats.Conn) *Broker { return &Broker{nc: nc} }

// Conn returns the underlying connection.
func (b *Broker) Conn() *nats.Conn { return b.nc }

func (b *Broker) Publish(_ context.Context, m *broker.Message) error {
	return b.nc.PublishMsg(toNATS(m))
}

func (b *Broker) Request(ctx context.Context, m *broker.Message) (*broker.Message, error) {
	rm, err := b.nc.RequestMsgWithContext(ctx, toNATS(m))
	if err != nil {
		return nil, err
	}
	return fromNATS(rm), nil
}

// Subscribe uses a core NATS subscription, which has no acknowledgements:
// the error returned by h is ignored.
func (b *Broker) Subscribe(subject, queue string, h func(*broker.Message) error) (broker.Subscription, error) {
	cb := func(m *nats.Msg) { _ = h(fromNATS(m)) }
	if queue != "" {
		return b.nc.QueueSubscribe(subject, queue, cb)
	}
	return b.nc.Subscribe(subject, cb)
}

func toNATS(m *broker.Message) *nats.Msg {
	return &nats.Msg{Subject: m.Subject, Reply: m.Reply, Header: nats.Header(m.Header), Data: m.Data}
}

func fromNATS(m *nats.Msg) *broker.Message {
	return &broker.Message{Subject: m.Subject, Reply: m.Reply, Header: broker.Header(m.Header), Data: m.Data}
}
