// Package natsbroker implements broker.Broker on a NATS connection.
package natsbroker

import (
	"context"
	"fmt"

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
// the error returned by h is ignored. It supports no SubConfig.Source. h gets
// a background context; use broker.WithReceiveInterceptors to derive one from
// the message, or to act on the error.
func (b *Broker) Subscribe(cfg broker.SubConfig, h func(context.Context, *broker.Message) error) (broker.Subscription, error) {
	if cfg.Source != nil {
		return nil, fmt.Errorf("natsbroker: unsupported source %T", cfg.Source)
	}
	cb := func(m *nats.Msg) { _ = h(context.Background(), fromNATS(m)) }
	if cfg.Queue != "" {
		return b.nc.QueueSubscribe(cfg.Subject, cfg.Queue, cb)
	}
	return b.nc.Subscribe(cfg.Subject, cb)
}

func toNATS(m *broker.Message) *nats.Msg {
	return &nats.Msg{Subject: m.Subject, Reply: m.Reply, Header: nats.Header(m.Header), Data: m.Data}
}

func fromNATS(m *nats.Msg) *broker.Message {
	return &broker.Message{Subject: m.Subject, Reply: m.Reply, Header: broker.Header(m.Header), Data: m.Data}
}
