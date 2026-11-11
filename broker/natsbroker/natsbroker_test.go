package natsbroker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/natsbroker"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

type greeting struct {
	Text string `json:"text"`
}

type length struct {
	N int `json:"n"`
}

var (
	greetings = broker.Channel[greeting]{Address: "greetings"}
	dead      = broker.Channel[greeting]{Address: "dead.greetings"}
	measure   = broker.Channel[greeting]{Address: "measure"}
)

func setup(t *testing.T) *natsbroker.Broker {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return natsbroker.New(nc)
}

// TestReceiveInterceptorDeadLetters forwards a message whose handler failed,
// which core NATS would otherwise lose.
func TestReceiveInterceptorDeadLetters(t *testing.T) {
	b := setup(t)
	c := broker.New(b, broker.WithReceiveInterceptors(func(next broker.ReceiveFunc) broker.ReceiveFunc {
		return func(ctx context.Context, m *broker.Message) error {
			err := next(ctx, m)
			if err == nil {
				return nil
			}
			return b.Publish(ctx, &broker.Message{Subject: "dead." + m.Subject, Header: m.Header, Data: m.Data})
		}
	}))
	if _, err := broker.Subscribe(c, greetings, func(context.Context, broker.Received[greeting]) error {
		return errors.New("fails")
	}); err != nil {
		t.Fatal(err)
	}
	got := make(chan greeting, 1)
	if _, err := broker.Subscribe(broker.New(b), dead, func(_ context.Context, m broker.Received[greeting]) error {
		got <- m.Payload
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), broker.New(b), greetings, nil, greeting{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	select {
	case g := <-got:
		if g.Text != "hi" {
			t.Errorf("dead letter = %+v", g)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the dead letter")
	}
}

// TestRequestReplyInterceptors passes a header through a request interceptor,
// and back through the responder's publish interceptor.
func TestRequestReplyInterceptors(t *testing.T) {
	b := setup(t)
	server := broker.New(b,
		broker.WithReceiveInterceptors(func(next broker.ReceiveFunc) broker.ReceiveFunc {
			return func(ctx context.Context, m *broker.Message) error {
				if m.Header.Get("Auth") != "secret" {
					t.Errorf("request header = %v", m.Header)
				}
				return next(ctx, m)
			}
		}),
		broker.WithPublishInterceptors(func(next broker.PublishFunc) broker.PublishFunc {
			return func(ctx context.Context, m *broker.Message) error {
				m.Header.Set("Served-By", "test")
				return next(ctx, m)
			}
		}))
	if _, err := broker.Respond(server, measure, func(_ context.Context, m broker.Received[greeting]) (length, error) {
		return length{N: len(m.Payload.Text)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	var servedBy string
	client := broker.ForRequester(b, broker.WithRequestInterceptors(func(next broker.RequestFunc) broker.RequestFunc {
		return func(ctx context.Context, m *broker.Message) (*broker.Message, error) {
			m.Header.Set("Auth", "secret")
			r, err := next(ctx, m)
			if err == nil {
				servedBy = r.Header.Get("Served-By")
			}
			return r, err
		}
	}))
	resp, err := broker.Request[greeting, length](context.Background(), client, measure, nil, greeting{Text: "hello"})
	if err != nil || resp.N != 5 {
		t.Fatalf("Request = %+v, %v", resp, err)
	}
	if servedBy != "test" {
		t.Errorf("Served-By = %q", servedBy)
	}
}

// TestQueueSubscribersShareMessages checks that a queue group delivers each
// message to one member, not to all of them.
func TestQueueSubscribersShareMessages(t *testing.T) {
	c := broker.New(setup(t))
	var got atomic.Int32
	for range 2 {
		if _, err := broker.Subscribe(c, greetings, func(context.Context, broker.Received[greeting]) error {
			got.Add(1)
			return nil
		}, broker.Queue("workers")); err != nil {
			t.Fatal(err)
		}
	}
	const n = 10
	for range n {
		if err := broker.Publish(context.Background(), c, greetings, nil, greeting{}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for got.Load() < n && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	// Give any duplicate deliveries time to arrive.
	time.Sleep(100 * time.Millisecond)
	if v := got.Load(); v != n {
		t.Errorf("handled %d messages, want %d", v, n)
	}
}
