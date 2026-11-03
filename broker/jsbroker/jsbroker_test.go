package jsbroker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/jsbroker"
	"github.com/jnszkr/go-asyncapi/broker/membroker"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type event struct {
	N int `json:"n"`
}

var events = broker.Channel[event]{Address: "events.{kind}"}

func setup(t *testing.T) jetstream.JetStream {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"events.>"}}); err != nil {
		t.Fatal(err)
	}
	return js
}

func consumer(t *testing.T, js jetstream.JetStream, cfg jetstream.ConsumerConfig) jetstream.Consumer {
	t.Helper()
	cons, err := js.CreateOrUpdateConsumer(context.Background(), "EVENTS", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cons
}

func TestSubscribeWithoutConsumerFails(t *testing.T) {
	c := broker.New(jsbroker.New(setup(t)))
	_, err := broker.Subscribe(c, events, func(context.Context, broker.Received[event]) error { return nil })
	if !errors.Is(err, jsbroker.ErrNoConsumer) {
		t.Fatalf("err = %v, want ErrNoConsumer", err)
	}
}

// TestFilterOutsideSubjectFails checks that a consumer delivering subjects
// the channel cannot parse is rejected instead of terminating them.
func TestFilterOutsideSubjectFails(t *testing.T) {
	js := setup(t)
	c := broker.New(jsbroker.New(js))
	h := func(context.Context, broker.Received[event]) error { return nil }
	for _, filter := range []string{"events.>", "other.*", "events.a.b"} {
		cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: filter, AckPolicy: jetstream.AckExplicitPolicy})
		if _, err := broker.Subscribe(c, events, h, jsbroker.From(cons)); err == nil {
			t.Errorf("filter %q: expected an error", filter)
		}
	}
	for _, filter := range []string{"", "events.*", "events.a"} {
		cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: filter, AckPolicy: jetstream.AckExplicitPolicy})
		sub, err := broker.Subscribe(c, events, h, jsbroker.From(cons))
		if err != nil {
			t.Errorf("filter %q: %v", filter, err)
			continue
		}
		sub.Unsubscribe()
	}
}

func TestOtherBrokersRejectConsumers(t *testing.T) {
	cons := consumer(t, setup(t), jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
	c := broker.New(membroker.New())
	_, err := broker.Subscribe(c, events, func(context.Context, broker.Received[event]) error { return nil }, jsbroker.From(cons))
	if err == nil {
		t.Fatal("expected membroker to reject a JetStream consumer")
	}
}

// TestAcks checks that a handler error redelivers the message and a
// malformed message is terminated rather than retried.
func TestAcks(t *testing.T) {
	js := setup(t)
	cons := consumer(t, js, jetstream.ConsumerConfig{
		Durable:       "workers",
		FilterSubject: events.Pattern(),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Second,
	})
	c := broker.New(jsbroker.New(js))

	var attempts atomic.Int32
	done := make(chan event, 1)
	sub, err := broker.Subscribe(c, events, func(_ context.Context, m broker.Received[event]) error {
		if attempts.Add(1) == 1 {
			return errors.New("try again")
		}
		done <- m.Payload
		return nil
	}, jsbroker.From(cons))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	ctx := context.Background()
	// Not JSON: decoding fails with ErrMalformed, so it is never retried.
	if _, err := js.Publish(ctx, "events.bad", []byte("{")); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(ctx, c, events, broker.Params{"kind": "good"}, event{N: 7}); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-done:
		if e.N != 7 {
			t.Errorf("event = %+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for redelivery")
	}
	if n := attempts.Load(); n != 2 {
		t.Errorf("handler attempts = %d, want 2 (one failure, one redelivery)", n)
	}

	// Everything is settled: nothing pending, nothing waiting for an ack.
	deadline := time.Now().Add(3 * time.Second)
	for {
		info, err := cons.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.NumAckPending == 0 && info.NumPending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer not settled: %d ack pending, %d pending", info.NumAckPending, info.NumPending)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestPublishWithoutStreamFails(t *testing.T) {
	c := broker.New(jsbroker.New(setup(t)))
	nowhere := broker.Channel[event]{Address: "nowhere"}
	if err := broker.Publish(context.Background(), c, nowhere, nil, event{}); err == nil {
		t.Fatal("expected an error publishing to a subject no stream captures")
	}
}
