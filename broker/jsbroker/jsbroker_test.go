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

// recording is a consumer that records the options given to Consume.
type recording struct {
	jetstream.Consumer
	opts []jetstream.PullConsumeOpt
}

func (r *recording) Consume(h jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	r.opts = opts
	return r.Consumer.Consume(h, opts...)
}

// TestFactory checks that the factory sees the subscription, and that its
// options reach Consume with none added by the Broker.
func TestFactory(t *testing.T) {
	js := setup(t)
	cons := &recording{Consumer: consumer(t, js, jetstream.ConsumerConfig{FilterSubject: events.Pattern(), AckPolicy: jetstream.AckExplicitPolicy})}
	var got broker.SubConfig
	f := jsbroker.ConsumerFunc(func(cfg broker.SubConfig) (jetstream.Consumer, []jetstream.PullConsumeOpt, error) {
		got = cfg
		return cons, []jetstream.PullConsumeOpt{jetstream.PullMaxMessages(10)}, nil
	})
	c := broker.New(jsbroker.New(js))
	h := func(context.Context, broker.Received[event]) error { return nil }
	sub, err := broker.Subscribe(c, events, h, jsbroker.FromFactory(f), broker.Queue("workers"))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if got.Subject != events.Pattern() || got.Queue != "workers" {
		t.Errorf("factory got %+v", got)
	}
	if len(cons.opts) != 1 {
		t.Errorf("Consume got %d options, want the factory's 1", len(cons.opts))
	}
}

func TestFactoryErrorFails(t *testing.T) {
	boom := errors.New("boom")
	f := jsbroker.ConsumerFunc(func(broker.SubConfig) (jetstream.Consumer, []jetstream.PullConsumeOpt, error) {
		return nil, nil, boom
	})
	c := broker.New(jsbroker.New(setup(t)))
	_, err := broker.Subscribe(c, events, func(context.Context, broker.Received[event]) error { return nil }, jsbroker.FromFactory(f))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

// TestFromPassesOptions checks that options given with an existing consumer
// reach Consume.
func TestFromPassesOptions(t *testing.T) {
	js := setup(t)
	cons := &recording{Consumer: consumer(t, js, jetstream.ConsumerConfig{FilterSubject: events.Pattern(), AckPolicy: jetstream.AckExplicitPolicy})}
	c := broker.New(jsbroker.New(js))
	h := func(context.Context, broker.Received[event]) error { return nil }
	sub, err := broker.Subscribe(c, events, h, jsbroker.From(cons, jetstream.PullMaxMessages(10)))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if len(cons.opts) != 1 {
		t.Errorf("Consume got %d options, want 1", len(cons.opts))
	}
}

// settled waits until the consumer has nothing pending or waiting for an ack.
func settled(t *testing.T, cons jetstream.Consumer) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		info, err := cons.Info(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if info.NumAckPending == 0 && info.NumPending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer not settled: %d ack pending, %d pending", info.NumAckPending, info.NumPending)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestInterceptorDeadLetters moves a message that keeps failing to another
// stream, and one that cannot be decoded straight away.
func TestInterceptorDeadLetters(t *testing.T) {
	js := setup(t)
	ctx := context.Background()
	dlq, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "DLQ", Subjects: []string{"dlq.>"}})
	if err != nil {
		t.Fatal(err)
	}
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: events.Pattern(), AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second})
	deadLetter := func(next jsbroker.Deliver) jsbroker.Deliver {
		return func(ctx context.Context, msg jetstream.Msg) error {
			err := next(ctx, msg)
			if err == nil {
				return nil
			}
			md, merr := msg.Metadata()
			if merr != nil {
				return merr
			}
			if md.NumDelivered < 2 && !errors.Is(err, broker.ErrMalformed) {
				return err
			}
			_, perr := js.Publish(ctx, "dlq."+msg.Subject(), msg.Data())
			return perr
		}
	}
	var attempts atomic.Int32
	c := broker.New(jsbroker.New(js))
	sub, err := broker.Subscribe(c, events, func(context.Context, broker.Received[event]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}, jsbroker.FromFactory(jsbroker.Existing(cons), deadLetter))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	if _, err := js.Publish(ctx, "events.bad", []byte("{")); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(ctx, c, events, broker.Params{"kind": "good"}, event{N: 7}); err != nil {
		t.Fatal(err)
	}
	settled(t, cons)
	if n := attempts.Load(); n != 2 {
		t.Errorf("handler attempts = %d, want 2", n)
	}
	info, err := dlq.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 2 {
		t.Errorf("dead letters = %d, want 2", info.State.Msgs)
	}
}

// TestInterceptorSettles checks that a message an Interceptor terminated is
// not redelivered, even though the delivery returns an error.
func TestInterceptorSettles(t *testing.T) {
	js := setup(t)
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: events.Pattern(), AckPolicy: jetstream.AckExplicitPolicy, AckWait: 200 * time.Millisecond})
	termErrs := make(chan error, 8)
	terminate := func(next jsbroker.Deliver) jsbroker.Deliver {
		return func(ctx context.Context, msg jetstream.Msg) error {
			err := next(ctx, msg)
			termErrs <- msg.Term()
			return err
		}
	}
	c := broker.New(jsbroker.New(js))
	sub, err := broker.Subscribe(c, events, func(context.Context, broker.Received[event]) error {
		return errors.New("fails")
	}, jsbroker.FromFactory(jsbroker.Existing(cons), terminate))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	if err := broker.Publish(context.Background(), c, events, broker.Params{"kind": "a"}, event{}); err != nil {
		t.Fatal(err)
	}
	settled(t, cons)
	time.Sleep(500 * time.Millisecond) // longer than AckWait: a nak would have redelivered
	if n := len(termErrs); n != 1 {
		t.Fatalf("deliveries = %d, want 1", n)
	}
	if err := <-termErrs; err != nil {
		t.Errorf("Term: %v", err)
	}
}

// TestInterceptorOrderAndContext checks that the first interceptor is the
// outermost and that the context it passes on reaches the handler.
func TestInterceptorOrderAndContext(t *testing.T) {
	js := setup(t)
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: events.Pattern(), AckPolicy: jetstream.AckExplicitPolicy})
	type key struct{}
	with := func(name string) jsbroker.Interceptor {
		return func(next jsbroker.Deliver) jsbroker.Deliver {
			return func(ctx context.Context, msg jetstream.Msg) error {
				path, _ := ctx.Value(key{}).(string)
				return next(context.WithValue(ctx, key{}, path+name), msg)
			}
		}
	}
	got := make(chan string, 1)
	c := broker.New(jsbroker.New(js))
	sub, err := broker.Subscribe(c, events, func(ctx context.Context, _ broker.Received[event]) error {
		got <- ctx.Value(key{}).(string)
		return nil
	}, jsbroker.FromFactory(jsbroker.Existing(cons), with("a"), with("b")))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	if err := broker.Publish(context.Background(), c, events, broker.Params{"kind": "a"}, event{}); err != nil {
		t.Fatal(err)
	}
	select {
	case path := <-got:
		if path != "ab" {
			t.Errorf("path = %q, want ab", path)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

// TestClientInterceptorDecidesAck checks that the error a transport-agnostic
// interceptor returns settles the message.
func TestClientInterceptorDecidesAck(t *testing.T) {
	js := setup(t)
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: events.Pattern(), AckPolicy: jetstream.AckExplicitPolicy, AckWait: 200 * time.Millisecond})
	var attempts atomic.Int32
	c := broker.New(jsbroker.New(js), broker.WithReceiveInterceptors(func(next broker.ReceiveFunc) broker.ReceiveFunc {
		return func(ctx context.Context, m *broker.Message) error {
			_ = next(ctx, m)
			return nil
		}
	}))
	sub, err := broker.Subscribe(c, events, func(context.Context, broker.Received[event]) error {
		attempts.Add(1)
		return errors.New("fails")
	}, jsbroker.From(cons))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	if err := broker.Publish(context.Background(), c, events, broker.Params{"kind": "a"}, event{}); err != nil {
		t.Fatal(err)
	}
	settled(t, cons)
	time.Sleep(500 * time.Millisecond) // longer than AckWait
	if n := attempts.Load(); n != 1 {
		t.Errorf("handler attempts = %d, want 1", n)
	}
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
