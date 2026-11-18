package jsbroker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/jsbroker"
	"github.com/jnszkr/go-asyncapi/broker/natsbroker"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const filter = "events.*"

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
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"events.>"}}); err != nil {
		t.Fatal(err)
	}
	return js
}

func consumer(t *testing.T, js jetstream.JetStream, cfg jetstream.ConsumerConfig) jetstream.Consumer {
	t.Helper()
	cfg.AckPolicy = jetstream.AckExplicitPolicy
	cons, err := js.CreateOrUpdateConsumer(context.Background(), "EVENTS", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cons
}

func ack(context.Context, broker.Inbox) (broker.Settlement, error) { return broker.Ack, nil }

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
	cons := &recording{Consumer: consumer(t, js, jetstream.ConsumerConfig{FilterSubject: filter})}
	var gotFilter, gotQueue string
	f := jsbroker.ConsumerFunc(func(filter, queue string) (jetstream.Consumer, []jetstream.PullConsumeOpt, error) {
		gotFilter, gotQueue = filter, queue
		return cons, []jetstream.PullConsumeOpt{jetstream.PullMaxMessages(10)}, nil
	})
	b := jsbroker.New(js, jsbroker.WithConsumers(f))
	if b.JetStream() != js {
		t.Error("JetStream is not the context given")
	}
	sub, err := b.Subscribe(filter, "workers", ack)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if gotFilter != filter || gotQueue != "workers" {
		t.Errorf("factory got %q, %q", gotFilter, gotQueue)
	}
	if len(cons.opts) != 1 {
		t.Errorf("Consume got %d options, want the factory's 1", len(cons.opts))
	}
}

func TestExistingPassesOptions(t *testing.T) {
	js := setup(t)
	cons := &recording{Consumer: consumer(t, js, jetstream.ConsumerConfig{FilterSubject: filter})}
	b := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons, jetstream.PullMaxMessages(10))))
	sub, err := b.Subscribe(filter, "", ack)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if len(cons.opts) != 1 {
		t.Errorf("Consume got %d options, want 1", len(cons.opts))
	}
}

func TestFactoryErrorFails(t *testing.T) {
	boom := errors.New("boom")
	f := jsbroker.ConsumerFunc(func(string, string) (jetstream.Consumer, []jetstream.PullConsumeOpt, error) {
		return nil, nil, boom
	})
	_, err := jsbroker.New(setup(t), jsbroker.WithConsumers(f)).Subscribe(filter, "", ack)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

func TestSubscribeWithoutConsumerFails(t *testing.T) {
	_, err := jsbroker.New(setup(t)).Subscribe(filter, "", ack)
	if !errors.Is(err, jsbroker.ErrNoConsumer) {
		t.Fatalf("err = %v, want ErrNoConsumer", err)
	}
}

// failingConsume is a consumer whose Consume fails.
type failingConsume struct{ jetstream.Consumer }

func (failingConsume) Consume(jetstream.MessageHandler, ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	return nil, errors.New("consume failed")
}

func (failingConsume) CachedInfo() *jetstream.ConsumerInfo { return nil }

func TestConsumeErrorFails(t *testing.T) {
	b := jsbroker.New(setup(t), jsbroker.WithConsumers(jsbroker.Existing(failingConsume{})))
	if _, err := b.Subscribe(filter, "", ack); err == nil || err.Error() != "consume failed" {
		t.Fatalf("err = %v, want the Consume error", err)
	}
}

// TestFilterOutsideSubjectFails checks that a consumer delivering subjects
// the subscription cannot parse is rejected instead of terminating them.
func TestFilterOutsideSubjectFails(t *testing.T) {
	js := setup(t)
	for _, f := range []string{"events.>", "other.*", "events.a.b", "events"} {
		cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: f})
		if _, err := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons))).Subscribe(filter, "", ack); err == nil {
			t.Errorf("filter %q: expected an error", f)
		}
	}
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubjects: []string{"events.a", "other.b"}})
	if _, err := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons))).Subscribe(filter, "", ack); err == nil {
		t.Error("filters with one outside: expected an error")
	}
	for _, cfg := range []jetstream.ConsumerConfig{
		{},
		{FilterSubject: "events.*"},
		{FilterSubject: "events.a"},
		{FilterSubjects: []string{"events.a", "events.b"}},
	} {
		cons := consumer(t, js, cfg)
		sub, err := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons))).Subscribe(filter, "", ack)
		if err != nil {
			t.Errorf("%+v: %v", cfg, err)
			continue
		}
		sub.Unsubscribe()
	}
	// A literal token in the subscription needs the same token in the filter.
	cons = consumer(t, js, jetstream.ConsumerConfig{FilterSubject: "events.a"})
	if _, err := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons))).Subscribe("events.b", "", ack); err == nil {
		t.Error("filter events.a on events.b: expected an error")
	}
}

// pending waits until the consumer has nothing pending or waiting for an ack.
func pending(t *testing.T, cons jetstream.Consumer) {
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

// TestSettlements checks that Nak and unknown settlements redeliver, and that
// Ack and Term do not.
func TestSettlements(t *testing.T) {
	js := setup(t)
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: filter, AckWait: time.Second})
	var errs atomic.Int32
	b := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons)), jsbroker.WithErrorHandler(func(error) { errs.Add(1) }))

	attempts := map[string]*atomic.Int32{}
	for _, s := range []string{"events.ack", "events.term", "events.nak", "events.zero"} {
		attempts[s] = &atomic.Int32{}
	}
	sub, err := b.Subscribe(filter, "", func(_ context.Context, m broker.Inbox) (broker.Settlement, error) {
		n := attempts[m.Subject()].Add(1)
		switch {
		case m.Subject() == "events.ack":
			return broker.Ack, nil
		case m.Subject() == "events.term":
			return broker.Term, errors.New("never")
		case n > 1:
			return broker.Ack, nil
		case m.Subject() == "events.nak":
			return broker.Nak, errors.New("later")
		default:
			return 0, nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	ctx := context.Background()
	for s := range attempts {
		if err := b.Publish(ctx, s, nil, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	pending(t, cons)
	time.Sleep(1500 * time.Millisecond) // past AckWait: anything unsettled is redelivered
	pending(t, cons)
	for s, want := range map[string]int32{"events.ack": 1, "events.term": 1, "events.nak": 2, "events.zero": 2} {
		if n := attempts[s].Load(); n != want {
			t.Errorf("%s: %d deliveries, want %d", s, n, want)
		}
	}
	if n := errs.Load(); n != 2 {
		t.Errorf("reported %d errors, want 2", n)
	}
}

func TestInboxCarriesTheMessage(t *testing.T) {
	js := setup(t)
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: filter})
	b := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons)))
	type delivery struct {
		subject string
		headers map[string][]string
		data    string
	}
	got := make(chan delivery, 2)
	sub, err := b.Subscribe(filter, "", func(_ context.Context, m broker.Inbox) (broker.Settlement, error) {
		got <- delivery{m.Subject(), m.Headers(), string(m.Data())}
		return broker.Ack, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	ctx := context.Background()
	if err := b.Publish(ctx, "events.a", map[string][]string{"K": {"v"}}, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if d := <-got; d.subject != "events.a" || d.data != "one" || d.headers["K"][0] != "v" {
		t.Errorf("received %+v", d)
	}
	if err := b.Publish(ctx, "events.b", nil, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if d := <-got; d.headers == nil {
		t.Errorf("headers of a message without any are nil")
	}
}

func TestUnsubscribeStopsConsuming(t *testing.T) {
	js := setup(t)
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: filter})
	b := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons)))
	var n atomic.Int32
	sub, err := b.Subscribe(filter, "", func(context.Context, broker.Inbox) (broker.Settlement, error) {
		n.Add(1)
		return broker.Ack, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), "events.a", nil, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if n.Load() != 0 {
		t.Error("message delivered after Unsubscribe")
	}
}

// TestInterceptorDeadLetters moves a message that keeps failing to another
// stream, and one that can never be handled straight away.
func TestInterceptorDeadLetters(t *testing.T) {
	js := setup(t)
	ctx := context.Background()
	dlq, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "DLQ", Subjects: []string{"dlq.>"}})
	if err != nil {
		t.Fatal(err)
	}
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: filter, AckWait: time.Second})
	deadLetter := func(next jsbroker.Deliver) jsbroker.Deliver {
		return func(ctx context.Context, msg jetstream.Msg) (broker.Settlement, error) {
			s, err := next(ctx, msg)
			if s == broker.Ack {
				return s, err
			}
			md, merr := msg.Metadata()
			if merr != nil {
				return broker.Nak, merr
			}
			if md.NumDelivered < 2 && s != broker.Term {
				return s, err
			}
			_, perr := js.Publish(ctx, "dlq."+msg.Subject(), msg.Data())
			return broker.Ack, perr
		}
	}
	var attempts atomic.Int32
	b := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons)), jsbroker.WithInterceptors(deadLetter))
	sub, err := b.Subscribe(filter, "", func(_ context.Context, m broker.Inbox) (broker.Settlement, error) {
		if m.Subject() == "events.bad" {
			return broker.Term, errors.New("malformed")
		}
		attempts.Add(1)
		return broker.Nak, errors.New("always fails")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	for _, s := range []string{"events.bad", "events.good"} {
		if err := b.Publish(ctx, s, nil, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	pending(t, cons)
	time.Sleep(1500 * time.Millisecond)
	pending(t, cons)
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
// not redelivered, even though the delivery says Nak.
func TestInterceptorSettles(t *testing.T) {
	js := setup(t)
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: filter, AckWait: 200 * time.Millisecond})
	termErrs := make(chan error, 8)
	terminate := func(next jsbroker.Deliver) jsbroker.Deliver {
		return func(ctx context.Context, msg jetstream.Msg) (broker.Settlement, error) {
			s, err := next(ctx, msg)
			termErrs <- msg.Term()
			return s, err
		}
	}
	b := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons)), jsbroker.WithInterceptors(terminate))
	sub, err := b.Subscribe(filter, "", func(context.Context, broker.Inbox) (broker.Settlement, error) {
		return broker.Nak, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	if err := b.Publish(context.Background(), "events.a", nil, nil); err != nil {
		t.Fatal(err)
	}
	pending(t, cons)
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
	cons := consumer(t, js, jetstream.ConsumerConfig{FilterSubject: filter})
	type key struct{}
	with := func(name string) jsbroker.Interceptor {
		return func(next jsbroker.Deliver) jsbroker.Deliver {
			return func(ctx context.Context, msg jetstream.Msg) (broker.Settlement, error) {
				path, _ := ctx.Value(key{}).(string)
				return next(context.WithValue(ctx, key{}, path+name), msg)
			}
		}
	}
	got := make(chan string, 1)
	b := jsbroker.New(js, jsbroker.WithConsumers(jsbroker.Existing(cons)),
		jsbroker.WithInterceptors(with("a")), jsbroker.WithInterceptors(with("b"), with("c")))
	sub, err := b.Subscribe(filter, "", func(ctx context.Context, _ broker.Inbox) (broker.Settlement, error) {
		got <- ctx.Value(key{}).(string)
		return broker.Ack, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	if err := b.Publish(context.Background(), "events.a", nil, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case path := <-got:
		if path != "abc" {
			t.Errorf("path = %q, want abc", path)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestPublishWithoutStreamFails(t *testing.T) {
	if err := jsbroker.New(setup(t)).Publish(context.Background(), "nowhere", nil, nil); err == nil {
		t.Fatal("expected an error publishing to a subject no stream captures")
	}
}

// TestRequest sends requests over core NATS to a natsbroker responder.
func TestRequest(t *testing.T) {
	js := setup(t)
	nc := js.Conn()
	if _, err := natsbroker.New(nc).Reply("svc", "", func(_ context.Context, m broker.Inbox) ([]byte, error) {
		if string(m.Data()) == "fail" {
			return nil, errors.New("cannot")
		}
		return append([]byte("re:"), m.Data()...), nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := nc.Subscribe("slow", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	b := jsbroker.New(js, jsbroker.WithRequestTimeout(50*time.Millisecond))
	ctx := context.Background()
	body, h, err := b.Request(ctx, "svc", map[string][]string{"K": {"v"}}, []byte("hi"))
	if err != nil || string(body) != "re:hi" {
		t.Fatalf("Request = %q, %v, %v", body, h, err)
	}
	body, _, err = b.Request(ctx, "svc", nil, []byte("fail"))
	var remote *broker.RemoteError
	if !errors.As(err, &remote) || remote.Code != broker.CodeInternal || body != nil {
		t.Errorf("err = %v, body = %q, want RemoteError 500", err, body)
	}
	start := time.Now()
	if _, _, err := b.Request(ctx, "slow", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("took %v, want about the 50ms timeout", d)
	}
	deadline, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	start = time.Now()
	if _, _, err := jsbroker.New(js, jsbroker.WithRequestTimeout(time.Hour)).Request(deadline, "slow", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("took %v, want about the 20ms deadline", d)
	}
}
