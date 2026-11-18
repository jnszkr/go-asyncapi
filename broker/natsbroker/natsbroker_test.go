package natsbroker_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/natsbroker"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func conn(t testing.TB) *nats.Conn {
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
	return nc
}

// errs collects the errors a Broker reports.
type errs struct {
	mu   sync.Mutex
	list []error
	seen chan struct{}
}

func newErrs() *errs { return &errs{seen: make(chan struct{}, 16)} }

func (e *errs) add(err error) {
	e.mu.Lock()
	e.list = append(e.list, err)
	e.mu.Unlock()
	e.seen <- struct{}{}
}

func (e *errs) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-e.seen:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an error")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.list[len(e.list)-1]
}

func TestPublishSubscribe(t *testing.T) {
	nc := conn(t)
	reported := newErrs()
	b := natsbroker.New(nc, natsbroker.WithErrorHandler(reported.add))
	if b.Conn() != nc {
		t.Error("Conn is not the connection")
	}
	type delivery struct {
		subject string
		headers map[string][]string
		data    string
	}
	got := make(chan delivery, 2)
	boom := errors.New("boom")
	sub, err := b.Subscribe("greet.*", "", func(ctx context.Context, m broker.Inbox) (broker.Settlement, error) {
		if ctx == nil {
			t.Error("nil context")
		}
		got <- delivery{m.Subject(), m.Headers(), string(m.Data())}
		if string(m.Data()) == "fail" {
			return broker.Nak, boom
		}
		return broker.Ack, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if q := sub.(*nats.Subscription).Queue; q != "" {
		t.Errorf("queue = %q, want none", q)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := b.Publish(ctx, "greet.en", map[string][]string{"K": {"v"}}, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	d := <-got
	if d.subject != "greet.en" || d.data != "hi" || d.headers["K"][0] != "v" {
		t.Errorf("received %+v", d)
	}
	if err := b.Publish(ctx, "greet.en", nil, []byte("fail")); err != nil {
		t.Fatal(err)
	}
	if d := <-got; d.headers == nil || len(d.headers) != 0 {
		t.Errorf("headers of a message without any = %#v, want empty", d.headers)
	}
	if err := reported.wait(t); !errors.Is(err, boom) {
		t.Errorf("reported %v, want boom", err)
	}
}

// TestQueueSubscribersShareMessages checks that a queue group delivers each
// message to one member, not to all of them.
func TestQueueSubscribersShareMessages(t *testing.T) {
	nc := conn(t)
	b := natsbroker.New(nc)
	var got atomic.Int32
	for range 2 {
		sub, err := b.Subscribe("work", "workers", func(context.Context, broker.Inbox) (broker.Settlement, error) {
			got.Add(1)
			return broker.Ack, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if q := sub.(*nats.Subscription).Queue; q != "workers" {
			t.Errorf("queue = %q", q)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	const n = 10
	for range n {
		if err := b.Publish(context.Background(), "work", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for got.Load() < n && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // time for duplicates to arrive
	if v := got.Load(); v != n {
		t.Errorf("handled %d messages, want %d", v, n)
	}
}

type malformed struct{}

func (malformed) Error() string   { return "bad request" }
func (malformed) Malformed() bool { return true }

func TestRequestReply(t *testing.T) {
	nc := conn(t)
	reported := newErrs()
	b := natsbroker.New(nc, natsbroker.WithErrorHandler(reported.add))
	sub, err := b.Reply("svc", "q", func(_ context.Context, m broker.Inbox) ([]byte, error) {
		switch string(m.Data()) {
		case "bad":
			return nil, malformed{}
		case "fail":
			return nil, errors.New("cannot")
		}
		return []byte(m.Headers()["Who"][0] + ":" + string(m.Data())), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if q := sub.(*nats.Subscription).Queue; q != "q" {
		t.Errorf("queue = %q", q)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background() // no deadline: the Broker adds one
	body, _, err := b.Request(ctx, "svc", map[string][]string{"Who": {"ann"}}, []byte("hi"))
	if err != nil || string(body) != "ann:hi" {
		t.Fatalf("Request = %q, %v", body, err)
	}

	for data, code := range map[string]string{"bad": broker.CodeBadRequest, "fail": broker.CodeInternal} {
		body, h, err := b.Request(ctx, "svc", nil, []byte(data))
		var remote *broker.RemoteError
		if !errors.As(err, &remote) || remote.Code != code {
			t.Errorf("%s: err = %v, want RemoteError %s", data, err, code)
		}
		if body != nil || h[broker.HeaderServiceErrorCode][0] != code {
			t.Errorf("%s: body = %q, headers = %v", data, body, h)
		}
		if err := reported.wait(t); err == nil {
			t.Errorf("%s: no error reported", data)
		}
	}
}

func TestRequestWithoutResponders(t *testing.T) {
	b := natsbroker.New(conn(t))
	if _, _, err := b.Request(context.Background(), "nobody", nil, nil); !errors.Is(err, nats.ErrNoResponders) {
		t.Errorf("err = %v, want ErrNoResponders", err)
	}
}

func TestRequestTimeout(t *testing.T) {
	nc := conn(t)
	// A subscriber that never answers.
	if _, err := nc.Subscribe("slow", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	b := natsbroker.New(nc, natsbroker.WithRequestTimeout(50*time.Millisecond))
	start := time.Now()
	if _, _, err := b.Request(context.Background(), "slow", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("took %v, want about the 50ms timeout", d)
	}
	// A deadline of the caller's own is kept.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	b = natsbroker.New(nc, natsbroker.WithRequestTimeout(time.Hour))
	start = time.Now()
	if _, _, err := b.Request(ctx, "slow", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("took %v, want about the 20ms deadline", d)
	}
}

func TestReplyToPublishWithoutReplySubject(t *testing.T) {
	nc := conn(t)
	reported := newErrs()
	b := natsbroker.New(nc, natsbroker.WithErrorHandler(reported.add))
	called := false
	if _, err := b.Reply("svc", "", func(context.Context, broker.Inbox) ([]byte, error) {
		called = true
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), "svc", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := reported.wait(t); !strings.Contains(err.Error(), "svc: request without reply subject") {
		t.Errorf("reported %v", err)
	}
	if called {
		t.Error("handler called for a message nobody waits an answer to")
	}
}

func TestReplyFailureIsReported(t *testing.T) {
	nc := conn(t)
	reported := newErrs()
	b := natsbroker.New(nc, natsbroker.WithErrorHandler(reported.add))
	if _, err := b.Reply("svc", "", func(context.Context, broker.Inbox) ([]byte, error) {
		return make([]byte, 2<<20), nil // over the server's max payload
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := nc.PublishRequest("svc", "inbox.x", nil); err != nil {
		t.Fatal(err)
	}
	if err := reported.wait(t); !strings.Contains(err.Error(), "svc: reply:") {
		t.Errorf("reported %v", err)
	}
}

func TestErrorsAreDiscardedByDefault(t *testing.T) {
	nc := conn(t)
	b := natsbroker.New(nc)
	done := make(chan struct{})
	if _, err := b.Subscribe("s", "", func(context.Context, broker.Inbox) (broker.Settlement, error) {
		defer close(done)
		return broker.Nak, errors.New("dropped")
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), "s", nil, nil); err != nil {
		t.Fatal(err)
	}
	<-done
}
