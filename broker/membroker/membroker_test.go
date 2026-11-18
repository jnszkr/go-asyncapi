package membroker_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/membroker"
)

type received struct {
	name    string
	subject string
	headers map[string][]string
	data    string
}

func collect(b *membroker.Broker, got *[]received, name, filter, queue string, s broker.Settlement, err error) broker.Subscription {
	sub, _ := b.Subscribe(filter, queue, func(ctx context.Context, m broker.Inbox) (broker.Settlement, error) {
		if ctx == nil {
			panic("nil context")
		}
		*got = append(*got, received{name, m.Subject(), m.Headers(), string(m.Data())})
		m.Headers()["Mutated"] = []string{"yes"}
		m.Data()[0] = 'X'
		return s, err
	})
	return sub
}

func names(got []received) []string {
	var out []string
	for _, r := range got {
		out = append(out, r.name)
	}
	return out
}

func TestPublishMatchesAndCopies(t *testing.T) {
	b := membroker.New()
	var got []received
	collect(b, &got, "star", "a.*.c", "", broker.Ack, nil)
	collect(b, &got, "exact", "a.b.c", "", broker.Ack, nil)
	collect(b, &got, "short", "a.b", "", broker.Ack, nil)
	collect(b, &got, "long", "a.b.c.d", "", broker.Ack, nil)
	collect(b, &got, "other", "a.x.d", "", broker.Ack, nil)

	headers := map[string][]string{"K": {"v"}}
	body := []byte("data")
	if err := b.Publish(context.Background(), "a.b.c", headers, body); err != nil {
		t.Fatal(err)
	}
	if want := []string{"star", "exact"}; !slices.Equal(names(got), want) {
		t.Fatalf("delivered to %v, want %v", names(got), want)
	}
	for _, r := range got {
		if r.subject != "a.b.c" || r.data != "data" || r.headers["K"][0] != "v" {
			t.Errorf("%s received %+v", r.name, r)
		}
	}
	// Each receiver got its own copy: the first one's changes reached
	// neither the second nor the sender.
	if _, ok := headers["Mutated"]; ok || string(body) != "data" {
		t.Errorf("sender's message changed: %v %q", headers, body)
	}
	if got[1].data != "data" {
		t.Errorf("second receiver saw %q", got[1].data)
	}
}

func TestQueueGroupsRoundRobin(t *testing.T) {
	b := membroker.New()
	var got []received
	collect(b, &got, "q1", "s", "q", broker.Ack, nil)
	collect(b, &got, "q2", "s", "q", broker.Ack, nil)
	collect(b, &got, "r1", "s", "r", broker.Ack, nil)
	collect(b, &got, "plain", "s", "", broker.Ack, nil)
	for range 3 {
		if err := b.Publish(context.Background(), "s", nil, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"plain", "q1", "r1", "plain", "q2", "r1", "plain", "q1", "r1"}
	if !slices.Equal(names(got), want) {
		t.Errorf("delivered to %v, want %v", names(got), want)
	}
}

func TestUnsubscribe(t *testing.T) {
	b := membroker.New()
	var got []received
	first := collect(b, &got, "first", "s", "", broker.Ack, nil)
	collect(b, &got, "second", "s", "", broker.Ack, nil)
	if err := first.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	_ = b.Publish(context.Background(), "s", nil, []byte("x"))
	if want := []string{"second"}; !slices.Equal(names(got), want) {
		t.Errorf("delivered to %v, want %v", names(got), want)
	}
}

func TestErrorHandler(t *testing.T) {
	var errs []error
	b := membroker.New(membroker.WithErrorHandler(func(err error) { errs = append(errs, err) }))
	var got []received
	boom := errors.New("boom")
	collect(b, &got, "fails", "s", "", broker.Nak, boom)
	collect(b, &got, "ok", "s", "", broker.Ack, nil)
	_ = b.Publish(context.Background(), "s", nil, []byte("x"))
	if len(errs) != 1 || !errors.Is(errs[0], boom) {
		t.Errorf("errors = %v, want [boom]", errs)
	}
	// Without a handler, errors are discarded.
	b = membroker.New()
	collect(b, &got, "fails", "s", "", broker.Nak, boom)
	_ = b.Publish(context.Background(), "s", nil, []byte("x"))
}

func TestRequestReply(t *testing.T) {
	var errs []error
	b := membroker.New(membroker.WithErrorHandler(func(err error) { errs = append(errs, err) }))
	var subscribed []received
	collect(b, &subscribed, "subscriber", "svc", "", broker.Ack, nil)
	if _, err := b.Reply("svc", "", func(_ context.Context, m broker.Inbox) ([]byte, error) {
		if string(m.Data()) == "fail" {
			return nil, errors.New("cannot")
		}
		return append([]byte("re:"), m.Data()...), nil
	}); err != nil {
		t.Fatal(err)
	}
	body, h, err := b.Request(context.Background(), "svc", nil, []byte("hi"))
	if err != nil || string(body) != "re:hi" || h == nil {
		t.Fatalf("Request = %q, %v, %v", body, h, err)
	}
	if len(subscribed) != 0 {
		t.Errorf("a request reached a subscriber: %v", subscribed)
	}

	_, h, err = b.Request(context.Background(), "svc", nil, []byte("fail"))
	var remote *broker.RemoteError
	if !errors.As(err, &remote) || remote.Code != broker.CodeInternal || remote.Description != "cannot" {
		t.Errorf("err = %v, want RemoteError 500", err)
	}
	if h[broker.HeaderServiceError][0] != "cannot" {
		t.Errorf("reply headers = %v", h)
	}
	if len(errs) != 1 {
		t.Errorf("reported errors = %v, want 1", errs)
	}

	// Publishing does not reach repliers.
	_ = b.Publish(context.Background(), "svc", nil, []byte("x"))
	if _, _, err := b.Request(context.Background(), "nobody", nil, nil); !errors.Is(err, membroker.ErrNoResponders) {
		t.Errorf("err = %v, want ErrNoResponders", err)
	}
}

func TestRequestHonoursContext(t *testing.T) {
	b := membroker.New()
	release := make(chan struct{})
	defer close(release)
	if _, err := b.Reply("slow", "", func(context.Context, broker.Inbox) ([]byte, error) {
		<-release
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := b.Request(ctx, "slow", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
}

func TestReplyQueueGroups(t *testing.T) {
	b := membroker.New()
	for _, name := range []string{"a", "b"} {
		if _, err := b.Reply("svc", "q", func(context.Context, broker.Inbox) ([]byte, error) {
			return []byte(name), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	for range 3 {
		body, _, err := b.Request(context.Background(), "svc", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(body))
	}
	if want := []string{"a", "b", "a"}; !slices.Equal(got, want) {
		t.Errorf("answered by %v, want %v", got, want)
	}
}
