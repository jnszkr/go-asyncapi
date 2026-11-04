package broker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/membroker"
)

func publishTag(name string) broker.PublishInterceptor {
	return func(next broker.PublishFunc) broker.PublishFunc {
		return func(ctx context.Context, m *broker.Message) error {
			m.Header["Tag"] = append(m.Header["Tag"], name)
			return next(ctx, m)
		}
	}
}

func TestPublishInterceptorsWrapInOrder(t *testing.T) {
	b := membroker.New()
	got := make(chan broker.Header, 1)
	if _, err := broker.Subscribe(broker.New(b), measure, func(_ context.Context, m broker.Received[greeting]) error {
		got <- m.Header
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c := broker.ForPublisher(b, broker.WithPublishInterceptors(publishTag("a"), publishTag("b")))
	if err := broker.Publish(context.Background(), c, measure, nil, greeting{}); err != nil {
		t.Fatal(err)
	}
	if tags := (<-got)["Tag"]; len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
		t.Errorf("tags = %v, want [a b]", tags)
	}
}

// TestRespondPublishesRepliesThroughInterceptors checks both sides of a
// request: the requester's interceptor sees request and reply, the
// responder's sees the request and the outgoing reply.
func TestRespondPublishesRepliesThroughInterceptors(t *testing.T) {
	b := membroker.New()
	var received []string
	server := broker.New(b,
		broker.WithPublishInterceptors(publishTag("reply")),
		broker.WithReceiveInterceptors(func(next broker.ReceiveFunc) broker.ReceiveFunc {
			return func(ctx context.Context, m *broker.Message) error {
				received = append(received, m.Header.Get("Auth"))
				return next(ctx, m)
			}
		}))
	if _, err := broker.Respond(server, measure, func(_ context.Context, m broker.Received[greeting]) (length, error) {
		return length{N: len(m.Payload.Text)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	var replyTag string
	client := broker.ForRequester(b, broker.WithRequestInterceptors(func(next broker.RequestFunc) broker.RequestFunc {
		return func(ctx context.Context, m *broker.Message) (*broker.Message, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("interceptor's context has no deadline")
			}
			m.Header.Set("Auth", "secret")
			r, err := next(ctx, m)
			if err == nil {
				replyTag = r.Header.Get("Tag")
			}
			return r, err
		}
	}))
	resp, err := broker.Request[greeting, length](context.Background(), client, measure, nil, greeting{Text: "hello"})
	if err != nil || resp.N != 5 {
		t.Fatalf("Request = %+v, %v", resp, err)
	}
	if len(received) != 1 || received[0] != "secret" {
		t.Errorf("responder saw %v", received)
	}
	if replyTag != "reply" {
		t.Errorf("reply tag = %q", replyTag)
	}
}

func TestRequestInterceptorRetries(t *testing.T) {
	calls := 0
	transport := broker.RequestFunc(func(context.Context, *broker.Message) (*broker.Message, error) {
		if calls++; calls == 1 {
			return nil, errors.New("unavailable")
		}
		return &broker.Message{Data: []byte(`{"n":3}`)}, nil
	})
	c := broker.ForRequester(transport, broker.WithRequestInterceptors(func(next broker.RequestFunc) broker.RequestFunc {
		return func(ctx context.Context, m *broker.Message) (*broker.Message, error) {
			r, err := next(ctx, m)
			if err != nil {
				return next(ctx, m)
			}
			return r, nil
		}
	}))
	resp, err := broker.Request[greeting, length](context.Background(), c, measure, nil, greeting{})
	if err != nil || resp.N != 3 || calls != 2 {
		t.Errorf("Request = %+v, %v after %d calls", resp, err, calls)
	}
}

// TestReceiveInterceptorSeesFailures checks that the interceptor gets handler
// and decoding errors, and passes its context on to the handler.
func TestReceiveInterceptorSeesFailures(t *testing.T) {
	type key struct{}
	var errs []error
	b := membroker.New()
	c := broker.New(b, broker.WithReceiveInterceptors(func(next broker.ReceiveFunc) broker.ReceiveFunc {
		return func(ctx context.Context, m *broker.Message) error {
			err := next(context.WithValue(ctx, key{}, "traced"), m)
			errs = append(errs, err)
			return err
		}
	}))
	failure := errors.New("fails")
	if _, err := broker.Subscribe(c, measure, func(ctx context.Context, _ broker.Received[greeting]) error {
		if ctx.Value(key{}) != "traced" {
			t.Error("handler did not get the interceptor's context")
		}
		return failure
	}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := broker.Publish(ctx, c, measure, nil, greeting{}); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, &broker.Message{Subject: "measure", Header: broker.Header{}, Data: []byte("{")}); err != nil {
		t.Fatal(err)
	}
	if len(errs) != 2 || !errors.Is(errs[0], failure) || !errors.Is(errs[1], broker.ErrMalformed) {
		t.Errorf("errs = %v", errs)
	}
}

func TestInterceptorsDoNotAddCapabilities(t *testing.T) {
	c := broker.ForRequester(broker.RequestFunc(func(context.Context, *broker.Message) (*broker.Message, error) {
		return nil, errors.New("unused")
	}), broker.WithPublishInterceptors(publishTag("a")))
	if err := broker.Publish(context.Background(), c, measure, nil, greeting{}); !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("Publish err = %v, want ErrUnsupported", err)
	}
}

// TestInterceptorsSetHeadersOnMessagesWithout checks that messages arriving
// without headers can be given some, and that the handler sees them.
func TestInterceptorsSetHeadersOnMessagesWithout(t *testing.T) {
	b := membroker.New()
	c := broker.New(b, broker.WithReceiveInterceptors(func(next broker.ReceiveFunc) broker.ReceiveFunc {
		return func(ctx context.Context, m *broker.Message) error {
			m.Header.Set("Tag", "received")
			return next(ctx, m)
		}
	}))
	got := make(chan string, 1)
	if _, err := broker.Subscribe(c, measure, func(_ context.Context, m broker.Received[greeting]) error {
		got <- m.Header.Get("Tag")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), &broker.Message{Subject: "measure", Data: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if tag := <-got; tag != "received" {
		t.Errorf("handler saw tag %q", tag)
	}

	transport := broker.RequestFunc(func(context.Context, *broker.Message) (*broker.Message, error) {
		return &broker.Message{Data: []byte(`{"n":3}`)}, nil
	})
	rc := broker.ForRequester(transport, broker.WithRequestInterceptors(func(next broker.RequestFunc) broker.RequestFunc {
		return func(ctx context.Context, m *broker.Message) (*broker.Message, error) {
			r, err := next(ctx, m)
			if err == nil {
				r.Header.Set("Tag", "reply")
			}
			return r, err
		}
	}))
	if _, err := broker.Request[greeting, length](context.Background(), rc, measure, nil, greeting{}); err != nil {
		t.Errorf("Request: %v", err)
	}
}

// TestPublishInterceptorOverridesHeaders checks that an interceptor runs after
// the headers of the message are set, whatever set them.
func TestPublishInterceptorOverridesHeaders(t *testing.T) {
	b := membroker.New()
	got := make(chan broker.Header, 1)
	if _, err := broker.Subscribe(broker.New(b), measure, func(_ context.Context, m broker.Received[greeting]) error {
		got <- m.Header
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c := broker.ForPublisher(b, broker.WithPublishInterceptors(func(next broker.PublishFunc) broker.PublishFunc {
		return func(ctx context.Context, m *broker.Message) error {
			m.Header.Set(broker.HeaderContentType, "application/vnd.test+json")
			m.Header.Set("Trace", "interceptor")
			return next(ctx, m)
		}
	}))
	if err := broker.Publish(context.Background(), c, measure, nil, greeting{}, broker.WithHeader("Trace", "caller")); err != nil {
		t.Fatal(err)
	}
	h := <-got
	if ct := h.Get(broker.HeaderContentType); ct != "application/vnd.test+json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if tr := h.Get("Trace"); tr != "interceptor" {
		t.Errorf("Trace = %q", tr)
	}
}
