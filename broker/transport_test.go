package broker_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/membroker"
)

// requesterFunc is the smallest possible transport for a request client.
type requesterFunc func(context.Context, *broker.Message) (*broker.Message, error)

func (f requesterFunc) Request(ctx context.Context, m *broker.Message) (*broker.Message, error) {
	return f(ctx, m)
}

func TestRequesterOnlyTransport(t *testing.T) {
	c := broker.ForRequester(requesterFunc(func(_ context.Context, m *broker.Message) (*broker.Message, error) {
		return &broker.Message{Data: []byte(`{"n":3}`)}, nil
	}))
	resp, err := broker.Request[greeting, length](context.Background(), c, measure, nil, greeting{})
	if err != nil || resp.N != 3 {
		t.Errorf("Request = %+v, %v", resp, err)
	}
	if err := broker.Publish(context.Background(), c, measure, nil, greeting{}); !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("Publish err = %v, want ErrUnsupported", err)
	}
	_, err = broker.Subscribe(c, measure, func(context.Context, broker.Received[greeting]) error { return nil })
	if !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("Subscribe err = %v, want ErrUnsupported", err)
	}
}

// subscribeOnly hides everything but Subscribe of the broker it embeds.
type subscribeOnly struct{ broker.Subscriber }

func TestRespondWithoutPublisherFailsUpFront(t *testing.T) {
	c := broker.ForSubscriber(subscribeOnly{membroker.New()})
	_, err := broker.Respond(c, measure, func(context.Context, broker.Received[greeting]) (length, error) {
		return length{}, nil
	})
	if !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("Respond err = %v, want ErrUnsupported", err)
	}
}

func TestTransportCapabilitiesAreDetected(t *testing.T) {
	// A full broker handed in as a Requester still serves the embedded
	// Client's other generic functions.
	b := membroker.New()
	c := broker.ForRequester(b)
	got := make(chan struct{}, 1)
	if _, err := broker.Subscribe(c, measure, func(context.Context, broker.Received[greeting]) error {
		got <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), c, measure, nil, greeting{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	default:
		t.Error("message not delivered")
	}
	if c.Broker() != any(b) {
		t.Errorf("Broker() = %v, want the transport passed in", c.Broker())
	}
}

type traceKey struct{}

// tracing is the wrapping broker an application would write: it extracts a
// trace ID into the handler's context and records the context replies are
// published with.
type tracing struct {
	broker.Broker
	timeout  time.Duration
	replyCtx chan context.Context
}

func (b *tracing) Subscribe(cfg broker.SubConfig, h func(context.Context, *broker.Message) error) (broker.Subscription, error) {
	return b.Broker.Subscribe(cfg, func(ctx context.Context, m *broker.Message) error {
		ctx = context.WithValue(ctx, traceKey{}, m.Header.Get("trace"))
		if b.timeout != 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, b.timeout)
			defer cancel()
		}
		return h(ctx, m)
	})
}

func (b *tracing) Publish(ctx context.Context, m *broker.Message) error {
	if b.replyCtx != nil && ctx.Value(traceKey{}) != nil {
		b.replyCtx <- ctx
	}
	return b.Broker.Publish(ctx, m)
}

func TestSubscribeHandlerGetsTransportContext(t *testing.T) {
	c := broker.New(&tracing{Broker: membroker.New()})
	var trace any
	if _, err := broker.Subscribe(c, measure, func(ctx context.Context, _ broker.Received[greeting]) error {
		trace = ctx.Value(traceKey{})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err := broker.Publish(context.Background(), c, measure, nil, greeting{}, broker.WithHeader("trace", "t-1"))
	if err != nil {
		t.Fatal(err)
	}
	if trace != "t-1" {
		t.Errorf("trace in handler context = %v, want t-1", trace)
	}
}

func TestRespondPropagatesContextToHandlerAndReply(t *testing.T) {
	b := &tracing{Broker: membroker.New(), timeout: time.Millisecond, replyCtx: make(chan context.Context, 1)}
	c := broker.New(b)
	var trace any
	if _, err := broker.Respond(c, measure, func(ctx context.Context, _ broker.Received[greeting]) (length, error) {
		trace = ctx.Value(traceKey{})
		<-ctx.Done() // the caller's deadline passes while handling
		return length{N: 1}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := broker.Request[greeting, length](ctx, c, measure, nil, greeting{}, broker.WithHeader("trace", "t-2"))
	if err != nil || resp.N != 1 {
		t.Fatalf("Request = %+v, %v", resp, err)
	}
	if trace != "t-2" {
		t.Errorf("trace in responder context = %v, want t-2", trace)
	}
	replyCtx := <-b.replyCtx
	if got := replyCtx.Value(traceKey{}); got != "t-2" {
		t.Errorf("trace in reply context = %v, want t-2", got)
	}
	if err := replyCtx.Err(); err != nil {
		t.Errorf("reply context err = %v, want a reply that outlives the handler's deadline", err)
	}
}

func TestMembrokerKeepsContextOffTheWire(t *testing.T) {
	b := membroker.New()
	var got any = "unset"
	if _, err := b.Subscribe(broker.SubConfig{Subject: "s"}, func(ctx context.Context, _ *broker.Message) error {
		got = ctx.Value(traceKey{})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), traceKey{}, "sender")
	if err := b.Publish(ctx, &broker.Message{Subject: "s"}); err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("handler saw sender's context value %v; only headers cross the wire", got)
	}
}

// appError is an application's own error type.
type appError struct{ Message string }

func (e *appError) Error() string { return e.Message }

// xErrors carries errors in a single X-Error-Message header.
type xErrors struct{}

func (xErrors) EncodeError(reply *broker.Message, _ string, err error) {
	reply.Header.Set("X-Error-Message", err.Error())
}

func (xErrors) DecodeError(reply *broker.Message) error {
	if msg := reply.Header.Get("X-Error-Message"); msg != "" {
		return &appError{Message: msg}
	}
	return nil
}

func TestErrorCodecReplacesWireConvention(t *testing.T) {
	b := membroker.New()
	var header broker.Header
	if _, err := b.Subscribe(broker.SubConfig{Subject: "_INBOX.*"}, func(_ context.Context, m *broker.Message) error {
		header = m.Header
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c := broker.New(b, broker.WithErrorCodec(xErrors{}))
	if _, err := broker.Respond(c, measure, func(context.Context, broker.Received[greeting]) (length, error) {
		return length{}, errors.New("nope")
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := broker.Request[greeting, length](ctx, c, measure, nil, greeting{})
	var app *appError
	if !errors.As(err, &app) || app.Message != "nope" {
		t.Errorf("err = %#v, want appError nope", err)
	}
	if header.Get(broker.HeaderServiceError) != "" {
		t.Errorf("reply headers %v still carry the default convention", header)
	}
}

func TestDefaultErrorCodecMapsCodes(t *testing.T) {
	c := broker.New(membroker.New())
	if _, err := broker.Respond(c, measure, func(context.Context, broker.Received[greeting]) (length, error) {
		return length{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A payload that does not decode as greeting is rejected with 400.
	_, err := broker.Request[string, length](ctx, c, broker.Channel[string]{Address: measure.Address}, nil, "not an object")
	var remote *broker.RemoteError
	if !errors.As(err, &remote) || remote.Code != "400" {
		t.Errorf("err = %v, want RemoteError 400", err)
	}
}

func TestHandlerErrorIsNotTheRequestersFault(t *testing.T) {
	// A handler can return ErrMalformed about something else, such as an
	// invalid reply from a service it called. The request itself was fine.
	c := broker.New(membroker.New())
	if _, err := broker.Respond(c, measure, func(context.Context, broker.Received[greeting]) (length, error) {
		return length{}, fmt.Errorf("downstream: %w", broker.ErrMalformed)
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := broker.Request[greeting, length](ctx, c, measure, nil, greeting{})
	var remote *broker.RemoteError
	if !errors.As(err, &remote) || remote.Code != "500" {
		t.Errorf("err = %v, want RemoteError 500", err)
	}
}
