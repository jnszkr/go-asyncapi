package broker_test

import (
	"context"
	"testing"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/membroker"
	"github.com/jnszkr/go-asyncapi/broker/schemavalidate"
)

// Run with: go test ./broker -run '^$' -bench . -benchmem

var benchParams = broker.Params{"lang": "en", "name": "ada"}

func BenchmarkChannelSubject(b *testing.B) {
	for b.Loop() {
		if _, err := greetings.Subject(benchParams); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChannelPattern(b *testing.B) {
	for b.Loop() {
		_ = greetings.Pattern()
	}
}

func BenchmarkChannelParseSubject(b *testing.B) {
	for b.Loop() {
		if _, err := greetings.ParseSubject("greet.hu.bob"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPublish measures encoding and handing a message to a transport
// with no subscribers, i.e. the client's own overhead.
func BenchmarkPublish(b *testing.B) {
	c := broker.New(membroker.New())
	ctx := context.Background()
	msg := greeting{Text: "hello"}
	for b.Loop() {
		if err := broker.Publish(ctx, c, greetings, benchParams, msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPublishSubscribe measures a full publish, delivery and decode.
func BenchmarkPublishSubscribe(b *testing.B) {
	c := broker.New(membroker.New())
	benchPublishSubscribe(b, c)
}

// BenchmarkPublishSubscribeValidated adds JSON Schema validation on both
// sides.
func BenchmarkPublishSubscribeValidated(b *testing.B) {
	c := broker.New(membroker.New(), broker.WithValidator(schemavalidate.New()))
	ch := greetings
	ch.Schema = `{"type":"object","required":["text"],"properties":{"text":{"type":"string","minLength":1}}}`
	benchPublishSubscribeOn(b, c, ch)
}

// BenchmarkPublishSubscribeIntercepted adds one pass-through interceptor on
// each side, to show the cost of the chain.
func BenchmarkPublishSubscribeIntercepted(b *testing.B) {
	c := broker.New(membroker.New(),
		broker.WithPublishInterceptors(func(next broker.PublishFunc) broker.PublishFunc {
			return func(ctx context.Context, m *broker.Message) error { return next(ctx, m) }
		}),
		broker.WithReceiveInterceptors(func(next broker.ReceiveFunc) broker.ReceiveFunc {
			return func(ctx context.Context, m *broker.Message) error { return next(ctx, m) }
		}),
	)
	benchPublishSubscribe(b, c)
}

func benchPublishSubscribe(b *testing.B, c *broker.Client) {
	b.Helper()
	benchPublishSubscribeOn(b, c, greetings)
}

func benchPublishSubscribeOn(b *testing.B, c *broker.Client, ch broker.Channel[greeting]) {
	b.Helper()
	n := 0
	sub, err := broker.Subscribe(c, ch, func(context.Context, broker.Received[greeting]) error {
		n++
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sub.Unsubscribe()
	ctx := context.Background()
	msg := greeting{Text: "hello"}
	for b.Loop() {
		if err := broker.Publish(ctx, c, ch, benchParams, msg); err != nil {
			b.Fatal(err)
		}
	}
	if n != b.N {
		b.Fatalf("delivered %d of %d messages", n, b.N)
	}
}

// BenchmarkRequestRespond measures a request and its reply through the
// in-memory broker.
func BenchmarkRequestRespond(b *testing.B) {
	c := broker.New(membroker.New())
	sub, err := broker.Respond(c, measure, func(_ context.Context, m broker.Received[greeting]) (length, error) {
		return length{N: len(m.Payload.Text)}, nil
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sub.Unsubscribe()
	ctx := context.Background()
	req := greeting{Text: "hello"}
	for b.Loop() {
		resp, err := broker.Request[greeting, length](ctx, c, measure, nil, req)
		if err != nil || resp.N != 5 {
			b.Fatalf("Request = %+v, %v", resp, err)
		}
	}
}
