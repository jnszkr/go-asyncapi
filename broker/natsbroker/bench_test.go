package natsbroker_test

import (
	"context"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
)

// Run with: go test ./broker/natsbroker -run '^$' -bench . -benchmem
//
// These go through an embedded NATS server over loopback, so they measure the
// transport as well as the client.

// BenchmarkPublish measures publishing with no subscriber; nats.go buffers
// the writes, so this is the client-side cost.
func BenchmarkPublish(b *testing.B) {
	c := broker.New(setup(b))
	ctx := context.Background()
	msg := greeting{Text: "hello"}
	for b.Loop() {
		if err := broker.Publish(ctx, c, greetings, nil, msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPublishSubscribe measures throughput from publish to handler: it
// publishes b.N messages and waits until all of them have been received.
func BenchmarkPublishSubscribe(b *testing.B) {
	c := broker.New(setup(b))
	got := make(chan struct{}, 1024)
	sub, err := broker.Subscribe(c, greetings, func(context.Context, broker.Received[greeting]) error {
		got <- struct{}{}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	defer sub.Unsubscribe()
	ctx := context.Background()
	msg := greeting{Text: "hello"}
	received := 0
	for b.Loop() {
		if err := broker.Publish(ctx, c, greetings, nil, msg); err != nil {
			b.Fatal(err)
		}
		// Drain without blocking so the publisher is not throttled to one
		// message in flight.
		for drained := false; !drained; {
			select {
			case <-got:
				received++
			default:
				drained = true
			}
		}
	}
	timeout := time.After(10 * time.Second)
	for received < b.N {
		select {
		case <-got:
			received++
		case <-timeout:
			b.Fatalf("received %d of %d messages", received, b.N)
		}
	}
}

// BenchmarkRequestRespond measures request latency: one request in flight at
// a time.
func BenchmarkRequestRespond(b *testing.B) {
	c := broker.New(setup(b))
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
