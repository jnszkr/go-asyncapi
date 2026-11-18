package orders_test

import (
	"context"
	"testing"

	"github.com/jnszkr/go-asyncapi/broker/membroker"
	"github.com/jnszkr/go-asyncapi/examples/orders/gen/orders"
)

// Run with: go test ./examples/orders/gen/orders -run '^$' -bench . -benchmem

var benchOrder = orders.Order{ID: "o-1", Status: orders.OrderStatusPaid, Items: []orders.LineItem{{SKU: "SKU-1", Quantity: 1}}}

// BenchmarkPublishSubscribe measures the generated code on both sides of a
// message, in memory: encode, subject, decode and parse the parameters.
func BenchmarkPublishSubscribe(b *testing.B) {
	t := membroker.New()
	if _, err := orders.NewConsumer(t).OnOrderCreated(func(context.Context, orders.Received[orders.Order]) error { return nil }); err != nil {
		b.Fatal(err)
	}
	p := orders.NewProducer(t)
	ctx, params := context.Background(), orders.OrderCreatedParams("eu")
	for b.Loop() {
		if err := p.PublishOrderCreated(ctx, params, nil, benchOrder); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRequestReply measures a request and its reply, in memory.
func BenchmarkRequestReply(b *testing.B) {
	t := membroker.New()
	if _, err := orders.NewServer(t).ServeOrderStatus(func(_ context.Context, m orders.Received[orders.StatusRequest]) (orders.StatusReply, error) {
		return orders.StatusReply{OrderID: m.Payload.OrderID, Status: orders.OrderStatusPaid}, nil
	}); err != nil {
		b.Fatal(err)
	}
	c := orders.NewClient(t)
	ctx, req := context.Background(), orders.StatusRequest{OrderID: "o-1"}
	for b.Loop() {
		if _, err := c.GetOrderStatus(ctx, nil, req); err != nil {
			b.Fatal(err)
		}
	}
}
