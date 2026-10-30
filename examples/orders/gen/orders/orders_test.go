package orders_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/jsbroker"
	"github.com/jnszkr/go-asyncapi/broker/natsbroker"
	"github.com/jnszkr/go-asyncapi/examples/orders/gen/orders"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func connect(t *testing.T) *nats.Conn {
	t.Helper()
	srv := natstest.RunRandClientPortServer()
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestPublishSubscribe(t *testing.T) {
	nc := connect(t)
	errs := make(chan error, 1)
	c := orders.NewClient(natsbroker.New(nc), broker.WithErrorHandler(func(err error) { errs <- err }))

	got := make(chan broker.Received[orders.Order], 1)
	sub, err := c.OnOrderCreated(func(_ context.Context, m broker.Received[orders.Order]) error {
		got <- m
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if q := sub.(*nats.Subscription).Queue; q != "order-processors" {
		t.Errorf("queue = %q, want order-processors", q)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	order := orders.Order{
		ID:     "o-1",
		Status: orders.OrderStatusPending,
		Items:  []orders.LineItem{{SKU: "sku-1", Quantity: 3}},
	}
	err = c.PublishOrderCreated(context.Background(), orders.OrderCreatedParams("eu"), order,
		broker.WithHeader(orders.OrderCreatedHeaderTraceID, "trace-1"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-got:
		if m.Subject != "orders.eu.created" || m.Params["region"] != "eu" {
			t.Errorf("subject = %q, params = %v", m.Subject, m.Params)
		}
		if m.Payload.ID != "o-1" || len(m.Payload.Items) != 1 || m.Payload.Items[0].Quantity != 3 {
			t.Errorf("order = %+v", m.Payload)
		}
		if m.Header.Get(orders.OrderCreatedHeaderTraceID) != "trace-1" {
			t.Errorf("headers = %v", m.Header)
		}
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestPublishRejectsInvalidParams(t *testing.T) {
	c := orders.NewClient(natsbroker.New(connect(t)))
	for _, region := range []string{"", "apac", "eu.x", "*"} {
		if err := c.PublishOrderCreated(context.Background(), orders.OrderCreatedParams(region), orders.Order{}); err == nil {
			t.Errorf("region %q: expected error", region)
		}
	}
}

func TestRequestReply(t *testing.T) {
	c := orders.NewClient(natsbroker.New(connect(t)))

	sub, err := c.ServeOrderStatus(func(_ context.Context, m broker.Received[orders.StatusRequest]) (orders.StatusReply, error) {
		if m.Payload.OrderID == "missing" {
			return orders.StatusReply{}, errors.New("order not found")
		}
		return orders.StatusReply{OrderID: m.Payload.OrderID, Status: orders.OrderStatusShipped}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := c.GetOrderStatus(ctx, orders.StatusRequest{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OrderID != "o-1" || resp.Status != orders.OrderStatusShipped {
		t.Errorf("reply = %+v", resp)
	}

	_, err = c.GetOrderStatus(ctx, orders.StatusRequest{OrderID: "missing"})
	var remote *broker.RemoteError
	if !errors.As(err, &remote) || remote.Code != "500" || remote.Description != "order not found" {
		t.Errorf("err = %v, want RemoteError 500", err)
	}
}

// TestGenericCallsWork shows the generated channels used directly with the
// generic functions, without the typed wrappers.
func TestGenericCallsWork(t *testing.T) {
	c := orders.NewClient(natsbroker.New(connect(t)))
	got := make(chan string, 1)
	_, err := broker.Subscribe(c.Client, orders.OrderCreatedChannel, func(_ context.Context, m broker.Received[orders.Order]) error {
		got <- m.Payload.ID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Broker().(*natsbroker.Broker).Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), c.Client, orders.OrderCreatedChannel, orders.OrderCreatedParams("us"), orders.Order{ID: "o-2"}); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-got:
		if id != "o-2" {
			t.Errorf("id = %q", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

// TestJetStream runs the same generated client on JetStream. The stream and
// consumer are the application's; the generated endpoint only says which
// subject and queue the operation subscribes with.
func TestJetStream(t *testing.T) {
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
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{orders.OrderCreatedChannel.Pattern()}})
	if err != nil {
		t.Fatal(err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jsbroker.ConsumerConfig(orders.OnOrderCreatedEndpoint))
	if err != nil {
		t.Fatal(err)
	}
	b := jsbroker.New(js)
	b.Bind(orders.OnOrderCreatedEndpoint, cons)
	c := orders.NewClient(b)

	// Published before anyone subscribes: the stream keeps it.
	if err := c.PublishOrderCreated(ctx, orders.OrderCreatedParams("us"), orders.Order{ID: "o-3"}); err != nil {
		t.Fatal(err)
	}

	got := make(chan broker.Received[orders.Order], 1)
	sub, err := c.OnOrderCreated(func(_ context.Context, m broker.Received[orders.Order]) error {
		got <- m
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	select {
	case m := <-got:
		if m.Payload.ID != "o-3" || m.Params["region"] != "us" {
			t.Errorf("received %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stored message")
	}
}
