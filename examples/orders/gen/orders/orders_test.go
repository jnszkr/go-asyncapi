package orders_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/examples/orders/gen/orders"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
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
	c := orders.NewClient(nc, orders.WithErrorHandler(func(err error) { errs <- err }))

	type received struct {
		params  orders.OrderCreatedParams
		order   orders.Order
		headers orders.OrderCreatedHeaders
	}
	got := make(chan received, 1)
	sub, err := c.OnOrderCreated(func(_ context.Context, p orders.OrderCreatedParams, o orders.Order, h orders.OrderCreatedHeaders) error {
		got <- received{p, o, h}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if q := sub.Queue; q != "order-processors" {
		t.Errorf("queue = %q, want order-processors", q)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	attempt := int64(2)
	order := orders.Order{
		ID:     "o-1",
		Status: orders.OrderStatusPending,
		Items:  []orders.LineItem{{SKU: "sku-1", Quantity: 3}},
	}
	err = c.PublishOrderCreated(context.Background(), orders.OrderCreatedParams{Region: "eu"}, order,
		orders.OrderCreatedHeaders{TraceID: "trace-1", Attempt: &attempt})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case r := <-got:
		if r.params.Region != "eu" {
			t.Errorf("region = %q", r.params.Region)
		}
		if r.order.ID != "o-1" || len(r.order.Items) != 1 || r.order.Items[0].Quantity != 3 {
			t.Errorf("order = %+v", r.order)
		}
		if r.headers.TraceID != "trace-1" || r.headers.Attempt == nil || *r.headers.Attempt != 2 {
			t.Errorf("headers = %+v", r.headers)
		}
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestPublishRejectsInvalidParams(t *testing.T) {
	c := orders.NewClient(connect(t))
	for _, region := range []string{"", "apac", "eu.x", "*"} {
		err := c.PublishOrderCreated(context.Background(), orders.OrderCreatedParams{Region: region},
			orders.Order{}, orders.OrderCreatedHeaders{TraceID: "t"})
		if err == nil {
			t.Errorf("region %q: expected error", region)
		}
	}
}

func TestRequestReply(t *testing.T) {
	nc := connect(t)
	c := orders.NewClient(nc)

	sub, err := c.ServeOrderStatus(func(_ context.Context, req orders.StatusRequest) (orders.StatusReply, error) {
		if req.OrderID == "missing" {
			return orders.StatusReply{}, errors.New("order not found")
		}
		return orders.StatusReply{OrderID: req.OrderID, Status: orders.OrderStatusShipped}, nil
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
	var remote *orders.RemoteError
	if !errors.As(err, &remote) || remote.Code != "500" || remote.Description != "order not found" {
		t.Errorf("err = %v, want RemoteError 500", err)
	}
}

func TestParseSubject(t *testing.T) {
	p, err := orders.ParseOrderCreatedSubject("orders.us.created")
	if err != nil || p.Region != "us" {
		t.Errorf("got %+v, %v", p, err)
	}
	for _, s := range []string{"orders.us", "orders.us.deleted", "x.us.created"} {
		if _, err := orders.ParseOrderCreatedSubject(s); err == nil {
			t.Errorf("%q: expected error", s)
		}
	}
}
