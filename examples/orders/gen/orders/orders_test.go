package orders_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/jsbroker"
	"github.com/jnszkr/go-asyncapi/broker/natsbroker"
	"github.com/jnszkr/go-asyncapi/broker/schemavalidate"
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
	b := natsbroker.New(nc)
	c := orders.NewConsumer(b, broker.WithErrorHandler(func(err error) { errs <- err }))
	p := orders.NewProducer(b)

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
	err = p.PublishOrderCreated(context.Background(), orders.OrderCreatedParams("eu"), order,
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
	p := orders.NewProducer(natsbroker.New(connect(t)))
	for _, region := range []string{"", "apac", "eu.x", "*"} {
		if err := p.PublishOrderCreated(context.Background(), orders.OrderCreatedParams(region), orders.Order{}); err == nil {
			t.Errorf("region %q: expected error", region)
		}
	}
}

func TestRequestReply(t *testing.T) {
	b := natsbroker.New(connect(t))
	c := orders.NewClient(b)
	s := orders.NewServer(b)

	sub, err := s.ServeOrderStatus(func(_ context.Context, m broker.Received[orders.StatusRequest]) (orders.StatusReply, error) {
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
	c := orders.NewProducer(natsbroker.New(connect(t)))
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

// TestJetStream runs the same generated code on JetStream. The stream and
// consumer are the application's, configured here in full and passed to the
// receive operation with jsbroker.From.
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
	cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:       orders.OnOrderCreatedEndpoint.Queue,
		FilterSubject: orders.OnOrderCreatedEndpoint.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	b := jsbroker.New(js)
	p := orders.NewProducer(b)
	c := orders.NewConsumer(b)

	// Published before anyone subscribes: the stream keeps it.
	if err := p.PublishOrderCreated(ctx, orders.OrderCreatedParams("us"), orders.Order{ID: "o-3"}); err != nil {
		t.Fatal(err)
	}

	got := make(chan broker.Received[orders.Order], 1)
	sub, err := c.OnOrderCreated(func(_ context.Context, m broker.Received[orders.Order]) error {
		got <- m
		return nil
	}, jsbroker.From(cons, jetstream.PullMaxMessages(10)))
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

// TestValidation runs the schemas of the generated code on every operation.
func TestValidation(t *testing.T) {
	nc := connect(t)
	b := natsbroker.New(nc)
	validating := broker.WithValidator(schemavalidate.New())
	ctx := context.Background()

	// Publishing a value the document forbids fails before it is sent.
	noItems := orders.Order{ID: "o-1", Status: "unknown"}
	err := orders.NewProducer(b, validating).PublishOrderCreated(ctx, orders.OrderCreatedParams("eu"), noItems)
	if !errors.Is(err, broker.ErrInvalidPayload) {
		t.Errorf("publish: err = %v, want ErrInvalidPayload", err)
	}

	// Receiving one reaches the error handler instead of the handler.
	errs := make(chan error, 1)
	c := orders.NewConsumer(b, validating, broker.WithErrorHandler(func(err error) { errs <- err }))
	sub, err := c.OnOrderCreated(func(_ context.Context, m broker.Received[orders.Order]) error {
		t.Errorf("handler received %+v", m.Payload)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := orders.NewProducer(b).PublishOrderCreated(ctx, orders.OrderCreatedParams("eu"), noItems); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errs:
		if !errors.Is(err, broker.ErrMalformed) {
			t.Errorf("receive: err = %v, want ErrMalformed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	// An invalid request is answered with a 400, and an invalid reply is not sent.
	srv, err := orders.NewServer(b, validating).ServeOrderStatus(func(context.Context, broker.Received[orders.StatusRequest]) (orders.StatusReply, error) {
		return orders.StatusReply{OrderID: "o-1", Status: "unknown"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Unsubscribe()
	var remote *broker.RemoteError
	_, err = orders.NewClient(b).GetOrderStatus(ctx, orders.StatusRequest{OrderID: "o-1"})
	if !errors.As(err, &remote) || remote.Code != "500" {
		t.Errorf("invalid reply: err = %v, want a RemoteError with code 500", err)
	}
}

// requesterFunc is all a request client needs from its transport.
type requesterFunc func(context.Context, *broker.Message) (*broker.Message, error)

func (f requesterFunc) Request(ctx context.Context, m *broker.Message) (*broker.Message, error) {
	return f(ctx, m)
}

type requestIDKey struct{}

// serverSide wraps a broker to extract an incoming header into the handler's
// context, and to copy it back onto the reply.
type serverSide struct{ broker.PubSub }

func (b serverSide) Subscribe(cfg broker.SubConfig, h func(context.Context, *broker.Message) error) (broker.Subscription, error) {
	return b.PubSub.Subscribe(cfg, func(ctx context.Context, m *broker.Message) error {
		return h(context.WithValue(ctx, requestIDKey{}, m.Header.Get("X-Request-Id")), m)
	})
}

func (b serverSide) Publish(ctx context.Context, m *broker.Message) error {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		m.Header.Set("X-Request-Id", id)
	}
	return b.PubSub.Publish(ctx, m)
}

// TestApplicationSuppliedTransports runs the generated Client and Server over
// transports written by the application, on a real NATS connection.
func TestApplicationSuppliedTransports(t *testing.T) {
	nb := natsbroker.New(connect(t))

	s := orders.NewServer(serverSide{nb})
	var handlerID any
	sub, err := s.ServeOrderStatus(func(ctx context.Context, m broker.Received[orders.StatusRequest]) (orders.StatusReply, error) {
		handlerID = ctx.Value(requestIDKey{})
		return orders.StatusReply{OrderID: m.Payload.OrderID}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nb.Conn().Flush(); err != nil {
		t.Fatal(err)
	}

	var replyID string
	c := orders.NewClient(requesterFunc(func(ctx context.Context, m *broker.Message) (*broker.Message, error) {
		m.Header.Set("X-Request-Id", "r-1")
		rm, err := nb.Request(ctx, m)
		if err == nil {
			replyID = rm.Header.Get("X-Request-Id")
		}
		return rm, err
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.GetOrderStatus(ctx, orders.StatusRequest{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OrderID != "o-1" {
		t.Errorf("reply = %+v", resp)
	}
	if handlerID != "r-1" {
		t.Errorf("request ID in handler context = %v, want r-1", handlerID)
	}
	if replyID != "r-1" {
		t.Errorf("request ID on reply = %q, want r-1", replyID)
	}
}
