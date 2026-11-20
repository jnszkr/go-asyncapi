package orders_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/jsbroker"
	"github.com/jnszkr/go-asyncapi/broker/membroker"
	"github.com/jnszkr/go-asyncapi/broker/natsbroker"
	"github.com/jnszkr/go-asyncapi/examples/orders/gen/orders"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The transports of package broker satisfy the interfaces the generated code
// declares, without either package importing the other.
var (
	_ orders.Publisher  = (*natsbroker.Broker)(nil)
	_ orders.Subscriber = (*natsbroker.Broker)(nil)
	_ orders.Requester  = (*natsbroker.Broker)(nil)
	_ orders.Replier    = (*natsbroker.Broker)(nil)
	_ orders.Publisher  = (*jsbroker.Broker)(nil)
	_ orders.Subscriber = (*jsbroker.Broker)(nil)
	_ orders.Requester  = (*jsbroker.Broker)(nil)
	_ orders.Replier    = (*membroker.Broker)(nil)
	_ broker.Subscriber = orders.Subscriber(nil)
	_ broker.Replier    = orders.Replier(nil)
)

func TestSettlementsMatchTheContract(t *testing.T) {
	if orders.Ack != broker.Ack || orders.Nak != broker.Nak || orders.Term != broker.Term {
		t.Errorf("settlements = %d, %d, %d; want those of package broker", orders.Ack, orders.Nak, orders.Term)
	}
}

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
	b := natsbroker.New(nc, natsbroker.WithErrorHandler(func(err error) { errs <- err }))

	got := make(chan orders.Received[orders.Order], 1)
	sub, err := orders.NewConsumer(b).OnOrderCreated(func(_ context.Context, m orders.Received[orders.Order]) error {
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
		Items:  []orders.LineItem{{SKU: "SKU-1", Quantity: 3}},
	}
	headers := map[string][]string{orders.OrderCreatedHeaderTraceID: {"trace-1"}}
	if err := orders.NewProducer(b).PublishOrderCreated(context.Background(), orders.OrderCreatedParams("eu"), headers, order); err != nil {
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
		if v := m.Headers[orders.OrderCreatedHeaderTraceID]; len(v) != 1 || v[0] != "trace-1" {
			t.Errorf("headers = %v", m.Headers)
		}
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestPublishRejectsInvalidParams(t *testing.T) {
	p := orders.NewProducer(membroker.New())
	for _, region := range []string{"", "apac", "eu.x", "*"} {
		if err := p.PublishOrderCreated(context.Background(), orders.OrderCreatedParams(region), nil, orders.Order{}); err == nil {
			t.Errorf("region %q: expected error", region)
		}
	}
}

// recorder is a Subscriber written without any of this module's packages,
// as an application could, recording how each message was settled.
type recorder struct {
	h       func(context.Context, orders.Inbox) (orders.Settlement, error)
	settled []orders.Settlement
	errs    []error
}

func (r *recorder) Subscribe(_, _ string, h func(context.Context, orders.Inbox) (orders.Settlement, error)) (orders.Subscription, error) {
	r.h = h
	return nil, nil
}

func (r *recorder) deliver(subject, data string) {
	s, err := r.h(context.Background(), inbox{subject, data})
	r.settled = append(r.settled, s)
	r.errs = append(r.errs, err)
}

type inbox struct{ subject, data string }

func (i inbox) Subject() string              { return i.subject }
func (i inbox) Headers() map[string][]string { return map[string][]string{} }
func (i inbox) Data() []byte                 { return []byte(i.data) }

func TestConsumerSettles(t *testing.T) {
	r := &recorder{}
	retry := errors.New("try later")
	if _, err := orders.NewConsumer(r).OnOrderCreated(func(_ context.Context, m orders.Received[orders.Order]) error {
		switch m.Payload.ID {
		case "retry":
			return retry
		case "bad":
			return orders.Terminate(errors.New("bad order"))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	order := func(id string) string {
		return `{"id":"` + id + `","status":"paid","items":[{"sku":"SKU-1","quantity":1}]}`
	}
	r.deliver("orders.eu.created", order("ok"))
	r.deliver("orders.eu.created", order("retry"))
	r.deliver("orders.eu.created", order("bad"))
	r.deliver("orders.eu.created", `{`)
	r.deliver("orders.eu.x.created", order("ok"))
	r.deliver("orders.eu.deleted", order("ok"))
	r.deliver("orders.eu.created", `{"id":"ok"}`)

	want := []orders.Settlement{orders.Ack, orders.Nak, orders.Term, orders.Term, orders.Term, orders.Term, orders.Term}
	if !slices.Equal(r.settled, want) {
		t.Errorf("settled = %v, want %v", r.settled, want)
	}
	if r.errs[0] != nil || !errors.Is(r.errs[1], retry) {
		t.Errorf("errors = %v", r.errs)
	}
	for i, err := range r.errs[2:] {
		if !broker.IsMalformed(err) {
			t.Errorf("delivery %d: err = %v, want a malformed one", i+2, err)
		}
	}
	if broker.IsMalformed(r.errs[1]) {
		t.Errorf("handler error %v is malformed", r.errs[1])
	}
	if msg := r.errs[2].Error(); msg != "malformed message: bad order" {
		t.Errorf("Terminate error = %q", msg)
	}
	if !strings.Contains(r.errs[3].Error(), "orders.eu.created: decode") {
		t.Errorf("decode error = %q", r.errs[3])
	}
	if !strings.Contains(r.errs[6].Error(), `orders.eu.created: invalid payload: /: missing required property "items"`) {
		t.Errorf("validation error = %q", r.errs[6])
	}
}

func TestRequestReply(t *testing.T) {
	b := natsbroker.New(connect(t))
	sub, err := orders.NewServer(b).ServeOrderStatus(func(_ context.Context, m orders.Received[orders.StatusRequest]) (orders.StatusReply, error) {
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
	c := orders.NewClient(b)
	resp, err := c.GetOrderStatus(ctx, nil, orders.StatusRequest{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OrderID != "o-1" || resp.Status != orders.OrderStatusShipped {
		t.Errorf("reply = %+v", resp)
	}

	_, err = c.GetOrderStatus(ctx, nil, orders.StatusRequest{OrderID: "missing"})
	var remote *broker.RemoteError
	if !errors.As(err, &remote) || remote.Code != "500" || remote.Description != "order not found" {
		t.Errorf("err = %v, want RemoteError 500", err)
	}

	// A request the server cannot decode is the requester's fault.
	_, rh, err := b.Request(ctx, "orders.status.get", nil, []byte("{"))
	if !errors.As(err, &remote) || remote.Code != "400" {
		t.Errorf("malformed request: err = %v, headers = %v, want RemoteError 400", err, rh)
	}
}

func TestClientReportsUndecodableReply(t *testing.T) {
	b := membroker.New()
	if _, err := b.Reply("orders.status.get", "", func(context.Context, broker.Inbox) ([]byte, error) {
		return []byte("not json"), nil
	}); err != nil {
		t.Fatal(err)
	}
	_, err := orders.NewClient(b).GetOrderStatus(context.Background(), nil, orders.StatusRequest{OrderID: "o-1"})
	if err == nil || !strings.Contains(err.Error(), "unmarshal StatusReply") {
		t.Errorf("err = %v, want an unmarshal error", err)
	}

	_, err = orders.NewClient(membroker.New()).GetOrderStatus(context.Background(), nil, orders.StatusRequest{OrderID: "o-1"})
	if !errors.Is(err, membroker.ErrNoResponders) || !strings.Contains(err.Error(), "request orders.status.get") {
		t.Errorf("err = %v, want no responders", err)
	}
}

func TestServerAnswersMalformedRequests(t *testing.T) {
	b := membroker.New()
	called := false
	if _, err := orders.NewServer(b).ServeOrderStatus(func(context.Context, orders.Received[orders.StatusRequest]) (orders.StatusReply, error) {
		called = true
		return orders.StatusReply{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	_, _, err := b.Request(context.Background(), "orders.status.get", nil, []byte("{"))
	var remote *broker.RemoteError
	if !errors.As(err, &remote) || remote.Code != "400" {
		t.Errorf("err = %v, want RemoteError 400", err)
	}
	if called {
		t.Error("handler called for a malformed request")
	}
}

type requestIDKey struct{}

// serverSide wraps a Replier to put an incoming header into the handler's
// context.
type serverSide struct{ orders.Replier }

func (s serverSide) Reply(subject, queue string, h func(context.Context, orders.Inbox) ([]byte, error)) (orders.Subscription, error) {
	return s.Replier.Reply(subject, queue, func(ctx context.Context, msg orders.Inbox) ([]byte, error) {
		var id string
		if v := msg.Headers()["X-Request-Id"]; len(v) > 0 {
			id = v[0]
		}
		return h(context.WithValue(ctx, requestIDKey{}, id), msg)
	})
}

// requesterFunc is a Requester written by the application.
type requesterFunc func(ctx context.Context, subject string, headers map[string][]string, body []byte) ([]byte, map[string][]string, error)

func (f requesterFunc) Request(ctx context.Context, subject string, headers map[string][]string, body []byte) ([]byte, map[string][]string, error) {
	return f(ctx, subject, headers, body)
}

// TestApplicationSuppliedTransports runs the generated Client and Server over
// transports the application wrapped, on a real NATS connection.
func TestApplicationSuppliedTransports(t *testing.T) {
	nb := natsbroker.New(connect(t))
	var handlerID any
	sub, err := orders.NewServer(serverSide{nb}).ServeOrderStatus(func(ctx context.Context, m orders.Received[orders.StatusRequest]) (orders.StatusReply, error) {
		handlerID = ctx.Value(requestIDKey{})
		return orders.StatusReply{OrderID: m.Payload.OrderID, Status: orders.OrderStatusPending}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nb.Conn().Flush(); err != nil {
		t.Fatal(err)
	}

	c := orders.NewClient(requesterFunc(func(ctx context.Context, subject string, _ map[string][]string, body []byte) ([]byte, map[string][]string, error) {
		return nb.Request(ctx, subject, map[string][]string{"X-Request-Id": {"r-1"}}, body)
	}))
	resp, err := c.GetOrderStatus(context.Background(), nil, orders.StatusRequest{OrderID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OrderID != "o-1" {
		t.Errorf("reply = %+v", resp)
	}
	if handlerID != "r-1" {
		t.Errorf("request ID in handler context = %v, want r-1", handlerID)
	}
}

// TestJetStream runs the same generated code on JetStream. The stream and
// consumer are the application's, configured here in full.
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
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{orders.OnOrderCreatedEndpoint.Subject}}); err != nil {
		t.Fatal(err)
	}
	consumers := jsbroker.ConsumerFunc(func(filter, queue string) (jetstream.Consumer, []jetstream.PullConsumeOpt, error) {
		cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
			Durable:       queue,
			FilterSubject: filter,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
		})
		return cons, []jetstream.PullConsumeOpt{jetstream.PullMaxMessages(10)}, err
	})
	b := jsbroker.New(js, jsbroker.WithConsumers(consumers))

	// Published before anyone subscribes: the stream keeps it.
	if err := orders.NewProducer(b).PublishOrderCreated(ctx, orders.OrderCreatedParams("us"), nil, validOrder("o-3")); err != nil {
		t.Fatal(err)
	}

	got := make(chan orders.Received[orders.Order], 1)
	sub, err := orders.NewConsumer(b).OnOrderCreated(func(_ context.Context, m orders.Received[orders.Order]) error {
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
	if _, err := js.Consumer(ctx, "ORDERS", "order-processors"); err != nil {
		t.Errorf("durable named after the queue: %v", err)
	}
}

// TestChannelSubjects exercises the generated Channel on an address with
// several parameters, an allowed list and a default.
func TestChannelSubjects(t *testing.T) {
	ch := orders.Channel[string]{
		Address:  "shop.{region}.{kind}.done",
		Allowed:  map[string][]string{"region": {"eu", "us"}},
		Defaults: map[string]string{"kind": "order"},
	}
	for params, want := range map[string]string{
		"eu,":       "shop.eu.order.done",
		"us,refund": "shop.us.refund.done",
	} {
		region, kind, _ := strings.Cut(params, ",")
		got, err := ch.Subject(orders.Params{"region": region, "kind": kind})
		if err != nil || got != want {
			t.Errorf("Subject(%s) = %q, %v; want %q", params, got, err, want)
		}
		p, err := ch.ParseSubject(want)
		if err != nil || p["region"] != region || (kind != "" && p["kind"] != kind) || len(p) != 2 {
			t.Errorf("ParseSubject(%q) = %v, %v", want, p, err)
		}
	}
	for _, p := range []orders.Params{
		{"kind": "x"},                   // missing, no default
		{"region": "apac"},              // not allowed
		{"region": "eu", "kind": "a.b"}, // not a token
		{"region": "eu", "kind": "a b"}, // not a token
		{"region": "eu", "kind": ">"},   // wildcard
	} {
		if s, err := ch.Subject(p); err == nil {
			t.Errorf("Subject(%v) = %q, want an error", p, s)
		}
	}
	for _, s := range []string{"shop.eu.order", "shop.eu.order.done.x", "shop.eu.order.open", "store.eu.order.done"} {
		if p, err := ch.ParseSubject(s); err == nil {
			t.Errorf("ParseSubject(%q) = %v, want an error", s, p)
		}
	}
	plain := orders.Channel[string]{Address: "a.b"}
	if s, err := plain.Subject(nil); err != nil || s != "a.b" {
		t.Errorf("Subject without parameters = %q, %v", s, err)
	}
	// Braces only make a parameter when they span the token.
	odd := orders.Channel[string]{Address: "a.{}.{x"}
	if s, err := odd.Subject(nil); err != nil || s != "a.{}.{x" {
		t.Errorf("Subject of literal braces = %q, %v", s, err)
	}
}

func TestMalformedError(t *testing.T) {
	inner := errors.New("inner")
	err := orders.Terminate(inner)
	if !errors.Is(err, inner) || err.Error() != "malformed message: inner" || !broker.IsMalformed(err) {
		t.Errorf("Terminate(inner) = %v", err)
	}
}

func validOrder(id string) orders.Order {
	return orders.Order{ID: id, Status: orders.OrderStatusPaid, Items: []orders.LineItem{{SKU: "ABC-1", Quantity: 1}}}
}

// TestProducerValidates checks that an order that does not match its schema
// is not published, and that the error says why.
func TestProducerValidates(t *testing.T) {
	b := membroker.New()
	published := 0
	if _, err := b.Subscribe("orders.*.created", "", func(context.Context, broker.Inbox) (broker.Settlement, error) {
		published++
		return broker.Ack, nil
	}); err != nil {
		t.Fatal(err)
	}
	p := orders.NewProducer(b)
	ctx, params := context.Background(), orders.OrderCreatedParams("eu")
	shippedAt := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	email := "a@example.com"
	total := 9.5

	valid := []func(*orders.Order){
		func(*orders.Order) {},
		func(o *orders.Order) { o.Status, o.ShippedAt = orders.OrderStatusShipped, &shippedAt },
		func(o *orders.Order) { o.Status = orders.OrderStatusShipped },
		func(o *orders.Order) { o.CustomerEmail, o.Total = &email, &total },
		func(o *orders.Order) { o.Items[0].Quantity = 1000 },
		func(o *orders.Order) { o.Metadata = map[string]string{"k": "v"} },
		func(o *orders.Order) {
			o.Category = &orders.Category{Name: "a", Children: []orders.Category{{Name: "b", Children: []orders.Category{{Name: "c"}}}}}
		},
	}
	for i, change := range valid {
		o := validOrder("o-1")
		change(&o)
		if err := p.PublishOrderCreated(ctx, params, nil, o); err != nil {
			t.Errorf("valid order %d: %v", i, err)
		}
	}
	if published != len(valid) {
		t.Fatalf("published %d orders, want %d", published, len(valid))
	}

	bad := "not-an-email"
	negative := -1.0
	invalid := map[string]struct {
		change func(*orders.Order)
		want   string
	}{
		"empty id":         {func(o *orders.Order) { o.ID = "" }, "/id: want at least 1 characters, got 0"},
		"no items":         {func(o *orders.Order) { o.Items = nil }, `/: missing required property "items"`},
		"empty items":      {func(o *orders.Order) { o.Items = []orders.LineItem{} }, "/items: want at least 1 items, got 0"},
		"unknown status":   {func(o *orders.Order) { o.Status = "lost" }, "/status: must be one of [pending paid shipped cancelled]"},
		"sku pattern":      {func(o *orders.Order) { o.Items[0].SKU = "abc-1" }, "/items/0/sku: does not match ^[A-Z]{3}-[0-9]+$"},
		"quantity minimum": {func(o *orders.Order) { o.Items[0].Quantity = 0 }, "/items/0/quantity: must be >= 1"},
		"quantity maximum": {func(o *orders.Order) { o.Items[0].Quantity = 1001 }, "/items/0/quantity: must be <= 1000"},
		"negative total":   {func(o *orders.Order) { o.Total = &negative }, "/total: must be >= 0"},
		"email format":     {func(o *orders.Order) { o.CustomerEmail = &bad }, "/customerEmail: not a valid email"},
		"recursive schema": {func(o *orders.Order) {
			o.Category = &orders.Category{Name: "a", Children: []orders.Category{{Name: "b", Children: []orders.Category{{Name: ""}}}}}
		}, "/category/children/0/children/0/name: want at least 1 characters, got 0"},
	}
	for name, tc := range invalid {
		t.Run(name, func(t *testing.T) {
			o := validOrder("o-1")
			tc.change(&o)
			err := p.PublishOrderCreated(ctx, params, nil, o)
			var verr *orders.ValidationError
			if !errors.As(err, &verr) || !slices.Contains(verr.Problems, tc.want) {
				t.Fatalf("err = %v, want a ValidationError with %q", err, tc.want)
			}
			if broker.IsMalformed(err) {
				t.Errorf("outgoing validation error %v is malformed", err)
			}
		})
	}
	if published != len(valid) {
		t.Errorf("published %d orders, want only the %d valid ones", published, len(valid))
	}

	// Several problems are reported together, in a stable order.
	o := validOrder("")
	o.Status = "lost"
	o.Items[0].SKU = "x"
	err := p.PublishOrderCreated(ctx, params, nil, o)
	const want = "Order: invalid payload: /id: want at least 1 characters, got 0; /items/0/sku: does not match ^[A-Z]{3}-[0-9]+$; /status: must be one of [pending paid shipped cancelled]"
	if err == nil || err.Error() != want {
		t.Errorf("err = %v\nwant %s", err, want)
	}
}

// TestConsumerTerminatesInvalidMessages checks that a received message whose
// payload does not validate, or does not decode, is terminated without
// calling the handler.
func TestConsumerTerminatesInvalidMessages(t *testing.T) {
	r := &recorder{}
	called := 0
	if _, err := orders.NewConsumer(r).OnOrderCreated(func(context.Context, orders.Received[orders.Order]) error {
		called++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		`{"id":"o-1","status":"paid","items":[{"sku":"ABC-1","quantity":1}],"category":{"name":"a","children":[{"name":"b"}]}}`,
		`{"id":"o-1","status":"paid","items":[{"sku":"ABC-1","quantity":1}],"category":{"name":"a","children":[{"name":""}]}}`,
		`{"id":"o-1","status":"paid","items":[{"sku":"ABC-1","quantity":0}]}`,
		`{"id":"o-1","status":"lost","items":[{"sku":"ABC-1","quantity":1}]}`,
		`{"id":"o-1","status":"paid"}`,
	} {
		r.deliver("orders.eu.created", body)
	}
	for _, body := range []string{
		`{"id":"o-1","status":"paid","items":[{"sku":"ABC-1","quantity":1.5}]}`,
		`{"id":"o-1","status":"paid","items":[{"sku":"ABC-1","quantity":1}],"metadata":{"k":1}}`,
		`{"id":"o-1","status":"shipped","items":[{"sku":"ABC-1","quantity":1}],"shippedAt":"yesterday"}`,
	} {
		r.deliver("orders.eu.created", body)
	}
	want := []orders.Settlement{orders.Ack, orders.Term, orders.Term, orders.Term, orders.Term, orders.Term, orders.Term, orders.Term}
	if !slices.Equal(r.settled, want) || called != 1 {
		t.Fatalf("settled = %v, handler called %d times; want %v, once", r.settled, called, want)
	}
	for i, problem := range []string{
		"/category/children/0/name: want at least 1 characters, got 0",
		"/items/0/quantity: must be >= 1",
		"/status: must be one of [pending paid shipped cancelled]",
		`/: missing required property "items"`,
	} {
		err := r.errs[i+1]
		var verr *orders.ValidationError
		if !broker.IsMalformed(err) || !errors.As(err, &verr) || !slices.Contains(verr.Problems, problem) {
			t.Errorf("delivery %d: err = %v, want a malformed ValidationError with %q", i+1, err, problem)
		}
	}
	for i := 5; i < 8; i++ {
		err := r.errs[i]
		var verr *orders.ValidationError
		if !broker.IsMalformed(err) || errors.As(err, &verr) || !strings.Contains(err.Error(), "orders.eu.created: decode: ") {
			t.Errorf("delivery %d: err = %v, want a malformed decode error", i, err)
		}
	}
}

// TestValidate checks the Validate methods directly.
func TestValidate(t *testing.T) {
	if err := validOrder("o-1").Validate(); err != nil {
		t.Errorf("valid order: %v", err)
	}
	if err := orders.OrderStatusShipped.Validate(); err != nil {
		t.Errorf("valid status: %v", err)
	}
	err := orders.OrderStatus("lost").Validate()
	var verr *orders.ValidationError
	if !errors.As(err, &verr) || !slices.Equal(verr.Problems, []string{"/: must be one of [pending paid shipped cancelled]"}) {
		t.Errorf("invalid status: err = %v", err)
	}
	err = orders.Order{}.Validate()
	want := []string{
		`/: missing required property "items"`,
		"/id: want at least 1 characters, got 0",
		"/status: must be one of [pending paid shipped cancelled]",
	}
	if !errors.As(err, &verr) || !slices.Equal(verr.Problems, want) {
		t.Errorf("empty order: err = %v, want %q", err, want)
	}
}

// TestRequestReplyValidates checks validation on both sides of a request: the
// client does not send an invalid request, the server answers one with 400
// without calling the handler, and an invalid reply becomes a 500.
func TestRequestReplyValidates(t *testing.T) {
	b := natsbroker.New(connect(t))
	called := 0
	sub, err := orders.NewServer(b).ServeOrderStatus(func(_ context.Context, m orders.Received[orders.StatusRequest]) (orders.StatusReply, error) {
		called++
		if m.Payload.OrderID == "broken" {
			return orders.StatusReply{OrderID: "broken", Status: "lost"}, nil
		}
		return orders.StatusReply{OrderID: m.Payload.OrderID, Status: orders.OrderStatusPaid}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c := orders.NewClient(b)

	_, err = c.GetOrderStatus(ctx, nil, orders.StatusRequest{})
	var verr *orders.ValidationError
	if !errors.As(err, &verr) || err.Error() != "StatusRequest: invalid payload: /orderId: want at least 1 characters, got 0" {
		t.Errorf("invalid request: err = %v", err)
	}
	if called != 0 {
		t.Errorf("handler called %d times for a request the client must not send", called)
	}

	var remote *broker.RemoteError
	_, _, err = b.Request(ctx, "orders.status.get", nil, []byte(`{"orderId":""}`))
	if !errors.As(err, &remote) || remote.Code != "400" || !strings.Contains(remote.Description, "/orderId: want at least 1 characters") {
		t.Errorf("invalid request sent anyway: err = %v, want RemoteError 400", err)
	}
	if called != 0 {
		t.Errorf("handler called %d times for an invalid request", called)
	}

	_, err = c.GetOrderStatus(ctx, nil, orders.StatusRequest{OrderID: "broken"})
	if !errors.As(err, &remote) || remote.Code != "500" ||
		remote.Description != "StatusReply: invalid payload: /status: must be one of [pending paid shipped cancelled]" {
		t.Errorf("invalid reply: err = %v, want RemoteError 500", err)
	}

	resp, err := c.GetOrderStatus(ctx, nil, orders.StatusRequest{OrderID: "o-1"})
	if err != nil || resp.Status != orders.OrderStatusPaid {
		t.Errorf("valid request: %+v, %v", resp, err)
	}
}
