package schemavalidate_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/jsbroker"
	"github.com/jnszkr/go-asyncapi/broker/natsbroker"
	"github.com/jnszkr/go-asyncapi/broker/schemavalidate"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type payload = map[string]any

const schema = `{
	"type": "object",
	"required": ["id"],
	"properties": {
		"id": {"type": "string", "pattern": "^o-"},
		"count": {"type": "integer", "minimum": 1},
		"kind": {"enum": ["a", "b"]},
		"state": {"not": {"const": "bad"}}
	},
	"if": {"required": ["express"]},
	"then": {"required": ["courier"]}
}`

// invalid holds a payload breaking each keyword. The error is expected to
// name the offending location.
var invalid = []struct {
	keyword string
	data    string
	want    string
}{
	{"required", `{}`, "missing property 'id'"},
	{"minimum", `{"id":"o-1","count":0}`, "/count"},
	{"pattern", `{"id":"x-1"}`, "/id"},
	{"enum", `{"id":"o-1","kind":"c"}`, "/kind"},
	{"not", `{"id":"o-1","state":"bad"}`, "/state"},
	{"if", `{"id":"o-1","express":true}`, "missing property 'courier'"},
}

const valid = `{"id":"o-1","count":1,"kind":"a","state":"ok","express":true,"courier":"x"}`

func decodeJSON(t *testing.T, data string) payload {
	t.Helper()
	var p payload
	if err := (broker.JSON{}).Unmarshal([]byte(data), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestValidate(t *testing.T) {
	check, err := schemavalidate.New().Compile(schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := check([]byte(valid)); err != nil {
		t.Errorf("valid payload: %v", err)
	}
	for _, tc := range invalid {
		t.Run(tc.keyword, func(t *testing.T) {
			err := check([]byte(tc.data))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			// The error travels in a NATS header when a responder reports it.
			if strings.Contains(err.Error(), "\n") {
				t.Errorf("error %q spans several lines", err)
			}
			// Requesters see it too, so it must not reveal local paths.
			if strings.Contains(err.Error(), "file:") {
				t.Errorf("error %q names the schema's location", err)
			}
		})
	}
	for _, notJSON := range []string{``, `{`, `{} {}`} {
		if err := check([]byte(notJSON)); err == nil {
			t.Errorf("%q: expected an error", notJSON)
		}
	}
}

func TestLargeIntegersKeepPrecision(t *testing.T) {
	check, err := schemavalidate.New().Compile(`{"maximum": 9007199254740992}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := check([]byte(`9007199254740993`)); err == nil {
		t.Error("expected a value one above the maximum to fail")
	}
}

func TestInvalidSchema(t *testing.T) {
	for _, s := range []string{`{`, `{"type": 1}`, `{"pattern": "("}`, `{"$ref": "https://example.com/other.json"}`} {
		if _, err := schemavalidate.New().Compile(s); err == nil {
			t.Errorf("%s: expected an error", s)
		}
	}
}

func TestRecursiveRefs(t *testing.T) {
	check, err := schemavalidate.New().Compile(`{
		"$ref": "#/definitions/Node",
		"definitions": {"Node": {
			"type": "object",
			"required": ["v"],
			"properties": {"v": {"type": "integer"}, "next": {"$ref": "#/definitions/Node"}}
		}}
	}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := check([]byte(`{"v":1,"next":{"v":2}}`)); err != nil {
		t.Error(err)
	}
	if err := check([]byte(`{"v":1,"next":{"next":{"v":2}}}`)); err == nil {
		t.Error("expected the nested node without v to fail")
	}
}

var (
	channel = broker.Channel[payload]{Address: "orders.{kind}", Schema: schema}
	plain   = broker.Channel[payload]{Address: "orders.{kind}"}
)

var params = broker.Params{"kind": "x"}

func checkInvalid(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, broker.ErrInvalidPayload) {
		t.Errorf("err = %v, want ErrInvalidPayload", err)
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

func TestNATSReceive(t *testing.T) {
	nc := connect(t)
	errs := make(chan error, 1)
	got := make(chan payload, 1)
	c := broker.New(natsbroker.New(nc), broker.WithValidator(schemavalidate.New()),
		broker.WithErrorHandler(func(err error) { errs <- err }))
	sub, err := broker.Subscribe(c, channel, func(_ context.Context, m broker.Received[payload]) error {
		got <- m.Payload
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	for _, tc := range invalid {
		t.Run(tc.keyword, func(t *testing.T) {
			if err := nc.Publish("orders.x", []byte(tc.data)); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-errs:
				checkInvalid(t, err)
				if !errors.Is(err, broker.ErrMalformed) {
					t.Errorf("err = %v, want ErrMalformed", err)
				}
			case p := <-got:
				t.Errorf("handler received %v", p)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out")
			}
		})
	}
	if err := nc.Publish("orders.x", []byte(valid)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	case err := <-errs:
		t.Errorf("valid payload: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestNATSSend(t *testing.T) {
	nc := connect(t)
	raw, err := nc.SubscribeSync("orders.*")
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	c := broker.New(natsbroker.New(nc), broker.WithValidator(schemavalidate.New()))
	ctx := context.Background()
	for _, tc := range invalid {
		t.Run(tc.keyword, func(t *testing.T) {
			err := broker.Publish(ctx, c, channel, params, decodeJSON(t, tc.data))
			checkInvalid(t, err)
			// A handler returning this error must not get its own message dropped.
			if errors.Is(err, broker.ErrMalformed) {
				t.Errorf("err = %v must not wrap ErrMalformed", err)
			}
			_, err = broker.Request[payload, payload](ctx, c, channel, params, decodeJSON(t, tc.data))
			checkInvalid(t, err)
		})
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if m, err := raw.NextMsg(100 * time.Millisecond); err == nil {
		t.Errorf("rejected payload was sent: %s", m.Data)
	}
	if err := broker.Publish(ctx, c, channel, params, decodeJSON(t, valid)); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.NextMsg(5 * time.Second); err != nil {
		t.Errorf("valid payload: %v", err)
	}
}

func TestRespondReportsInvalidRequest(t *testing.T) {
	nc := connect(t)
	b := natsbroker.New(nc)
	var calls atomic.Int32
	server := broker.New(b, broker.WithValidator(schemavalidate.New()))
	sub, err := broker.Respond(server, channel, func(_ context.Context, m broker.Received[payload]) (payload, error) {
		calls.Add(1)
		return m.Payload, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	client := broker.New(b) // does not validate, so the responder sees everything
	ctx := context.Background()
	for _, tc := range invalid {
		t.Run(tc.keyword, func(t *testing.T) {
			_, err := broker.Request[payload, payload](ctx, client, channel, params, decodeJSON(t, tc.data))
			var remote *broker.RemoteError
			if !errors.As(err, &remote) || remote.Code != "400" {
				t.Fatalf("err = %v, want a RemoteError with code 400", err)
			}
			if !strings.Contains(remote.Description, tc.want) {
				t.Errorf("description %q does not mention %q", remote.Description, tc.want)
			}
		})
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("responder called %d times", n)
	}
	if _, err := broker.Request[payload, payload](ctx, client, channel, params, decodeJSON(t, valid)); err != nil {
		t.Errorf("valid request: %v", err)
	}
}

func TestReplyValidation(t *testing.T) {
	nc := connect(t)
	b := natsbroker.New(nc)
	validating := broker.WithValidator(schemavalidate.New())
	withReply := plain.WithReplySchema(schema)
	ctx := context.Background()
	h := func(_ context.Context, m broker.Received[payload]) (payload, error) { return m.Payload, nil }

	// The responder refuses to send an invalid reply.
	sub, err := broker.Respond(broker.New(b, validating), withReply, h)
	if err != nil {
		t.Fatal(err)
	}
	var remote *broker.RemoteError
	_, err = broker.Request[payload, payload](ctx, broker.New(b), plain, params, payload{"id": "x-1"})
	if !errors.As(err, &remote) || remote.Code != "500" {
		t.Errorf("err = %v, want a RemoteError with code 500", err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}

	// The requester refuses an invalid reply.
	sub, err = broker.Respond(broker.New(b), plain, h)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	c := broker.New(b, validating)
	_, err = broker.Request[payload, payload](ctx, c, withReply, params, payload{"id": "x-1"})
	checkInvalid(t, err)
	if !errors.Is(err, broker.ErrMalformed) {
		t.Errorf("err = %v, want ErrMalformed", err)
	}
	if _, err := broker.Request[payload, payload](ctx, c, withReply, params, decodeJSON(t, valid)); err != nil {
		t.Errorf("valid reply: %v", err)
	}
}

func jetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
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
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}}); err != nil {
		t.Fatal(err)
	}
	return js
}

// TestJetStreamReceive checks that an invalid payload is terminated rather
// than redelivered, and never reaches the handler.
func TestJetStreamReceive(t *testing.T) {
	js := jetStream(t)
	ctx := context.Background()
	cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:       "workers",
		FilterSubject: channel.Pattern(),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	var rejected, malformed, handled atomic.Int32
	c := broker.New(jsbroker.New(js), broker.WithValidator(schemavalidate.New()),
		broker.WithErrorHandler(func(err error) {
			rejected.Add(1)
			if errors.Is(err, broker.ErrMalformed) && errors.Is(err, broker.ErrInvalidPayload) {
				malformed.Add(1)
			}
		}))
	sub, err := broker.Subscribe(c, channel, func(context.Context, broker.Received[payload]) error {
		handled.Add(1)
		return nil
	}, jsbroker.From(cons))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	for _, tc := range invalid {
		if _, err := js.Publish(ctx, "orders."+tc.keyword, []byte(tc.data)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := js.Publish(ctx, "orders.valid", []byte(valid)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := cons.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.NumAckPending == 0 && info.NumPending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer not settled: %d ack pending, %d pending", info.NumAckPending, info.NumPending)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Longer than AckWait, so a redelivery would have happened by now.
	time.Sleep(400 * time.Millisecond)
	want := int32(len(invalid))
	if n := rejected.Load(); n != want {
		t.Errorf("rejected %d deliveries, want %d", n, want)
	}
	if n := malformed.Load(); n != want {
		t.Errorf("%d rejections wrap ErrMalformed and ErrInvalidPayload, want %d", n, want)
	}
	if n := handled.Load(); n != 1 {
		t.Errorf("handler called %d times, want 1", n)
	}
}

func TestJetStreamSend(t *testing.T) {
	js := jetStream(t)
	ctx := context.Background()
	c := broker.New(jsbroker.New(js), broker.WithValidator(schemavalidate.New()))
	for _, tc := range invalid {
		t.Run(tc.keyword, func(t *testing.T) {
			checkInvalid(t, broker.Publish(ctx, c, channel, params, decodeJSON(t, tc.data)))
		})
	}
	stream, err := js.Stream(ctx, "ORDERS")
	if err != nil {
		t.Fatal(err)
	}
	if info, err := stream.Info(ctx); err != nil || info.State.Msgs != 0 {
		t.Errorf("stream holds rejected payloads: %+v, %v", info, err)
	}
	if err := broker.Publish(ctx, c, channel, params, decodeJSON(t, valid)); err != nil {
		t.Errorf("valid payload: %v", err)
	}
}
