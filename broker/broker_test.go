package broker_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/jnszkr/go-asyncapi/broker/membroker"
)

type greeting struct {
	Text string `json:"text"`
}

var greetings = broker.Channel[greeting]{
	Address: "greet.{lang}.{name}",
	Allowed: map[string][]string{"lang": {"en", "hu"}},
}

func TestChannel(t *testing.T) {
	subj, err := greetings.Subject(broker.Params{"lang": "en", "name": "ada"})
	if err != nil || subj != "greet.en.ada" {
		t.Fatalf("Subject = %q, %v", subj, err)
	}
	if got := greetings.Pattern(); got != "greet.*.*" {
		t.Errorf("Pattern = %q", got)
	}
	p, err := greetings.ParseSubject("greet.hu.bob")
	if err != nil || p["lang"] != "hu" || p["name"] != "bob" {
		t.Errorf("ParseSubject = %v, %v", p, err)
	}
	for _, bad := range []broker.Params{
		{"lang": "en"},                // missing
		{"lang": "de", "name": "ada"}, // not allowed
		{"lang": "en", "name": "a.b"}, // multiple tokens
		{"lang": "en", "name": "*"},   // wildcard
	} {
		if _, err := greetings.Subject(bad); err == nil {
			t.Errorf("Subject(%v): expected error", bad)
		}
	}
	for _, bad := range []string{"greet.en", "hello.en.ada", "greet.en.ada.x"} {
		if _, err := greetings.ParseSubject(bad); err == nil {
			t.Errorf("ParseSubject(%q): expected error", bad)
		}
	}
}

func TestPublishSubscribe(t *testing.T) {
	c := broker.New(membroker.New())
	var got broker.Received[greeting]
	_, err := broker.Subscribe(c, greetings, func(_ context.Context, m broker.Received[greeting]) error {
		got = m
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.Publish(context.Background(), c, greetings, broker.Params{"lang": "hu", "name": "ada"},
		greeting{Text: "szia"}, broker.WithHeader("trace", "t-1"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Payload.Text != "szia" || got.Params["name"] != "ada" || got.Header.Get("trace") != "t-1" ||
		got.Header.Get(broker.HeaderContentType) != "application/json" {
		t.Errorf("received %+v", got)
	}
}

func TestQueue(t *testing.T) {
	c := broker.New(membroker.New())
	counts := [2]int{}
	for i := range counts {
		_, err := broker.Subscribe(c, greetings, func(context.Context, broker.Received[greeting]) error {
			counts[i]++
			return nil
		}, broker.Queue("workers"))
		if err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		if err := broker.Publish(context.Background(), c, greetings, broker.Params{"lang": "en", "name": "x"}, greeting{}); err != nil {
			t.Fatal(err)
		}
	}
	if counts != [2]int{2, 2} {
		t.Errorf("queue distribution = %v, want [2 2]", counts)
	}
}

func TestHandlerErrorsReported(t *testing.T) {
	var reported error
	c := broker.New(membroker.New(), broker.WithErrorHandler(func(err error) { reported = err }))
	_, _ = broker.Subscribe(c, greetings, func(context.Context, broker.Received[greeting]) error {
		return errors.New("boom")
	})
	_ = broker.Publish(context.Background(), c, greetings, broker.Params{"lang": "en", "name": "x"}, greeting{})
	if reported == nil {
		t.Fatal("handler error was not reported")
	}
}

type length struct {
	N int `json:"n"`
}

var measure = broker.Channel[greeting]{Address: "measure"}

func TestRequestRespond(t *testing.T) {
	c := broker.New(membroker.New())
	_, err := broker.Respond(c, measure, func(_ context.Context, m broker.Received[greeting]) (length, error) {
		if m.Payload.Text == "" {
			return length{}, errors.New("empty")
		}
		return length{N: len(m.Payload.Text)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	resp, err := broker.Request[greeting, length](ctx, c, measure, nil, greeting{Text: "hello"})
	if err != nil || resp.N != 5 {
		t.Errorf("Request = %+v, %v", resp, err)
	}
	_, err = broker.Request[greeting, length](ctx, c, measure, nil, greeting{})
	var remote *broker.RemoteError
	if !errors.As(err, &remote) || remote.Code != "500" || remote.Description != "empty" {
		t.Errorf("err = %v, want RemoteError 500", err)
	}
}

func TestChannelDefaults(t *testing.T) {
	ch := broker.Channel[string]{
		Address:  "a.{region}.{id}",
		Allowed:  map[string][]string{"region": {"eu"}},
		Defaults: map[string]string{"region": "eu"},
	}
	got, err := ch.Subject(broker.Params{"id": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "a.eu.1" {
		t.Errorf("subject = %q, want the default region", got)
	}
	if _, err := ch.Subject(broker.Params{"region": "us", "id": "1"}); err == nil {
		t.Error("a value outside Allowed was accepted")
	}
	if _, err := ch.Subject(broker.Params{}); err == nil {
		t.Error("a parameter without a value or default was accepted")
	}
}

// rejectBad is a Validator rejecting the payload `"bad"` under any schema
// except "uncompilable". It counts compilations.
type rejectBad struct{ compiled *atomic.Int32 }

func (v rejectBad) Compile(schema string) (func([]byte) error, error) {
	v.compiled.Add(1)
	if schema == "uncompilable" {
		return nil, errors.New("cannot compile")
	}
	return func(data []byte) error {
		if string(data) == `"bad"` {
			return errors.New("rejected")
		}
		return nil
	}, nil
}

var validated = broker.Channel[string]{Address: "validated", Schema: "s"}

func TestValidationIsOptional(t *testing.T) {
	c := broker.New(membroker.New())
	var got string
	if _, err := broker.Subscribe(c, validated, func(_ context.Context, m broker.Received[string]) error {
		got = m.Payload
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), c, validated, nil, "bad"); err != nil {
		t.Fatal(err)
	}
	if got != "bad" {
		t.Errorf("received %q", got)
	}
}

func TestChannelWithoutSchemaIsNotValidated(t *testing.T) {
	c := broker.New(membroker.New(), broker.WithValidator(rejectBad{new(atomic.Int32)}))
	plain := broker.Channel[string]{Address: "validated"}
	if err := broker.Publish(context.Background(), c, plain, nil, "bad"); err != nil {
		t.Error(err)
	}
}

type textCodec struct{ contentType string }

func (c textCodec) ContentType() string           { return c.contentType }
func (textCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (textCodec) Unmarshal(d []byte, v any) error { return json.Unmarshal(d, v) }

func TestValidationFollowsCodec(t *testing.T) {
	for contentType, validates := range map[string]bool{
		"application/json":                true,
		"Application/JSON; charset=utf-8": true,
		"application/cloudevents+json":    true,
		"application/x-protobuf":          false,
		"text/plain":                      false,
		"application/jsonish":             false,
	} {
		c := broker.New(membroker.New(), broker.WithCodec(textCodec{contentType}),
			broker.WithValidator(rejectBad{new(atomic.Int32)}))
		err := broker.Publish(context.Background(), c, validated, nil, "bad")
		if got := errors.Is(err, broker.ErrInvalidPayload); got != validates {
			t.Errorf("%s: err = %v, want validated = %v", contentType, err, validates)
		}
	}
}

func TestSchemasAreCompiledOnce(t *testing.T) {
	var compiled atomic.Int32
	c := broker.New(membroker.New(), broker.WithValidator(rejectBad{&compiled}))
	if _, err := broker.Subscribe(c, validated, func(context.Context, broker.Received[string]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := broker.Publish(context.Background(), c, validated, nil, "ok"); err != nil {
			t.Fatal(err)
		}
	}
	if n := compiled.Load(); n != 1 {
		t.Errorf("compiled %d times, want 1", n)
	}
}

// TestUncompilableSchema checks that a schema the Validator cannot use fails
// when subscribing, not by rejecting every message as malformed.
func TestUncompilableSchema(t *testing.T) {
	c := broker.New(membroker.New(), broker.WithValidator(rejectBad{new(atomic.Int32)}))
	ch := broker.Channel[string]{Address: "validated", Schema: "uncompilable"}
	if _, err := broker.Subscribe(c, ch, func(context.Context, broker.Received[string]) error { return nil }); err == nil {
		t.Error("Subscribe: expected an error")
	}
	if _, err := broker.Respond(c, ch, func(context.Context, broker.Received[string]) (string, error) { return "", nil }); err == nil {
		t.Error("Respond: expected an error")
	}
	reply := validated.WithReplySchema("uncompilable")
	if _, err := broker.Respond(c, reply, func(context.Context, broker.Received[string]) (string, error) { return "", nil }); err == nil {
		t.Error("Respond with a reply schema: expected an error")
	}
	err := broker.Publish(context.Background(), c, ch, nil, "ok")
	if err == nil || errors.Is(err, broker.ErrInvalidPayload) || errors.Is(err, broker.ErrMalformed) {
		t.Errorf("Publish: err = %v, want a schema error", err)
	}
}

func TestWithReplySchemaLeavesChannelUnchanged(t *testing.T) {
	if got := validated.WithReplySchema("r"); got.ReplySchema != "r" || got.Schema != "s" || validated.ReplySchema != "" {
		t.Errorf("got %+v, original %+v", got, validated)
	}
}
