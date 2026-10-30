package broker_test

import (
	"context"
	"errors"
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
