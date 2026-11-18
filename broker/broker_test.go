package broker_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jnszkr/go-asyncapi/broker"
)

type malformed struct{ ok bool }

func (m malformed) Error() string   { return "malformed" }
func (m malformed) Malformed() bool { return m.ok }

func TestSettlementsAreNotZero(t *testing.T) {
	if broker.Ack != 1 || broker.Nak != 2 || broker.Term != 3 {
		t.Errorf("settlements = %d, %d, %d; generated code relies on 1, 2, 3", broker.Ack, broker.Nak, broker.Term)
	}
}

func TestIsMalformed(t *testing.T) {
	for err, want := range map[error]bool{
		nil:                  false,
		errors.New("plain"):  false,
		malformed{ok: true}:  true,
		malformed{ok: false}: false,
		fmt.Errorf("wrapped: %w", malformed{true}): true,
	} {
		if got := broker.IsMalformed(err); got != want {
			t.Errorf("IsMalformed(%v) = %v, want %v", err, got, want)
		}
	}
}

func TestErrorHeadersRoundTrip(t *testing.T) {
	for err, code := range map[error]string{
		errors.New("boom"):   broker.CodeInternal,
		malformed{ok: true}:  broker.CodeBadRequest,
		malformed{ok: false}: broker.CodeInternal,
	} {
		h := broker.ErrorHeaders(err)
		if got := h[broker.HeaderServiceErrorCode]; len(got) != 1 || got[0] != code {
			t.Errorf("%v: code header = %v, want %s", err, got, code)
		}
		var remote *broker.RemoteError
		if !errors.As(broker.ReplyError(h), &remote) || remote.Code != code || remote.Description != err.Error() {
			t.Errorf("%v: ReplyError = %v", err, broker.ReplyError(h))
		}
		if want := "remote error " + code + ": " + err.Error(); remote.Error() != want {
			t.Errorf("Error() = %q, want %q", remote.Error(), want)
		}
	}
}

func TestReplyErrorOfRegularReplies(t *testing.T) {
	for _, h := range []map[string][]string{
		nil,
		{},
		{broker.HeaderServiceError: {}},
		{broker.HeaderServiceError: {""}},
		{broker.HeaderServiceErrorCode: {"500"}},
	} {
		if err := broker.ReplyError(h); err != nil {
			t.Errorf("ReplyError(%v) = %v, want nil", h, err)
		}
	}
	err := broker.ReplyError(map[string][]string{broker.HeaderServiceError: {"no code"}})
	var remote *broker.RemoteError
	if !errors.As(err, &remote) || remote.Code != "" || remote.Description != "no code" {
		t.Errorf("ReplyError without a code = %#v", err)
	}
}
