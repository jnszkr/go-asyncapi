// Package broker is the transport contract of the code asyncapi-gen
// generates, for the transports in its subpackages: natsbroker for core NATS,
// jsbroker for JetStream and membroker in memory for tests.
//
// Generated code does not import this package. It declares the same
// interfaces itself, using only predeclared types and aliases of unnamed
// types, so that a transport written anywhere satisfies them without
// importing either package, and one transport serves every generated
// package. An application can use the transports here, wrap them, or copy
// one into its own code.
package broker

import (
	"context"
	"errors"
)

// Publisher is the transport of a generated Producer.
type Publisher interface {
	Publish(ctx context.Context, subject string, headers map[string][]string, body []byte) error
}

// Subscriber is the transport of a generated Consumer.
type Subscriber interface {
	// Subscribe calls h for every message matching filter, whose tokens may
	// be "*". Subscribers sharing queue split the messages between them. The
	// returned Settlement says what to do with the message; the error, if any,
	// says why and is for the transport to report.
	Subscribe(filter, queue string, h func(ctx context.Context, msg Inbox) (Settlement, error)) (Subscription, error)
}

// Requester is the transport of a generated Client.
type Requester interface {
	// Request sends a request and waits for its reply; ctx carries the
	// deadline. A reply reporting a failure is returned as err.
	Request(ctx context.Context, subject string, headers map[string][]string, body []byte) (replyBody []byte, replyHeaders map[string][]string, err error)
}

// Replier is the transport of a generated Server.
type Replier interface {
	// Reply calls h for every request matching subject and sends back what h
	// returns. A non-nil err is sent to the requester as a failure instead:
	// see IsMalformed for whose fault it is.
	Reply(subject, queue string, h func(ctx context.Context, msg Inbox) (replyBody []byte, err error)) (Subscription, error)
}

// Inbox is a received message.
type Inbox = interface {
	Subject() string
	Headers() map[string][]string
	Data() []byte
}

// Subscription is an active subscription.
type Subscription = interface {
	Unsubscribe() error
}

// Settlement tells a Subscriber what to do with a message it delivered.
// Transports without acknowledgements, such as core NATS, ignore it.
type Settlement = int

// Settlements. Zero is not one of them, so a forgotten value is not an Ack:
// transports treat anything else as Nak.
const (
	Ack  Settlement = iota + 1 // handled: do not deliver again
	Nak                        // not handled: deliver again
	Term                       // can never be handled: drop it
)

// IsMalformed reports whether err is about a message that can never be
// handled, such as one whose payload cannot be decoded. Generated code marks
// such errors with a Malformed() bool method returning true. A Replier
// answers them as the requester's fault.
func IsMalformed(err error) bool {
	var m interface{ Malformed() bool }
	return errors.As(err, &m) && m.Malformed()
}

// Headers of the NATS micro protocol, with which the transports here send a
// failure back to the requester.
const (
	HeaderServiceError     = "Nats-Service-Error"
	HeaderServiceErrorCode = "Nats-Service-Error-Code"
)

// Codes of the failures a Replier sends.
const (
	CodeBadRequest = "400" // IsMalformed: the request itself was rejected
	CodeInternal   = "500" // any other failure to answer it
)

// RemoteError is a failure reported by the responder of a request.
type RemoteError struct {
	Code        string
	Description string
}

func (e *RemoteError) Error() string {
	return "remote error " + e.Code + ": " + e.Description
}

// ErrorHeaders returns the headers of a reply reporting err.
func ErrorHeaders(err error) map[string][]string {
	code := CodeInternal
	if IsMalformed(err) {
		code = CodeBadRequest
	}
	return map[string][]string{
		HeaderServiceError:     {err.Error()},
		HeaderServiceErrorCode: {code},
	}
}

// ReplyError returns the *RemoteError carried by the headers of a reply, or
// nil if the reply is a regular one.
func ReplyError(headers map[string][]string) error {
	desc := first(headers[HeaderServiceError])
	if desc == "" {
		return nil
	}
	return &RemoteError{Code: first(headers[HeaderServiceErrorCode]), Description: desc}
}

func first(v []string) string {
	if len(v) == 0 {
		return ""
	}
	return v[0]
}
