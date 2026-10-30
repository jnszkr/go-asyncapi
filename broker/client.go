package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Codec encodes and decodes payloads.
type Codec interface {
	// ContentType is sent in the Content-Type header of outgoing messages.
	ContentType() string
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSON is the default Codec.
type JSON struct{}

func (JSON) ContentType() string                { return "application/json" }
func (JSON) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSON) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// Client pairs a Broker with a codec and error handling. It is the first
// argument of the generic functions in this package.
type Client struct {
	broker         Broker
	codec          Codec
	onError        func(error)
	requestTimeout time.Duration
}

// Option configures a Client.
type Option func(*Client)

// WithCodec replaces the default JSON codec.
func WithCodec(codec Codec) Option { return func(c *Client) { c.codec = codec } }

// WithErrorHandler receives errors from handling received messages
// (decoding failures, handler errors). By default they are discarded.
func WithErrorHandler(fn func(error)) Option { return func(c *Client) { c.onError = fn } }

// WithRequestTimeout applies to requests whose context has no deadline.
// The default is 5 seconds.
func WithRequestTimeout(d time.Duration) Option { return func(c *Client) { c.requestTimeout = d } }

// New returns a Client on top of b.
func New(b Broker, opts ...Option) *Client {
	c := &Client{broker: b, codec: JSON{}, onError: func(error) {}, requestTimeout: 5 * time.Second}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Broker returns the underlying transport.
func (c *Client) Broker() Broker { return c.broker }

const (
	HeaderContentType      = "Content-Type"
	HeaderServiceError     = "Nats-Service-Error"
	HeaderServiceErrorCode = "Nats-Service-Error-Code"
)

// RemoteError is returned by Request when the responder reported an error.
type RemoteError struct {
	Code        string
	Description string
}

func (e *RemoteError) Error() string {
	return "remote error " + e.Code + ": " + e.Description
}

// Received is a decoded incoming message.
type Received[T any] struct {
	Subject string
	Params  Params
	Header  Header
	Payload T
}

// Handler handles messages received by Subscribe.
type Handler[T any] func(ctx context.Context, m Received[T]) error

// Responder handles requests received by Respond and returns the reply.
type Responder[Req, Resp any] func(ctx context.Context, m Received[Req]) (Resp, error)

// MsgOption adjusts an outgoing message.
type MsgOption func(*Message)

// WithHeader sets a header on an outgoing message.
func WithHeader(key, value string) MsgOption { return func(m *Message) { m.Header.Set(key, value) } }

// SubOption adjusts a subscription.
type SubOption func(*subConfig)

type subConfig struct{ queue string }

// Queue load balances messages across subscribers with the same queue name.
func Queue(name string) SubOption { return func(s *subConfig) { s.queue = name } }

// Publish sends msg on ch.
func Publish[T any](ctx context.Context, c *Client, ch Channel[T], p Params, msg T, opts ...MsgOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m, err := c.encode(ch, p, msg, opts)
	if err != nil {
		return err
	}
	return c.broker.Publish(ctx, m)
}

// Request sends req on ch and decodes the reply.
func Request[Req, Resp any](ctx context.Context, c *Client, ch Channel[Req], p Params, req Req, opts ...MsgOption) (Resp, error) {
	var resp Resp
	m, err := c.encode(ch, p, req, opts)
	if err != nil {
		return resp, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.requestTimeout)
		defer cancel()
	}
	rm, err := c.broker.Request(ctx, m)
	if err != nil {
		return resp, fmt.Errorf("request %s: %w", m.Subject, err)
	}
	if desc := rm.Header.Get(HeaderServiceError); desc != "" {
		return resp, &RemoteError{Code: rm.Header.Get(HeaderServiceErrorCode), Description: desc}
	}
	if err := c.codec.Unmarshal(rm.Data, &resp); err != nil {
		return resp, fmt.Errorf("decode reply from %s: %w", m.Subject, err)
	}
	return resp, nil
}

// Subscribe calls h for every message on ch, across all parameter values.
func Subscribe[T any](c *Client, ch Channel[T], h Handler[T], opts ...SubOption) (Subscription, error) {
	return c.subscribe(ch.Pattern(), opts, func(m *Message) {
		r, err := decode(c, ch, m)
		if err == nil {
			err = h(context.Background(), r)
		}
		if err != nil {
			c.onError(fmt.Errorf("%s: %w", m.Subject, err))
		}
	})
}

// Respond answers requests on ch with the value returned by h. Errors are
// sent back to the requester, which receives them as *RemoteError.
func Respond[Req, Resp any](c *Client, ch Channel[Req], h Responder[Req, Resp], opts ...SubOption) (Subscription, error) {
	return c.subscribe(ch.Pattern(), opts, func(m *Message) {
		if m.Reply == "" {
			c.onError(fmt.Errorf("%s: request without reply subject", m.Subject))
			return
		}
		r, err := decode(c, ch, m)
		if err != nil {
			c.respondError(m, "400", err)
			return
		}
		resp, err := h(context.Background(), r)
		if err != nil {
			c.respondError(m, "500", err)
			return
		}
		data, err := c.codec.Marshal(resp)
		if err != nil {
			c.respondError(m, "500", err)
			return
		}
		reply := &Message{Subject: m.Reply, Header: Header{}, Data: data}
		reply.Header.Set(HeaderContentType, c.codec.ContentType())
		if err := c.broker.Publish(context.Background(), reply); err != nil {
			c.onError(fmt.Errorf("%s: respond: %w", m.Subject, err))
		}
	})
}

func (c *Client) encode(ch channelSubject, p Params, payload any, opts []MsgOption) (*Message, error) {
	subject, err := ch.Subject(p)
	if err != nil {
		return nil, err
	}
	data, err := c.codec.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", subject, err)
	}
	m := &Message{Subject: subject, Header: Header{}, Data: data}
	m.Header.Set(HeaderContentType, c.codec.ContentType())
	for _, opt := range opts {
		opt(m)
	}
	return m, nil
}

// channelSubject lets encode accept a Channel of any payload type.
type channelSubject interface {
	Subject(Params) (string, error)
}

func decode[T any](c *Client, ch Channel[T], m *Message) (Received[T], error) {
	r := Received[T]{Subject: m.Subject, Header: m.Header}
	if r.Header == nil {
		r.Header = Header{}
	}
	var err error
	if r.Params, err = ch.ParseSubject(m.Subject); err != nil {
		return r, err
	}
	if err := c.codec.Unmarshal(m.Data, &r.Payload); err != nil {
		return r, fmt.Errorf("decode: %w", err)
	}
	return r, nil
}

func (c *Client) subscribe(subject string, opts []SubOption, h func(*Message)) (Subscription, error) {
	var cfg subConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return c.broker.Subscribe(subject, cfg.queue, h)
}

func (c *Client) respondError(m *Message, code string, err error) {
	reply := &Message{Subject: m.Reply, Header: Header{}}
	reply.Header.Set(HeaderServiceError, err.Error())
	reply.Header.Set(HeaderServiceErrorCode, code)
	if perr := c.broker.Publish(context.Background(), reply); perr != nil {
		c.onError(fmt.Errorf("%s: respond: %w", m.Subject, perr))
	}
}
