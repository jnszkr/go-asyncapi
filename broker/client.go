package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"strings"
	"sync"
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

// ErrorCodec maps errors to and from reply messages, for applications whose
// requesters and responders follow their own convention.
type ErrorCodec interface {
	// EncodeError writes err onto a reply, which has an empty Header and no
	// Data yet. code is "400" when the request itself was rejected, and "500"
	// for any other failure to answer it.
	EncodeError(reply *Message, code string, err error)
	// DecodeError returns the error carried by a reply, or nil if it holds
	// a regular response. Request returns that error to its caller as is.
	DecodeError(reply *Message) error
}

// ServiceErrors is the default ErrorCodec. It uses the headers of the NATS
// micro protocol, HeaderServiceError and HeaderServiceErrorCode, and decodes
// to *RemoteError.
type ServiceErrors struct{}

func (ServiceErrors) EncodeError(reply *Message, code string, err error) {
	reply.Header.Set(HeaderServiceError, err.Error())
	reply.Header.Set(HeaderServiceErrorCode, code)
}

func (ServiceErrors) DecodeError(reply *Message) error {
	if desc := reply.Header.Get(HeaderServiceError); desc != "" {
		return &RemoteError{Code: reply.Header.Get(HeaderServiceErrorCode), Description: desc}
	}
	return nil
}

// Validator checks payloads against JSON Schemas. See schemavalidate for an
// implementation.
type Validator interface {
	// Compile prepares a JSON Schema document. The function it returns reports
	// why a payload does not conform to the schema. It must be safe for
	// concurrent use.
	Compile(schema string) (func(data []byte) error, error)
}

// Client pairs a transport with a codec and error handling. It is the first
// argument of the generic functions in this package.
type Client struct {
	transport      any
	pub            Publisher
	req            Requester
	sub            Subscriber
	codec          Codec
	errors         ErrorCodec
	onError        func(error)
	requestTimeout time.Duration
	validator      Validator

	publishInterceptors []PublishInterceptor
	requestInterceptors []RequestInterceptor
	receiveInterceptors []ReceiveInterceptor
	compiled            sync.Map // schema string → func([]byte) error
}

// Option configures a Client.
type Option func(*Client)

// WithCodec replaces the default JSON codec.
func WithCodec(codec Codec) Option { return func(c *Client) { c.codec = codec } }

// WithErrorHandler receives errors from handling received messages
// (decoding failures, handler errors). By default they are discarded.
func WithErrorHandler(fn func(error)) Option { return func(c *Client) { c.onError = fn } }

// WithErrorCodec replaces how errors travel in replies. The default is
// ServiceErrors.
func WithErrorCodec(codec ErrorCodec) Option { return func(c *Client) { c.errors = codec } }

// WithRequestTimeout applies to requests whose context has no deadline.
// The default is 5 seconds.
func WithRequestTimeout(d time.Duration) Option { return func(c *Client) { c.requestTimeout = d } }

// WithValidator checks the payloads the Client sends and receives against the
// Schema and ReplySchema of their Channel. By default nothing is checked.
//
// A received payload that fails is treated as malformed: its error wraps both
// ErrMalformed and ErrInvalidPayload. A payload about to be sent is not sent:
// its error wraps only ErrInvalidPayload, so that a handler returning it does
// not get the message it is handling dropped as malformed.
//
// Only JSON is checked: with a codec whose ContentType is not JSON, the
// validator is not used.
func WithValidator(v Validator) Option { return func(c *Client) { c.validator = v } }

// New returns a Client on top of b.
func New(b Broker, opts ...Option) *Client { return newClient(b, opts) }

// ForPublisher returns a Client on top of a transport that publishes. See
// ErrUnsupported for the operations it lacks.
func ForPublisher(p Publisher, opts ...Option) *Client { return newClient(p, opts) }

// ForRequester returns a Client on top of a transport that sends requests.
// See ErrUnsupported for the operations it lacks.
func ForRequester(r Requester, opts ...Option) *Client { return newClient(r, opts) }

// ForSubscriber returns a Client on top of a transport that subscribes. See
// ErrUnsupported for the operations it lacks.
func ForSubscriber(s Subscriber, opts ...Option) *Client { return newClient(s, opts) }

// ForPubSub returns a Client on top of a transport that subscribes and
// publishes, as answering requests takes. See ErrUnsupported for the
// operations it lacks.
func ForPubSub(ps PubSub, opts ...Option) *Client { return newClient(ps, opts) }

func newClient(transport any, opts []Option) *Client {
	c := &Client{transport: transport, codec: JSON{}, errors: ServiceErrors{}, onError: func(error) {}, requestTimeout: 5 * time.Second}
	c.pub, _ = transport.(Publisher)
	c.req, _ = transport.(Requester)
	c.sub, _ = transport.(Subscriber)
	for _, opt := range opts {
		opt(c)
	}
	if c.pub != nil {
		c.pub = chain(PublishFunc(c.pub.Publish), c.publishInterceptors)
	}
	if req := c.req; req != nil {
		// Replies get a Header for the interceptors, as received messages do.
		c.req = chain(RequestFunc(func(ctx context.Context, m *Message) (*Message, error) {
			r, err := req.Request(ctx, m)
			if r != nil && r.Header == nil {
				r.Header = Header{}
			}
			return r, err
		}), c.requestInterceptors)
	}
	return c
}

// Broker returns the transport the Client was created with.
func (c *Client) Broker() any { return c.transport }

func (c *Client) unsupported(op string) error {
	return fmt.Errorf("%w: %T cannot %s", ErrUnsupported, c.transport, op)
}

const (
	HeaderContentType      = "Content-Type"
	HeaderServiceError     = "Nats-Service-Error"
	HeaderServiceErrorCode = "Nats-Service-Error-Code"
)

// ErrMalformed wraps errors about messages that can never be handled, such
// as undecodable or invalid payloads, or subjects that don't match the
// channel. Brokers with redelivery drop these instead of retrying them.
var ErrMalformed = errors.New("malformed message")

// ErrUnsupported is returned by the generic functions when the Client's
// transport lacks the operation they need.
var ErrUnsupported = errors.New("operation not supported by transport")

// ErrInvalidPayload wraps errors about payloads that do not conform to their
// schema. See WithValidator.
var ErrInvalidPayload = errors.New("invalid payload")

// RemoteError is returned by Request when the responder reported an error
// and the Client uses ServiceErrors.
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
type SubOption func(*SubConfig)

// Queue load balances messages across subscribers with the same queue name.
func Queue(name string) SubOption { return func(s *SubConfig) { s.Queue = name } }

// WithSource sets SubConfig.Source. Transports wrap it in typed options,
// such as jsbroker.From.
func WithSource(src any) SubOption { return func(s *SubConfig) { s.Source = src } }

// Publish sends msg on ch.
func Publish[T any](ctx context.Context, c *Client, ch Channel[T], p Params, msg T, opts ...MsgOption) error {
	if c.pub == nil {
		return c.unsupported("publish")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m, err := c.encode(ch, ch.Schema, p, msg, opts)
	if err != nil {
		return err
	}
	return c.pub.Publish(ctx, m)
}

// Request sends req on ch and decodes the reply.
func Request[Req, Resp any](ctx context.Context, c *Client, ch Channel[Req], p Params, req Req, opts ...MsgOption) (Resp, error) {
	var resp Resp
	if c.req == nil {
		return resp, c.unsupported("request")
	}
	m, err := c.encode(ch, ch.Schema, p, req, opts)
	if err != nil {
		return resp, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.requestTimeout)
		defer cancel()
	}
	rm, err := c.req.Request(ctx, m)
	if err != nil {
		return resp, fmt.Errorf("request %s: %w", m.Subject, err)
	}
	if err := c.errors.DecodeError(rm); err != nil {
		return resp, err
	}
	if err := c.validateReceived(ch.ReplySchema, rm.Data); err != nil {
		return resp, fmt.Errorf("reply from %s: %w", m.Subject, err)
	}
	if err := c.codec.Unmarshal(rm.Data, &resp); err != nil {
		return resp, fmt.Errorf("decode reply from %s: %w", m.Subject, err)
	}
	return resp, nil
}

// Subscribe calls h for every message on ch, across all parameter values,
// with the context the transport provides for the message.
func Subscribe[T any](c *Client, ch Channel[T], h Handler[T], opts ...SubOption) (Subscription, error) {
	if _, err := c.checker(ch.Schema); err != nil {
		return nil, err
	}
	return c.subscribe(ch.Pattern(), opts, func(ctx context.Context, m *Message) error {
		r, err := decode(c, ch, m)
		if err == nil {
			err = h(ctx, r)
		}
		if err != nil {
			err = fmt.Errorf("%s: %w", m.Subject, err)
			c.onError(err)
		}
		return err
	})
}

// Respond answers requests on ch with the value returned by h, called with
// the context the transport provides for the request. Replies are published
// with that context's values, but not its deadline: a handler that ran out of
// time can still report it. Errors are sent back to the requester using the
// Client's ErrorCodec, by default as *RemoteError.
func Respond[Req, Resp any](c *Client, ch Channel[Req], h Responder[Req, Resp], opts ...SubOption) (Subscription, error) {
	if c.pub == nil {
		return nil, c.unsupported("publish replies")
	}
	for _, schema := range []string{ch.Schema, ch.ReplySchema} {
		if _, err := c.checker(schema); err != nil {
			return nil, err
		}
	}
	return c.subscribe(ch.Pattern(), opts, func(ctx context.Context, m *Message) error {
		if m.Reply == "" {
			err := fmt.Errorf("%s: %w: request without reply subject", m.Subject, ErrMalformed)
			c.onError(err)
			return err
		}
		r, err := decode(c, ch, m)
		if err != nil {
			return c.respondError(ctx, m, "400", err)
		}
		resp, err := h(ctx, r)
		if err != nil {
			return c.respondError(ctx, m, "500", err)
		}
		data, err := c.codec.Marshal(resp)
		if err != nil {
			return c.respondError(ctx, m, "500", err)
		}
		if err := c.validate(ch.ReplySchema, data); err != nil {
			return c.respondError(ctx, m, "500", err)
		}
		reply := &Message{Subject: m.Reply, Header: Header{}, Data: data}
		reply.Header.Set(HeaderContentType, c.codec.ContentType())
		return c.reply(ctx, m, reply)
	})
}

func (c *Client) encode(ch channelSubject, schema string, p Params, payload any, opts []MsgOption) (*Message, error) {
	subject, err := ch.Subject(p)
	if err != nil {
		return nil, err
	}
	data, err := c.codec.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", subject, err)
	}
	if err := c.validate(schema, data); err != nil {
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
	var err error
	if r.Params, err = ch.ParseSubject(m.Subject); err != nil {
		return r, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if err := c.validateReceived(ch.Schema, m.Data); err != nil {
		return r, err
	}
	if err := c.codec.Unmarshal(m.Data, &r.Payload); err != nil {
		return r, fmt.Errorf("%w: decode: %w", ErrMalformed, err)
	}
	return r, nil
}

// checker returns the function checking payloads against schema, or nil when
// they are not checked.
func (c *Client) checker(schema string) (func([]byte) error, error) {
	if c.validator == nil || schema == "" || !isJSON(c.codec.ContentType()) {
		return nil, nil
	}
	if check, ok := c.compiled.Load(schema); ok {
		return check.(func([]byte) error), nil
	}
	check, err := c.validator.Compile(schema)
	if err != nil {
		return nil, fmt.Errorf("compile schema: %w", err)
	}
	c.compiled.Store(schema, check)
	return check, nil
}

// validate checks data against schema. Only a payload that fails the check
// gives an error wrapping ErrInvalidPayload: a schema that cannot be compiled
// says nothing about the payload.
func (c *Client) validate(schema string, data []byte) error {
	check, err := c.checker(schema)
	if err != nil || check == nil {
		return err
	}
	if err := check(data); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPayload, err)
	}
	return nil
}

// validateReceived is validate for incoming payloads, which are malformed when
// they fail the check.
func (c *Client) validateReceived(schema string, data []byte) error {
	err := c.validate(schema, data)
	if errors.Is(err, ErrInvalidPayload) {
		return fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	return err
}

// isJSON reports whether contentType is JSON or a structured +json type.
func isJSON(contentType string) bool {
	media, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	return media == "application/json" || strings.HasSuffix(media, "+json")
}

func (c *Client) subscribe(subject string, opts []SubOption, h func(context.Context, *Message) error) (Subscription, error) {
	if c.sub == nil {
		return nil, c.unsupported("subscribe")
	}
	var cfg SubConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.Subject = subject
	receive := chain(ReceiveFunc(h), c.receiveInterceptors)
	return c.sub.Subscribe(cfg, func(ctx context.Context, m *Message) error {
		if m.Header == nil {
			m.Header = Header{}
		}
		return receive(ctx, m)
	})
}

// respondError sends err back to the requester. The request counts as
// handled once the error reply is sent.
func (c *Client) respondError(ctx context.Context, m *Message, code string, err error) error {
	reply := &Message{Subject: m.Reply, Header: Header{}}
	c.errors.EncodeError(reply, code, err)
	return c.reply(ctx, m, reply)
}

// reply publishes the reply to request m.
func (c *Client) reply(ctx context.Context, m, reply *Message) error {
	if err := c.pub.Publish(context.WithoutCancel(ctx), reply); err != nil {
		err = fmt.Errorf("%s: respond: %w", m.Subject, err)
		c.onError(err)
		return err
	}
	return nil
}
