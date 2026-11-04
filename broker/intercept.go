package broker

import "context"

// PublishFunc sends a message. It implements Publisher.
type PublishFunc func(ctx context.Context, m *Message) error

// Publish calls f.
func (f PublishFunc) Publish(ctx context.Context, m *Message) error { return f(ctx, m) }

// RequestFunc sends a request and waits for its reply. It implements Requester.
type RequestFunc func(ctx context.Context, m *Message) (*Message, error)

// Request calls f.
func (f RequestFunc) Request(ctx context.Context, m *Message) (*Message, error) { return f(ctx, m) }

// ReceiveFunc handles a received message.
type ReceiveFunc func(ctx context.Context, m *Message) error

// PublishInterceptor wraps the publishing of encoded messages, including the
// replies sent by Respond. It can change the message, e.g. to add headers.
type PublishInterceptor func(next PublishFunc) PublishFunc

// RequestInterceptor wraps sending a request and waiting for its reply, e.g. to
// add headers or retry. ctx carries the deadline. A reply holding an error is
// a regular reply here: the Client's ErrorCodec decodes it afterwards.
type RequestInterceptor func(next RequestFunc) RequestFunc

// ReceiveInterceptor wraps the handling of received messages by Subscribe and
// Respond. The next ReceiveFunc decodes the message and runs the application's
// handler, with the context it is given, so failures to decode wrap
// ErrMalformed. The returned error goes to the transport: on JetStream it
// decides whether the message is redelivered, so returning nil after moving a
// message to a dead letter queue settles it. Respond sends handler errors to
// the requester and returns nil for them.
type ReceiveInterceptor func(next ReceiveFunc) ReceiveFunc

// WithPublishInterceptors adds interceptors, the first being the outermost.
func WithPublishInterceptors(i ...PublishInterceptor) Option {
	return func(c *Client) { c.publishInterceptors = append(c.publishInterceptors, i...) }
}

// WithRequestInterceptors adds interceptors, the first being the outermost.
func WithRequestInterceptors(i ...RequestInterceptor) Option {
	return func(c *Client) { c.requestInterceptors = append(c.requestInterceptors, i...) }
}

// WithReceiveInterceptors adds interceptors, the first being the outermost.
func WithReceiveInterceptors(i ...ReceiveInterceptor) Option {
	return func(c *Client) { c.receiveInterceptors = append(c.receiveInterceptors, i...) }
}

func chain[F any, I ~func(F) F](f F, interceptors []I) F {
	for i := len(interceptors) - 1; i >= 0; i-- {
		f = interceptors[i](f)
	}
	return f
}
