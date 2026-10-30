# go-asyncapi

Generate typed Go clients for [NATS](https://nats.io) from
[AsyncAPI 3.0](https://www.asyncapi.com/docs/reference/specification/v3.0.0) documents,
including the [NATS bindings](https://github.com/asyncapi/bindings/blob/master/nats/README.md).

## Install

```sh
go install github.com/jnszkr/go-asyncapi/cmd/asyncapi-gen@latest
```

## Usage

```sh
asyncapi-gen -spec asyncapi.yaml -out ./gen/orders [-package orders]
```

or from a `go:generate` directive:

```go
//go:generate go run github.com/jnszkr/go-asyncapi/cmd/asyncapi-gen -spec asyncapi.yaml -out gen/orders
```

## How it fits together

```
broker/              generic, transport-agnostic runtime
  Channel[T]         address + allowed parameter values for payload type T
  Publish[T]         Subscribe[T]   Request[Req, Resp]   Respond[Req, Resp]
  Broker             interface: Publish / Request / Subscribe
  Endpoint           subject + queue a receive operation subscribes with
broker/natsbroker    Broker on a *nats.Conn (core NATS)
broker/jsbroker      Broker on JetStream, consuming from consumers you create
broker/membroker     in-memory Broker for tests
```

The generated package contains only plain models, channel definitions, header-name
constants, parameter helpers and a thin `Client` whose methods each call one generic
function:

```go
var OrderCreatedChannel = broker.Channel[Order]{
	Address: "orders.{region}.created",
	Allowed: map[string][]string{"region": {"eu", "us"}},
}

func (c *Client) PublishOrderCreated(ctx context.Context, params broker.Params, msg Order, opts ...broker.MsgOption) error {
	return broker.Publish(ctx, c.Client, OrderCreatedChannel, params, msg, opts...)
}
```

Using it:

```go
nc, _ := nats.Connect(nats.DefaultURL)
c := orders.NewClient(natsbroker.New(nc), broker.WithErrorHandler(log.Println))

// receive operation with a NATS `queue` binding → queue subscription on orders.*.created
c.OnOrderCreated(func(ctx context.Context, m broker.Received[orders.Order]) error {
	log.Printf("order %s in %s", m.Payload.ID, m.Params["region"])
	return nil
})

// send operation → publish on orders.eu.created
c.PublishOrderCreated(ctx, orders.OrderCreatedParams("eu"), order,
	broker.WithHeader(orders.OrderCreatedHeaderTraceID, "t-1"))

// send operation with a reply → request/reply
status, err := c.GetOrderStatus(ctx, orders.StatusRequest{OrderID: "o-1"})

// or skip the wrappers and use the generic functions directly
broker.Publish(ctx, c.Client, orders.OrderCreatedChannel, orders.OrderCreatedParams("us"), order)
```

See [examples/orders](examples/orders) for the full spec and generated code.

### JetStream

The same generated client runs on JetStream through `jsbroker`. Streams and consumers
are not created for you: their configuration (retention, replicas, ack wait, delivery
policy, ...) is the application's. Each receive operation gets a generated
`XxxEndpoint` (`broker.Endpoint{Subject, Queue}`) that you bind a consumer to;
`jsbroker.ConsumerConfig` turns an endpoint into a starting configuration (filter
subject, explicit acks, durable named after the queue) that you can adjust:

```go
js, _ := jetstream.New(nc)
cons, _ := js.CreateOrUpdateConsumer(ctx, "ORDERS", jsbroker.ConsumerConfig(orders.OnOrderCreatedEndpoint))

b := jsbroker.New(js)
b.Bind(orders.OnOrderCreatedEndpoint, cons) // or bind a consumer you looked up with js.Consumer
c := orders.NewClient(b)

c.OnOrderCreated(handle) // consumes from cons
c.PublishOrderCreated(ctx, orders.OrderCreatedParams("eu"), order) // waits for the stream's ack
```

A handler returning `nil` acks the message, an error naks it (redelivery), and a
message that cannot be decoded is terminated. Subscribing to an unbound endpoint fails
with `jsbroker.ErrNotBound`. Request/reply is not stored: `Request` goes over core NATS,
and responders should use a `natsbroker` client.

## Mapping

| AsyncAPI | Go |
|---|---|
| `components.schemas`, message payloads | structs, enums (typed string constants), aliases |
| channel `address` with `{params}` | `XxxChannel` (`broker.Channel[T]`) and an `XxxParams(...)` helper; parameter `enum`s become `Allowed` |
| `send` operation | method calling `broker.Publish` |
| `send` + `reply` | method calling `broker.Request` |
| `receive` operation | method calling `broker.Subscribe`, with `broker.Queue` when `bindings.nats.queue` is set |
| `receive` + `reply` | method calling `broker.Respond` |
| message `headers` | header-name constants; values are passed with `broker.WithHeader` |

Responder errors are sent back using the `Nats-Service-Error` / `Nats-Service-Error-Code`
headers and surface as `*broker.RemoteError` on the requesting side.

## Status

Early MVP. See [docs/PLAN.md](docs/PLAN.md) for the roadmap and current limitations
(one message type per channel, no `oneOf`/`allOf`, local `$ref`s only).

## Development

```sh
go test ./...                        # broker, golden and integration tests (embedded nats-server)
go test ./internal/codegen -update   # refresh golden files after template changes
```

## Contributing

Commits and PR titles follow [Conventional Commits](https://www.conventionalcommits.org/)
(`feat: …`, `fix: …`, `docs: …`, `ci: …`, `feat!: …` for breaking changes). PRs are
squash-merged, so the PR title becomes the commit on `main`; CI checks it.

## Releases

Every push to `main` is versioned automatically from those commits
([go-semantic-release](https://github.com/go-semantic-release/semantic-release)):
`fix` → patch, `feat` → minor, breaking → major (minor while below `v1.0.0`). The workflow
tags the commit, publishes a GitHub release with a changelog, and attaches `asyncapi-gen`
binaries built with [GoReleaser](https://goreleaser.com).
