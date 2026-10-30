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

The generated package depends only on `github.com/nats-io/nats.go`.

```go
nc, _ := nats.Connect(nats.DefaultURL)
c := orders.NewClient(nc, orders.WithErrorHandler(log.Println))

// receive operation with a NATS `queue` binding → QueueSubscribe on orders.*.created
c.OnOrderCreated(func(ctx context.Context, p orders.OrderCreatedParams, o orders.Order, h orders.OrderCreatedHeaders) error {
	log.Printf("order %s in %s", o.ID, p.Region)
	return nil
})

// send operation → Publish on orders.eu.created
c.PublishOrderCreated(ctx, orders.OrderCreatedParams{Region: "eu"}, order, orders.OrderCreatedHeaders{TraceID: "t-1"})

// send operation with a reply → request/reply
status, err := c.GetOrderStatus(ctx, orders.StatusRequest{OrderID: "o-1"})
```

See [examples/orders](examples/orders) for the full spec and generated code.

## What is generated

| AsyncAPI | Go |
|---|---|
| `components.schemas`, message payloads | structs, enums (typed string constants), aliases |
| channel `address` with `{params}` | `XxxParams` with `Subject()`, `ParseXxxSubject()`, `XxxSubjectPattern` (`*` wildcards) |
| `send` operation | publish method |
| `send` + `reply` | request method returning the typed reply |
| `receive` operation | subscribe method (uses `QueueSubscribe` when `bindings.nats.queue` is set) |
| `receive` + `reply` | responder: the handler's return value is sent to the reply inbox |
| message `headers` | typed headers struct carried in NATS headers |

Responder errors are sent back using the `Nats-Service-Error` / `Nats-Service-Error-Code`
headers and surface as `*RemoteError` on the requesting side.

## Status

Early MVP. See [docs/PLAN.md](docs/PLAN.md) for the roadmap and current limitations
(one message per operation, no `oneOf`/`allOf`, local `$ref`s only, no JetStream yet).

## Development

```sh
go test ./...                        # unit, golden and integration tests (embedded nats-server)
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
