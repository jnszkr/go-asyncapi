# go-asyncapi: AsyncAPI → Go code generator for NATS

## Goal

A CLI (`asyncapi-gen`) that reads an [AsyncAPI](https://www.asyncapi.com/) document
and generates a typed Go package for talking to a [NATS](https://nats.io) server,
honouring the [NATS bindings](https://github.com/asyncapi/bindings/blob/master/nats/README.md).

```
asyncapi-gen -spec asyncapi.yaml -out ./gen/events -package events
```

## NATS binding (v0.1.0) recap

| Object    | Fields                                   | Generator behaviour                              |
|-----------|------------------------------------------|--------------------------------------------------|
| server    | none (reserved)                          | accepted, ignored                                |
| channel   | none (reserved)                          | accepted, ignored                                |
| operation | `queue` (string, ≤255 chars), `bindingVersion` | `receive` ops use `QueueSubscribe` with that queue |
| message   | none (reserved)                          | accepted, ignored                                |

## Design: generic runtime, thin generated code

All messaging logic lives in the importable `broker` package and is written once with
generics: `Channel[T]`, `Publish[T]`, `Subscribe[T]`, `Request[Req, Resp]`,
`Respond[Req, Resp]`. Transports implement the small `broker.Broker` interface
(`natsbroker` for NATS, `membroker` in memory for tests).

The generator only emits what is specific to a document:

| AsyncAPI                         | Generated Go                                                     |
|----------------------------------|------------------------------------------------------------------|
| `components.schemas.*`, message payloads | structs / named types (JSON tags, `omitempty` for optional fields) |
| channel `address` + `parameters` | `XxxChannel = broker.Channel[T]{Address, Allowed}` and `XxxParams(...) broker.Params` |
| `send` / `receive` operations    | one-line `Client` methods calling `Publish` / `Subscribe` (`Queue` from the NATS binding) |
| `send` / `receive` + `reply`     | one-line `Client` methods calling `Request` / `Respond`          |
| message `headers`                | header-name constants                                            |
| `contentType`                    | JSON codec by default; `broker.WithCodec` to replace it          |

## Architecture

```
cmd/asyncapi-gen      CLI entrypoint (flags, IO)
broker/               generic runtime used by generated code
internal/spec         AsyncAPI document model, YAML/JSON loader, local $ref resolution, validation
internal/codegen      spec → intermediate model → text/template → gofmt
examples/             sample specs + checked-in generated output (also golden files)
```

## Phases

1. **MVP (this branch)**
   - AsyncAPI 3.0.x, YAML and JSON input, local `#/...` `$ref`s
   - JSON Schema subset: object, array, string (+`date-time` → `time.Time`), integer, number, boolean, enum, `additionalProperties`, `$ref`, nested inline objects
   - Channel parameters with wildcard subscription + subject parsing
   - Publish / subscribe / queue-subscribe / request-reply
   - Golden-file tests and an integration test against an embedded `nats-server`
2. **Breadth**
   - Multiple message types per channel / operation (discriminated decoding)
   - Typed header structs on top of `broker.Header`
   - `oneOf` / `anyOf` / `allOf` schemas
   - AsyncAPI 2.6 input (publish/subscribe semantics inverted vs 3.0)
   - External file `$ref`s
3. **JetStream** (outside the binding spec)
   - Done: `broker/jsbroker` publishes with stream acks and consumes from pull consumers
     the application creates and binds to generated `broker.Endpoint`s; handler errors
     nak, malformed messages are terminated
   - Open: push consumers, per-message ack control (in progress, delay), generating
     stream/consumer hints from `x-` extensions
4. **Polish**
   - Docs site, more examples (versioned releases with binaries are automated from `main`)

## Non-goals / constraints

- The repository is public: examples use generic domains (users, orders), no real
  hostnames, credentials or internal identifiers. Server URLs in examples point to
  `localhost` / `demo.nats.io`-style placeholders.
