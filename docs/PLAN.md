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

## Mapping AsyncAPI 3.0 → Go

| AsyncAPI                         | Generated Go                                                     |
|----------------------------------|------------------------------------------------------------------|
| `components.schemas.*`, message payloads | structs / named types (JSON tags, `omitempty` for optional fields) |
| channel `address` (`user.{id}.signup`) | `XxxSubject(params)` builder, `XxxSubjectPattern` (`user.*.signup`) and a parser that extracts params from an incoming subject |
| channel `parameters`             | `XxxParams` struct                                               |
| `send` operation                 | `(*Client).Xxx(ctx, params, msg) error` → `nats.Conn.PublishMsg` |
| `receive` operation              | `(*Client).Xxx(handler) (*nats.Subscription, error)` → `Subscribe` / `QueueSubscribe` |
| `send` + `reply`                 | request: `(*Client).Xxx(ctx, params, req) (Resp, error)` → `RequestMsgWithContext` |
| `receive` + `reply`              | responder: handler returns a reply which is sent to `msg.Reply`  |
| message `headers` (object schema)| typed headers struct mapped to `nats.Header`                     |
| message `contentType` / `defaultContentType` | JSON codec by default; pluggable `Codec` option          |

The generated package depends only on `github.com/nats-io/nats.go` and the stdlib.

## Architecture

```
cmd/asyncapi-gen      CLI entrypoint (flags, IO)
internal/spec         AsyncAPI document model, YAML/JSON loader, local $ref resolution, validation
internal/codegen      spec → intermediate model (names, Go types) → text/template → gofmt
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
   - Multiple messages per operation (discriminated decoding)
   - `oneOf` / `anyOf` / `allOf` schemas
   - AsyncAPI 2.6 input (publish/subscribe semantics inverted vs 3.0)
   - External file `$ref`s
3. **JetStream** (outside the binding spec, opt-in via `x-` extensions)
   - Stream/consumer config, `js.Publish`, pull/push consumers, acks
4. **Polish**
   - Docs site, more examples (versioned releases with binaries are automated from `main`)

## Non-goals / constraints

- The repository is public: examples use generic domains (users, orders), no real
  hostnames, credentials or internal identifiers. Server URLs in examples point to
  `localhost` / `demo.nats.io`-style placeholders.
