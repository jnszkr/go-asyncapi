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

## Design: self-contained generated code, pluggable transports

The generated package depends only on the standard library, so applications need this
module at build time only. Besides models, it declares the transport interfaces it needs
(`Publisher`, `Subscriber`, `Requester`, `Replier`) over plain subjects, headers
(`map[string][]string`) and bodies (`[]byte`), with `Inbox`, `Subscription` and
`Settlement` as aliases of unnamed types: any transport, in any package, satisfies them
without importing the generated code. The `broker` packages provide such transports
(`natsbroker` for NATS, `jsbroker` for JetStream, `membroker` in memory for tests).

| AsyncAPI                         | Generated Go                                                     |
|----------------------------------|------------------------------------------------------------------|
| `components.schemas.*`, message payloads | structs / named types (JSON tags, `omitempty` for optional fields) |
| channel `address` + `parameters` | `XxxChannel = Channel[T]{Address, Allowed, Defaults}` and `XxxParams(...) Params` |
| `send` / `receive` operations    | `Producer` / `Consumer` methods on `Publisher` / `Subscriber` (`queue` from the NATS binding) |
| `send` / `receive` + `reply`     | `Client` / `Server` methods on `Requester` / `Replier`           |
| message `headers`                | header-name constants                                            |
| `contentType`                    | always JSON (`encoding/json`)                                    |
| schema validation keywords       | a `Validate` method per struct and enum type, called on every payload sent and received |

Validation is generated too, as Go code checking each schema's constraints on the value of
its type: struct methods call those of their fields, and an alias type with constraints gets
an unexported function, since it cannot have methods. It is always on, with no options: a
received payload that does not validate is malformed (`Term`, or 400 to a requester), and one
about to be sent is an error (500 for a `Server` reply). What the Go types cannot tell apart,
such as an absent required string from an empty one, or unknown properties, is not checked.

## Architecture

```
cmd/asyncapi-gen      CLI entrypoint (flags, IO)
broker/               transport contract and transports (NATS, JetStream, in memory)
internal/spec         AsyncAPI document model, YAML/JSON loader, local $ref resolution, validation
internal/codegen      spec → intermediate model → text/template → gofmt
examples/             sample specs + checked-in generated output (also golden files)
```

## Phases

1. **Core** (done)
   - AsyncAPI 3.0.x and 3.1.x, YAML and JSON input, local `#/...` `$ref`s
   - JSON Schema subset: object, array, string (+`date-time` → `time.Time`), integer, number, boolean, enum, `additionalProperties`, `allOf`, `$ref`, nested inline objects
   - Channel parameters with wildcard subscription + subject parsing
   - Publish / subscribe / queue-subscribe / request-reply
   - Golden-file tests and an integration test against an embedded `nats-server`
2. **Using it in applications** (done)
   - Choosing the generated parts with `-generate`, and existing Go types with `x-go-type`
   - Failing on documents the generated code could not match
   - Application-supplied transports
   - Generated code that depends only on the standard library
   - Payload validation against the JSON Schemas, by generated `Validate` methods
3. **JetStream** (outside the binding spec)
   - Done: `broker/jsbroker` publishes with stream acks and consumes from pull consumers
     the application creates and configures, given to the Broker with `jsbroker.WithConsumers`;
     the handler's Settlement acks, naks or terminates each message, and interceptors can
     settle each message themselves
   - Open: push consumers, generating stream/consumer hints from `x-` extensions
4. **Breadth**
   - Multiple message types per channel / operation (discriminated decoding)
   - Typed header structs
   - Validation of what the Go types lose: absent required scalars and unknown properties
     (a generated `UnmarshalJSON`), and the keywords not enforced yet (`not`, `if`, `oneOf`
     and `anyOf` once they are typed, `patternProperties`, `contains`, more formats)
   - Typed `oneOf` / `anyOf` schemas
   - Nullable `type: [string, "null"]`
   - Reply addresses (an error today: replies go to the reply subject NATS provides)
   - AsyncAPI 2.6 input (publish/subscribe semantics inverted vs 3.0)
   - External file `$ref`s
5. **Polish**
   - Docs site, more examples (versioned releases with binaries are automated from `main`)

## Before v1.0.0

- Multiple message types per channel: documents using them fail to generate today
- Generating from a range of real-world AsyncAPI documents, not only `examples/orders`
- Use in a real service, and a stretch without breaking changes

## Non-goals / constraints

- The repository is public: examples use generic domains (users, orders), no real
  hostnames, credentials or internal identifiers. Server URLs in examples point to
  `localhost` / `demo.nats.io`-style placeholders.
