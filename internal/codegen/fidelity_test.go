package codegen

import (
	"slices"
	"strings"
	"testing"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

const head = `
asyncapi: 3.1.0
info: {title: t, version: "1"}
`

// payloadDoc is a document with one send operation carrying the given payload.
func payloadDoc(payload string) string {
	return head + `
channels:
  c: {address: a, messages: {m: {payload: ` + payload + `}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
`
}

// compile generates src, checks it compiles, and returns the code and warnings.
func compile(t *testing.T, src string) (string, []string) {
	t.Helper()
	doc, err := spec.Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	var warnings []string
	got, err := Generate(doc, Options{Package: "p", Warn: func(w string) { warnings = append(warnings, w) }})
	if err != nil {
		t.Fatal(err)
	}
	typecheck(t, string(got))
	return string(got), warnings
}

func contains(t *testing.T, out string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(out, want) {
			t.Errorf("generated code does not contain %q:\n%s", want, out)
		}
	}
}

func warned(t *testing.T, warnings []string, want string) {
	t.Helper()
	if !slices.ContainsFunc(warnings, func(w string) bool { return strings.Contains(w, want) }) {
		t.Errorf("warnings %q do not mention %q", warnings, want)
	}
}

func TestSameMessageKeyOnTwoChannelsKeepsBothPayloads(t *testing.T) {
	out, _ := compile(t, head+`
channels:
  a: {address: a, messages: {created: {payload: {type: object, required: [x], properties: {x: {type: string}}}}}}
  b: {address: b, messages: {created: {payload: {type: object, required: [y], properties: {y: {type: integer}}}}}}
operations:
  pubA: {action: send, channel: {$ref: '#/channels/a'}}
  pubB: {action: send, channel: {$ref: '#/channels/b'}}
`)
	contains(t, out, "broker.Channel[Created]", "broker.Channel[BCreated]", "X string", "Y int64")
}

func TestSameMessageNameOnTwoComponents(t *testing.T) {
	out, _ := compile(t, head+`
channels:
  a: {address: a, messages: {m: {$ref: '#/components/messages/A'}}}
  b: {address: b, messages: {m: {$ref: '#/components/messages/B'}}}
operations:
  pubA: {action: send, channel: {$ref: '#/channels/a'}}
  pubB: {action: send, channel: {$ref: '#/channels/b'}}
components:
  messages:
    A: {name: same, payload: {type: string}}
    B: {name: same, payload: {type: integer}}
`)
	contains(t, out, "type Same = string", "type B = int64")
}

func TestSharedComponentMessageIsOneType(t *testing.T) {
	out, _ := compile(t, head+`
channels:
  a: {address: a, messages: {m: {$ref: '#/components/messages/Event'}}}
  b: {address: b, messages: {m: {$ref: '#/components/messages/Event'}}}
operations:
  pubA: {action: send, channel: {$ref: '#/channels/a'}}
  pubB: {action: send, channel: {$ref: '#/channels/b'}}
components:
  messages:
    Event: {payload: {type: string}}
`)
	contains(t, out, "AChannel = broker.Channel[Event]", "BChannel = broker.Channel[Event]")
	if strings.Contains(out, "Event2") || strings.Contains(out, "BEvent") {
		t.Errorf("shared message was duplicated:\n%s", out)
	}
}

func TestMultiFormatSchemas(t *testing.T) {
	out, _ := compile(t, head+`
channels:
  a:
    address: a
    messages:
      inline:
        headers:
          schemaFormat: application/schema+json;version=draft-07
          schema: {type: object, properties: {trace: {type: string}}}
        payload:
          schemaFormat: application/vnd.aai.asyncapi+json;version=3.1.0
          schema: {type: object, required: [id], properties: {id: {type: string}}}
  b:
    address: b
    messages:
      byRef:
        payload:
          schemaFormat: application/schema+yaml;version=draft-07
          schema: {$ref: '#/components/schemas/Named'}
  c:
    address: c
    messages:
      wrapped:
        payload: {$ref: '#/components/schemas/Wrapped'}
operations:
  pubA: {action: send, channel: {$ref: '#/channels/a'}}
  pubB: {action: send, channel: {$ref: '#/channels/b'}}
  pubC: {action: send, channel: {$ref: '#/channels/c'}}
components:
  schemas:
    Named: {type: object, required: [n], properties: {n: {type: string}}}
    Wrapped:
      schemaFormat: application/schema+json;version=draft-07
      schema: {type: object, required: [w], properties: {w: {type: string}}}
`)
	contains(t, out,
		"type Inline struct", "ID string", `InlineHeaderTrace = "trace"`,
		"broker.Channel[Named]",
		"type Wrapped struct", "W string",
	)
}

func TestUnsupportedSchemaFormatIsAnError(t *testing.T) {
	_, err := generate(t, payloadDoc(`{schemaFormat: application/vnd.apache.avro+json;version=1.9.0, schema: {type: record}}`))
	if err == nil || !strings.Contains(err.Error(), "unsupported schemaFormat") {
		t.Fatalf("got error %v, want it to contain %q", err, "unsupported schemaFormat")
	}
}

func TestReplyAddressesAreRejected(t *testing.T) {
	tests := map[string]struct{ reply, wantErr string }{
		"fixed channel address": {`{channel: {$ref: '#/channels/fixed'}}`, "fixed address"},
		"location":              {`{address: {location: '$message.header#/replyTo'}, channel: {$ref: '#/channels/dynamic'}}`, "reply.address"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := generate(t, head+`
channels:
  req: {address: req, messages: {q: {payload: {type: string}}}}
  fixed: {address: fixed.reply, messages: {r: {payload: {type: string}}}}
  dynamic: {address: null, messages: {r: {payload: {type: string}}}}
operations:
  ask: {action: send, channel: {$ref: '#/channels/req'}, reply: `+tc.reply+`}
`)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got error %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestTraitsReachGeneratedCode(t *testing.T) {
	out, _ := compile(t, head+`
channels:
  c:
    address: a
    messages:
      m:
        traits: [{$ref: '#/components/messageTraits/traced'}]
        payload: {type: string}
operations:
  on:
    action: receive
    channel: {$ref: '#/channels/c'}
    traits: [{bindings: {nats: {queue: workers}}}]
components:
  messageTraits:
    traced: {headers: {type: object, properties: {trace: {type: string}}}}
`)
	contains(t, out, `MHeaderTrace = "trace"`, `broker.Queue("workers")`)
}

func TestAllOf(t *testing.T) {
	out, warnings := compile(t, head+`
channels:
  c:
    address: a
    messages:
      m:
        payload: {$ref: '#/components/schemas/Dog'}
  i:
    address: i
    messages:
      inline:
        payload:
          allOf:
            - {$ref: '#/components/schemas/Pet'}
            - {type: object, required: [tag], properties: {tag: {type: string}}}
          properties: {own: {type: integer}}
  r:
    address: r
    messages:
      only:
        payload: {allOf: [{$ref: '#/components/schemas/Pet'}]}
operations:
  pubC: {action: send, channel: {$ref: '#/channels/c'}}
  pubI: {action: send, channel: {$ref: '#/channels/i'}}
  pubR: {action: send, channel: {$ref: '#/channels/r'}}
components:
  schemas:
    Pet: {type: object, required: [name], properties: {name: {type: string}}}
    Dog:
      description: A dog.
      allOf:
        - {$ref: '#/components/schemas/Pet'}
        - {type: object, properties: {bark: {type: boolean}}}
`)
	contains(t, out,
		"// A dog.\ntype Dog struct", "Bark *bool", "Name string",
		"type Inline struct", "Own  *int64", "Tag  string",
		"RChannel = broker.Channel[Pet]",
	)
	if strings.Contains(out, "RawMessage") {
		t.Errorf("allOf should not fall back to json.RawMessage:\n%s", out)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %q", warnings)
	}
}

func TestAllOfConflictIsAnError(t *testing.T) {
	tests := map[string]string{
		"property": `{allOf: [{properties: {a: {type: string}}}, {properties: {a: {type: integer}}}]}`,
		"type":     `{allOf: [{type: string}, {type: integer}]}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := generate(t, payloadDoc(payload))
			if err == nil || !strings.Contains(err.Error(), "allOf") {
				t.Fatalf("got error %v, want it to contain %q", err, "allOf")
			}
		})
	}
}

func TestOneOfAnyOfWarn(t *testing.T) {
	for _, kw := range []string{"oneOf", "anyOf"} {
		t.Run(kw, func(t *testing.T) {
			out, warnings := compile(t, payloadDoc(`{`+kw+`: [{type: string}, {type: integer}]}`))
			contains(t, out, "json.RawMessage")
			warned(t, warnings, kw)
		})
	}
}

func TestEnums(t *testing.T) {
	out, warnings := compile(t, payloadDoc(`{type: object, properties: {
		level: {type: integer, enum: [1, 2, -2]},
		small: {type: integer, format: int32, enum: [7]},
		ratio: {type: number, enum: [0.5]},
		color: {enum: [red]},
		flag: {type: boolean, enum: [true]}
	}}`))
	contains(t, out,
		"type MLevel int64", "MLevelX1  MLevel = 1", "MLevelX2  MLevel = 2", "MLevelX22 MLevel = -2",
		"type MSmall int32", "MSmallX7 MSmall = 7",
		"type MRatio float64", "MRatio = 0.5",
		"type MColor string", `MColorRed MColor = "red"`,
		"Flag  *bool",
	)
	warned(t, warnings, "enum")
}

func TestConstIsTypedAndDocumented(t *testing.T) {
	out, _ := compile(t, payloadDoc(`{type: object, required: [kind, n], properties: {kind: {const: created}, n: {const: 3}}}`))
	contains(t, out, `// Must be "created".`, "Kind string", "// Must be 3.", "N int64")
}

func TestIgnoredKeywordsWarn(t *testing.T) {
	tests := map[string]struct{ payload, want string }{
		"not":                  {`{type: object, properties: {k: {not: {type: integer}}}}`, "not"},
		"if":                   {`{type: object, properties: {k: {if: {type: integer}}}}`, "if"},
		"root":                 {`{type: object, not: {required: [a]}, properties: {a: {type: string}}}`, "not"},
		"additionalProperties": {`{type: object, properties: {a: {type: string}}, additionalProperties: {type: integer}}`, "additionalProperties"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, warnings := compile(t, payloadDoc(tc.payload))
			warned(t, warnings, tc.want)
		})
	}
}

func TestSupportedDocumentsDoNotWarn(t *testing.T) {
	for _, payload := range []string{
		`{type: object, properties: {a: {type: string}}, additionalProperties: true}`,
		`{type: object, properties: {a: {type: string}}, additionalProperties: false}`,
		`{type: object, additionalProperties: {type: integer}}`,
	} {
		if _, warnings := compile(t, payloadDoc(payload)); len(warnings) != 0 {
			t.Errorf("%s: unexpected warnings: %q", payload, warnings)
		}
	}
}

func TestContentType(t *testing.T) {
	doc := func(contentType string) string {
		return head + `
channels:
  c: {address: a, messages: {m: {contentType: ` + contentType + `, payload: {type: string}}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
`
	}
	for _, json := range []string{"application/json", "application/vnd.x+json"} {
		if _, warnings := compile(t, doc(json)); len(warnings) != 0 {
			t.Errorf("%s: unexpected warnings: %q", json, warnings)
		}
	}
	_, warnings := compile(t, doc("application/avro"))
	warned(t, warnings, "WithCodec")
}

func TestServersMustIncludeNATS(t *testing.T) {
	doc := func(servers string) string {
		return head + "servers: " + servers + `
channels:
  c: {address: a, messages: {m: {payload: {type: string}}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
components:
  servers:
    shared: {host: 'h:1', protocol: mqtt}
`
	}
	const kafka = `{prod: {host: 'h:1', protocol: kafka}}`
	for _, src := range []string{
		doc(kafka),
		doc(`{prod: {$ref: '#/components/servers/shared'}}`),
	} {
		_, err := generate(t, src)
		if err == nil || !strings.Contains(err.Error(), "no NATS server") {
			t.Errorf("got error %v, want it to contain %q", err, "no NATS server")
		}
	}
	compile(t, doc(`{prod: {host: 'h:1', protocol: nats}, other: {host: 'h:1', protocol: kafka}}`))
	compile(t, doc(`{}`))

	// The models of a non-NATS document are still usable.
	parsed, err := spec.Parse([]byte(doc(kafka)))
	if err != nil {
		t.Fatal(err)
	}
	got, err := Generate(parsed, Options{Package: "p", Parts: []Part{PartModels}})
	if err != nil {
		t.Fatal(err)
	}
	typecheck(t, string(got))
}

func TestParameterDefaultAndLocation(t *testing.T) {
	out, warnings := compile(t, head+`
channels:
  c:
    address: 'a.{region}.{id}'
    parameters:
      region: {default: eu}
      id: {location: '$message.payload#/id'}
    messages: {m: {payload: {type: string}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
`)
	contains(t, out, `Defaults: map[string]string{`, `"region": "eu"`)
	warned(t, warnings, "location")
}

func TestValidSpecsCompile(t *testing.T) {
	tests := map[string]string{
		"required self reference": head + `
channels:
  c: {address: a, messages: {m: {payload: {$ref: '#/components/schemas/Node'}}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
components:
  schemas:
    Node: {type: object, required: [next], properties: {next: {$ref: '#/components/schemas/Node'}}}
`,
		"required mutual reference": head + `
components:
  schemas:
    A: {type: object, required: [b], properties: {b: {$ref: '#/components/schemas/B'}}}
    B: {type: object, required: [a], properties: {a: {$ref: '#/components/schemas/A'}}}
`,
		"field names collide": payloadDoc(`{type: object, properties: {foo_bar: {type: string}, fooBar: {type: string}}}`),
		"enum names collide":  payloadDoc(`{type: object, properties: {k: {type: string, enum: [a-b, a_b]}}}`),
		"parameter args collide": head + `
channels:
  c:
    address: 'a.{user-id}.{user_id}'
    parameters: {user-id: {}, user_id: {}}
    messages: {m: {payload: {type: string}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
`,
		"operation named client": head + `
channels:
  c: {address: a, messages: {m: {payload: {type: string}}}}
operations:
  client: {action: send, channel: {$ref: '#/channels/c'}}
`,
	}
	for name, src := range tests {
		t.Run(name, func(t *testing.T) { compile(t, src) })
	}
}

func TestCollidingFieldsKeepTheirJSONNames(t *testing.T) {
	out, _ := compile(t, payloadDoc(`{type: object, properties: {fooBar: {type: string}, foo_bar: {type: string}}}`))
	contains(t, out, `json:"fooBar,omitempty"`, `json:"foo_bar,omitempty"`, "FooBar2")
}

func TestRecursiveFieldBecomesPointer(t *testing.T) {
	out, _ := compile(t, head+`
components:
  schemas:
    Node: {type: object, required: [next], properties: {next: {$ref: '#/components/schemas/Node'}}}
`)
	contains(t, out, "Next *Node", `json:"next"`)
}

func TestNameErrors(t *testing.T) {
	tests := map[string]struct{ src, wantErr string }{
		"backtick property": {payloadDoc("{type: object, properties: {\"a`b\": {type: string}}}"), "property"},
		"quote property":    {payloadDoc(`{type: object, properties: {'a"b': {type: string}}}`), "property"},
		"comma property":    {payloadDoc(`{type: object, properties: {'a,b': {type: string}}}`), "property"},
		"dash property":     {payloadDoc(`{type: object, properties: {'-': {type: string}}}`), "property"},
		"operations collide": {head + `
channels:
  c: {address: a, messages: {m: {payload: {type: string}}}}
operations:
  foo-bar: {action: send, channel: {$ref: '#/channels/c'}}
  foo_bar: {action: send, channel: {$ref: '#/channels/c'}}
`, "same Go name"},
		"channels collide": {head + `
channels:
  a-b: {address: a, messages: {m: {payload: {type: string}}}}
  a_b: {address: b, messages: {n: {payload: {type: integer}}}}
operations:
  one: {action: send, channel: {$ref: '#/channels/a-b'}}
  two: {action: send, channel: {$ref: '#/channels/a_b'}}
`, "ABChannel"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := generate(t, tc.src)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got error %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
