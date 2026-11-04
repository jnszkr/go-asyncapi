package spec

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const validHead = `
asyncapi: 3.1.0
info: {title: t, version: "1"}
`

func TestParseAcceptsValidDocuments(t *testing.T) {
	tests := map[string]string{
		"3.0":          "asyncapi: 3.0.1\ninfo: {title: t, version: '1'}\n",
		"3.1":          validHead,
		"extensions":   validHead + "x-owner: me\nchannels:\n  c: {address: a, x-team: t}\n",
		"parameters":   validHead + "channels:\n  c: {address: 'a.{id}', parameters: {id: {}}}\n",
		"null address": validHead + "channels:\n  c: {address: null}\n",
		"message by component": validHead + `
channels:
  c:
    address: a
    messages: {m: {$ref: '#/components/messages/M'}}
operations:
  o:
    action: send
    channel: {$ref: '#/channels/c'}
    messages: [{$ref: '#/components/messages/M'}]
components:
  messages:
    M: {payload: {type: string}}
`,
	}
	for name, src := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(src)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestParseRejectsInvalidDocuments(t *testing.T) {
	tests := map[string]struct{ src, wantErr string }{
		"future version":     {"asyncapi: 3.99.0\ninfo: {title: t, version: '1'}\n", "only 3.0 and 3.1"},
		"no title":           {"asyncapi: 3.1.0\ninfo: {version: '1'}\n", "info.title"},
		"no version":         {"asyncapi: 3.1.0\ninfo: {title: t}\n", "info.version"},
		"unknown root field": {validHead + "channel: {}\n", `unknown field "channel"`},
		"unknown operation field": {validHead + `
channels:
  c: {address: a, messages: {m: {payload: {type: string}}}}
operations:
  o: {action: send, channel: {$ref: '#/channels/c'}, bogus: 1}
`, `unknown field "bogus" in /operations/o`},
		"unknown channel field":    {validHead + "channels:\n  c: {addr: a}\n", `unknown field "addr" in /channels/c`},
		"unknown message field":    {validHead + "channels:\n  c: {address: a, messages: {m: {paylod: {}}}}\n", `unknown field "paylod" in /channels/c/messages/m`},
		"payload in trait":         {validHead + "components:\n  messageTraits:\n    t: {payload: {}}\n", `unknown field "payload" in /components/messageTraits/t`},
		"undeclared parameter":     {validHead + "channels:\n  c: {address: 'a.{id}'}\n", `parameter "id"`},
		"parameter not in address": {validHead + "channels:\n  c: {address: a, parameters: {id: {}}}\n", `parameter "id"`},
		"operation message on another channel": {validHead + `
channels:
  a: {address: a, messages: {x: {payload: {type: string}}}}
  b: {address: b, messages: {y: {payload: {type: integer}}}}
operations:
  o: {action: send, channel: {$ref: '#/channels/a'}, messages: [{$ref: '#/channels/b/messages/y'}]}
`, "is not a message of its channel"},
		"reply message on another channel": {validHead + `
channels:
  a: {address: a, messages: {x: {payload: {type: string}}}}
  r: {address: null, messages: {y: {payload: {type: string}}}}
operations:
  o:
    action: send
    channel: {$ref: '#/channels/a'}
    reply: {channel: {$ref: '#/channels/r'}, messages: [{$ref: '#/channels/a/messages/x'}]}
`, "is not a message of its channel"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.src))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got error %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestMessageTraits(t *testing.T) {
	doc, err := Parse([]byte(validHead + `
channels:
  c:
    address: a
    messages:
      m:
        description: from the message
        contentType: application/json
        headers: {properties: {own: {type: string}}}
        traits:
          - $ref: '#/components/messageTraits/first'
          - {summary: second, description: from a trait}
components:
  messageTraits:
    first:
      summary: first
      title: from the first trait
      contentType: application/avro
      headers: {type: object, properties: {trace: {type: string}}}
`))
	if err != nil {
		t.Fatal(err)
	}
	m, _, err := doc.MessageByRef("#/channels/c/messages/m")
	if err != nil {
		t.Fatal(err)
	}
	if m.Description != "from the message" || m.ContentType != "application/json" {
		t.Errorf("a trait overrode the message: %q, %q", m.Description, m.ContentType)
	}
	if m.Summary != "second" {
		t.Errorf("summary = %q, want the later trait's", m.Summary)
	}
	if m.Title != "from the first trait" {
		t.Errorf("title = %q, want it taken from the trait", m.Title)
	}
	if m.Headers == nil || m.Headers.Type != "object" || m.Headers.Properties["trace"] == nil || m.Headers.Properties["own"] == nil {
		t.Errorf("headers were not merged: %+v", m.Headers)
	}
}

func TestOperationTraits(t *testing.T) {
	doc, err := Parse([]byte(validHead + `
channels:
  c: {address: a, messages: {m: {payload: {type: string}}}}
operations:
  o:
    action: receive
    channel: {$ref: '#/channels/c'}
    traits: [{$ref: '#/components/operationTraits/q'}]
components:
  operationTraits:
    q: {bindings: {nats: {queue: workers}}}
`))
	if err != nil {
		t.Fatal(err)
	}
	op, _, err := doc.ResolveOperation(doc.Operations["o"])
	if err != nil {
		t.Fatal(err)
	}
	if op.Bindings == nil || op.Bindings.NATS == nil || op.Bindings.NATS.Queue != "workers" {
		t.Errorf("queue from the trait is missing: %+v", op.Bindings)
	}
}

func TestTraitValidatesMergedOperation(t *testing.T) {
	_, err := Parse([]byte(validHead + `
channels:
  c: {address: a, messages: {m: {payload: {type: string}}}}
operations:
  o:
    action: receive
    channel: {$ref: '#/channels/c'}
    traits: [{bindings: {nats: {queue: 'has space'}}}]
`))
	if err == nil || !strings.Contains(err.Error(), "whitespace") {
		t.Fatalf("got error %v, want it to contain %q", err, "whitespace")
	}
}

func TestResolveSchemaMultiFormat(t *testing.T) {
	for _, format := range []string{
		"application/vnd.aai.asyncapi;version=3.1.0",
		"application/vnd.aai.asyncapi+json;version=3.0.0",
		"application/vnd.aai.asyncapi+yaml;version=3.1.0",
		"application/schema+json;version=draft-07",
		"application/schema+yaml; version=draft-07",
	} {
		t.Run(format, func(t *testing.T) {
			doc, err := Parse([]byte(validHead))
			if err != nil {
				t.Fatal(err)
			}
			var s Schema
			if err := unmarshalString("{schemaFormat: '"+format+"', schema: {type: string}}", &s); err != nil {
				t.Fatal(err)
			}
			got, _, err := doc.ResolveSchema(&s)
			if err != nil {
				t.Fatal(err)
			}
			if got.Type != "string" {
				t.Errorf("type = %q, want the inner schema's", got.Type)
			}
		})
	}
}

func TestResolveSchemaUnsupportedFormat(t *testing.T) {
	tests := map[string]string{
		"avro":           "{schemaFormat: application/vnd.apache.avro+json;version=1.9.0, schema: {type: record, fields: [{type: [a, b]}]}}",
		"protobuf":       "{schemaFormat: application/vnd.google.protobuf;version=3, schema: 'message M {}'}",
		"old json draft": "{schemaFormat: application/schema+json;version=draft-04, schema: {}}",
	}
	for name, src := range tests {
		t.Run(name, func(t *testing.T) {
			doc, err := Parse([]byte(validHead))
			if err != nil {
				t.Fatal(err)
			}
			var s Schema
			if err := unmarshalString(src, &s); err != nil {
				t.Fatalf("unmarshal must leave the inner schema undecoded: %v", err)
			}
			if _, _, err := doc.ResolveSchema(&s); err == nil || !strings.Contains(err.Error(), "unsupported schemaFormat") {
				t.Fatalf("got error %v, want it to contain %q", err, "unsupported schemaFormat")
			}
		})
	}
}

func unmarshalString(src string, v any) error { return yaml.Unmarshal([]byte(src), v) }
