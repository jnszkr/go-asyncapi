package spec

import (
	"strings"
	"testing"
)

const queueOp = `
channels:
  c: {address: a, messages: {m: {payload: {type: string}}}}
operations:
  o:
    action: receive
    channel: {$ref: '#/channels/c'}
    bindings: {nats: {queue: '%s'}}
`

func TestNATSQueueLength(t *testing.T) {
	src := func(q string) []byte { return []byte(validHead + strings.Replace(queueOp, "%s", q, 1)) }
	if _, err := Parse(src(strings.Repeat("q", maxQueueLen))); err != nil {
		t.Errorf("queue of %d characters: %v", maxQueueLen, err)
	}
	if _, err := Parse(src(strings.Repeat("q", maxQueueLen+1))); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("queue of %d characters: err = %v, want too long", maxQueueLen+1, err)
	}
}

func TestEveryAddressParameterMustBeDeclared(t *testing.T) {
	_, err := Parse([]byte(validHead + "channels:\n  c: {address: 'a.{id}.{kind}', parameters: {id: {}}}\n"))
	if err == nil || !strings.Contains(err.Error(), `parameter "kind"`) {
		t.Errorf("err = %v, want the second parameter reported", err)
	}
}

func TestTraitRefCycle(t *testing.T) {
	doc, err := Parse([]byte(validHead + `
channels:
  c: {address: a, messages: {m: {payload: {type: string}, traits: [{$ref: '#/components/messageTraits/a'}]}}}
components:
  messageTraits:
    a: {$ref: '#/components/messageTraits/b'}
    b: {$ref: '#/components/messageTraits/a'}
`))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = doc.MessageByRef("#/channels/c/messages/m")
	if err == nil || !strings.Contains(err.Error(), "too deep") {
		t.Errorf("err = %v, want a ref cycle error", err)
	}
}

func TestParameterRefCycle(t *testing.T) {
	doc, err := Parse([]byte(validHead + `
channels:
  c: {address: 'a.{id}', parameters: {id: {$ref: '#/components/parameters/a'}}}
components:
  parameters:
    a: {$ref: '#/components/parameters/b'}
    b: {$ref: '#/components/parameters/a'}
`))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = doc.ResolveParameter(&Parameter{Ref: "#/components/parameters/a"})
	if err == nil || !strings.Contains(err.Error(), "too deep") {
		t.Errorf("err = %v, want a ref cycle error", err)
	}
}

func TestResolveSchemaReturnsRefName(t *testing.T) {
	doc, err := Parse([]byte(validHead + "components:\n  schemas:\n    A: {$ref: '#/components/schemas/B'}\n    B: {type: string}\n"))
	if err != nil {
		t.Fatal(err)
	}
	got, name, err := doc.ResolveSchema(&Schema{Ref: "#/components/schemas/A"})
	if err != nil || got.Type != "string" || name != "B" {
		t.Errorf("ResolveSchema = %+v, %q, %v; want string, B", got, name, err)
	}
	if _, name, err := doc.ResolveSchema(&Schema{Type: "string"}); err != nil || name != "" {
		t.Errorf("inline schema: name = %q, %v; want none", name, err)
	}
}

func TestPayloadSchemaNumbersCollidingDefinitions(t *testing.T) {
	got := payloadSchema(t,
		`{properties: {a: {$ref: '#/components/schemas/A/properties/x'}, b: {$ref: '#/components/schemas/B/properties/x'}, c: {$ref: '#/components/schemas/C/properties/x'}}}`,
		`{schemas: {A: {properties: {x: {type: string}}}, B: {properties: {x: {type: integer}}}, C: {properties: {x: {type: boolean}}}}}`)
	for _, want := range []string{`"x":{"type":"string"}`, `"x2":{"type":"integer"}`, `"x3":{"type":"boolean"}`} {
		if !strings.Contains(got, want) {
			t.Errorf("got %s, want it to contain %s", got, want)
		}
	}
}
