package spec

import (
	"slices"
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

func TestSchemaValidationKeywords(t *testing.T) {
	var s Schema
	if err := unmarshalString(`{
		type: number, minimum: 1, maximum: 2.5, exclusiveMinimum: 0, exclusiveMaximum: true, multipleOf: 0.5,
		minLength: 1, maxLength: 2, pattern: '^a', minItems: 3, maxItems: 4, uniqueItems: true,
		minProperties: 5, maxProperties: 6,
		not: {type: string}, if: {type: integer}, then: {minimum: 3}, else: {maximum: 4},
		x-other: 1
	}`, &s); err != nil {
		t.Fatal(err)
	}
	if *s.Minimum != 1 || *s.Maximum != 2.5 || s.ExclusiveMinimum != 0 || s.ExclusiveMaximum != true || *s.MultipleOf != 0.5 {
		t.Errorf("numbers = %v %v %v %v %v", *s.Minimum, *s.Maximum, s.ExclusiveMinimum, s.ExclusiveMaximum, *s.MultipleOf)
	}
	if *s.MinLength != 1 || *s.MaxLength != 2 || s.Pattern != "^a" || *s.MinItems != 3 || *s.MaxItems != 4 || !s.UniqueItems ||
		*s.MinProperties != 5 || *s.MaxProperties != 6 {
		t.Errorf("counts = %+v", s)
	}
	if s.Not.Type != "string" || s.If.Type != "integer" || *s.Then.Minimum != 3 || *s.Else.Maximum != 4 {
		t.Errorf("subschemas = %+v %+v %+v %+v", s.Not, s.If, s.Then, s.Else)
	}
	want := []string{"type", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
		"minLength", "maxLength", "pattern", "minItems", "maxItems", "uniqueItems", "minProperties", "maxProperties",
		"not", "if", "then", "else", "x-other"}
	if !slices.Equal(s.Keywords, want) {
		t.Errorf("keywords = %q, want %q", s.Keywords, want)
	}
	if !s.Has("x-other") || s.Has("const") || !s.Then.Has("minimum") || s.Then.Has("type") {
		t.Error("Has does not report the keywords the schema was written with")
	}
}

func TestSchemaKeywordsThroughAliasesAndRefs(t *testing.T) {
	var s struct {
		A *Schema `yaml:"a"`
		B *Schema `yaml:"b"`
	}
	if err := unmarshalString("{a: &x {const: null, enum: [1]}, b: *x}", &s); err != nil {
		t.Fatal(err)
	}
	for _, sc := range []*Schema{s.A, s.B} {
		if !slices.Equal(sc.Keywords, []string{"const", "enum"}) {
			t.Errorf("keywords = %q", sc.Keywords)
		}
	}
	// Decoding again starts afresh, as when a $ref is resolved into a
	// schema that had keywords of its own.
	if err := unmarshalString("{type: string}", s.A); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(s.A.Keywords, []string{"type"}) {
		t.Errorf("keywords after decoding again = %q", s.A.Keywords)
	}
	var empty Schema
	if err := unmarshalString("null", &empty); err != nil || empty.Keywords != nil {
		t.Errorf("null schema: %q, %v", empty.Keywords, err)
	}
	if err := unmarshalString("{minLength: x}", &empty); err == nil {
		t.Error("minLength: x decoded without an error")
	}
}
