package codegen

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/jnszkr/go-asyncapi/internal/spec"
	"gopkg.in/yaml.v3"
)

const componentB = `
components:
  schemas:
    B: {type: integer}
    A: {$ref: '#/components/schemas/B'}
    Node: {properties: {next: {$ref: '#/components/schemas/Node'}}}
    M: {type: string}
    Bad: {multipleOf: 0}
`

// compileSchema compiles payload, a schema in YAML or "" for none, as the
// payload of a message named M, next to the components of componentB.
func compileSchema(t *testing.T, payload string) (name string, literals map[string]string, warnings []string, err error) {
	t.Helper()
	doc, err := spec.Parse([]byte(head + componentB))
	if err != nil {
		t.Fatal(err)
	}
	var s *spec.Schema
	if payload != "" {
		s = new(spec.Schema)
		if err := yaml.Unmarshal([]byte(payload), s); err != nil {
			t.Fatal(err)
		}
	}
	ss := newSchemaSet(doc, func(format string, args ...any) { warnings = append(warnings, fmt.Sprintf(format, args...)) })
	name, err = ss.payload(s, "M")
	literals = map[string]string{}
	for _, v := range ss.vars {
		literals[v.Name] = v.Literal
	}
	return name, literals, warnings, err
}

func TestSchemaLiterals(t *testing.T) {
	tests := []struct{ payload, want string }{
		{`{}`, `schema{}`},
		{`{type: string}`, `schema{Type: "string"}`},
		{`{title: t, description: d, examples: [1], default: 1}`, `schema{}`},
		{`{enum: [a, 1, 1.5, true, null, [1], {k: v, a: 2}]}`, `schema{Enum: []any{"a", 1.0, 1.5, true, nil, []any{1.0}, map[string]any{"a": 2.0, "k": "v"}}}`},
		{`{enum: []}`, `schema{Enum: []any{}}`},
		{`{const: 0}`, "schema{\nConst: 0.0,\nHasConst: true,\n}"},
		{`{const: null}`, "schema{\nConst: nil,\nHasConst: true,\n}"},
		{`{required: [a, b]}`, `schema{Required: []string{"a", "b"}}`},
		{`{properties: {b: {}, a: {type: string}}}`, "schema{\nProperties: map[string]*schema{\n\"a\": {Type: \"string\"},\n\"b\": {},\n},\n}"},
		{`{properties: {b: {$ref: '#/components/schemas/B'}}}`, "schema{\nProperties: map[string]*schema{\n\"b\": schemaB,\n},\n}"},
		{`{additionalProperties: false}`, `schema{NoAdditional: true}`},
		{`{additionalProperties: true}`, `schema{}`},
		{`{additionalProperties: {}}`, `schema{}`},
		{`{additionalProperties: {type: integer}}`, `schema{Additional: &schema{Type: "integer"}}`},
		{`{additionalProperties: {$ref: '#/components/schemas/B'}}`, `schema{Additional: schemaB}`},
		{`{minProperties: 1}`, `schema{MinProperties: bound(1)}`},
		{`{maxProperties: 0}`, `schema{MaxProperties: bound(0)}`},
		{`{items: {type: string}}`, `schema{Items: &schema{Type: "string"}}`},
		{`{minItems: 1}`, `schema{MinItems: bound(1)}`},
		{`{maxItems: 2}`, `schema{MaxItems: bound(2)}`},
		{`{uniqueItems: true}`, `schema{UniqueItems: true}`},
		{`{uniqueItems: false}`, `schema{}`},
		{`{minLength: 2}`, `schema{MinLength: bound(2)}`},
		{`{maxLength: 3}`, `schema{MaxLength: bound(3)}`},
		{`{pattern: '^a\d$'}`, `schema{Pattern: regexp.MustCompile("^a\\d$")}`},
		{`{format: int32}`, `schema{}`},
		{`{minimum: 1}`, `schema{Minimum: bound(1.0)}`},
		{`{minimum: -0.5}`, `schema{Minimum: bound(-0.5)}`},
		{`{minimum: 1e21}`, `schema{Minimum: bound(1e+21)}`},
		{`{maximum: 2}`, `schema{Maximum: bound(2.0)}`},
		{`{exclusiveMinimum: 2}`, `schema{ExclusiveMinimum: bound(2.0)}`},
		{`{exclusiveMaximum: 2.5}`, `schema{ExclusiveMaximum: bound(2.5)}`},
		{`{minimum: 1, exclusiveMinimum: true}`, `schema{ExclusiveMinimum: bound(1.0)}`},
		{`{maximum: 1, exclusiveMaximum: true}`, `schema{ExclusiveMaximum: bound(1.0)}`},
		{`{minimum: 1, exclusiveMinimum: false}`, `schema{Minimum: bound(1.0)}`},
		{`{maximum: 1, exclusiveMaximum: false}`, `schema{Maximum: bound(1.0)}`},
		{`{multipleOf: 3}`, `schema{MultipleOf: bound(3.0)}`},
		{`{allOf: [{type: string}, {$ref: '#/components/schemas/B'}]}`, "schema{\nAllOf: []*schema{\n{Type: \"string\"},\nschemaB,\n},\n}"},
		{`{anyOf: [{}]}`, "schema{\nAnyOf: []*schema{\n{},\n},\n}"},
		{`{oneOf: [{}]}`, "schema{\nOneOf: []*schema{\n{},\n},\n}"},
		{`{not: {type: string}}`, `schema{Not: &schema{Type: "string"}}`},
		{`{if: {}, then: {type: string}, else: {$ref: '#/components/schemas/B'}}`, "schema{\nIf: &schema{},\nThen: &schema{Type: \"string\"},\nElse: schemaB,\n}"},
		{`{type: object, required: [a], minLength: 1}`, "schema{\nType: \"object\",\nRequired: []string{\"a\"},\nMinLength: bound(1),\n}"},
	}
	for _, f := range enforcedFormats {
		tests = append(tests, struct{ payload, want string }{`{format: ` + f + `}`, `schema{Format: "` + f + `"}`})
	}
	for _, tt := range tests {
		name, literals, warnings, err := compileSchema(t, tt.payload)
		if err != nil {
			t.Errorf("%s: %v", tt.payload, err)
			continue
		}
		if name != "schemaM" || literals["schemaM"] != tt.want {
			t.Errorf("%s: %s = %s\nwant %s", tt.payload, name, literals["schemaM"], tt.want)
		}
		if len(warnings) != 0 {
			t.Errorf("%s: unexpected warnings %q", tt.payload, warnings)
		}
	}
}

func TestSchemaRefs(t *testing.T) {
	name, literals, _, err := compileSchema(t, `{$ref: '#/components/schemas/A'}`)
	if err != nil || name != "schemaB" || len(literals) != 1 || literals["schemaB"] != `schema{Type: "integer"}` {
		t.Errorf("ref chain: %s, %q, %v", name, literals, err)
	}

	name, literals, _, err = compileSchema(t, `{$ref: '#/components/schemas/Node'}`)
	if err != nil || name != "schemaNode" || len(literals) != 1 ||
		literals["schemaNode"] != "schema{\nProperties: map[string]*schema{\n\"next\": schemaNode,\n},\n}" {
		t.Errorf("recursive: %s, %q, %v", name, literals, err)
	}

	// A $ref to a schema outside components gets a variable too, as does the
	// schema a chain of refs ends at, and names stay unique.
	name, literals, _, err = compileSchema(t, `{properties: {a: {$ref: '#/components/schemas/M'}, b: {$ref: '#/components/schemas/Node/properties/next'}, c: {$ref: '#/components/schemas/Node/properties'}}}`)
	want := map[string]string{
		"schemaM":          "schema{\nProperties: map[string]*schema{\n\"a\": schemaM2,\n\"b\": schemaNode,\n\"c\": schemaProperties,\n},\n}",
		"schemaM2":         `schema{Type: "string"}`,
		"schemaNode":       "schema{\nProperties: map[string]*schema{\n\"next\": schemaNode,\n},\n}",
		"schemaProperties": "schema{}",
	}
	if err != nil || name != "schemaM" || !maps.Equal(literals, want) {
		t.Errorf("names: %s, %q, %v", name, literals, err)
	}

	// The variable is reused however often the schema is referred to.
	ss := newSchemaSet(mustParse(t, head+componentB), func(string, ...any) {})
	for range 2 {
		for _, ref := range []string{"#/components/schemas/B", "#/components/schemas/A"} {
			if got, err := ss.payload(&spec.Schema{Ref: ref}, "M"); err != nil || got != "schemaB" {
				t.Errorf("payload(%s) = %s, %v", ref, got, err)
			}
		}
	}
	if len(ss.vars) != 1 {
		t.Errorf("vars = %d, want 1", len(ss.vars))
	}

	if name, literals, _, err := compileSchema(t, ""); err != nil || name != "" || len(literals) != 0 {
		t.Errorf("no payload: %q, %q, %v", name, literals, err)
	}
	if name, literals, _, err := compileSchema(t, `{schemaFormat: application/schema+json;version=draft-07, schema: {type: string}}`); err != nil || name != "schemaM" || literals[name] != `schema{Type: "string"}` {
		t.Errorf("multi format: %q, %q, %v", name, literals, err)
	}
	if name, _, _, err := compileSchema(t, `{schemaFormat: application/schema+json;version=draft-07, schema: {$ref: '#/components/schemas/B'}}`); err != nil || name != "schemaB" {
		t.Errorf("multi format ref: %q, %v", name, err)
	}
}

func mustParse(t *testing.T, src string) *spec.Document {
	t.Helper()
	doc, err := spec.Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestSchemaWarnings(t *testing.T) {
	tests := []struct{ payload, want, literal string }{
		{`{format: uri}`, `M: format "uri" is not enforced`, `schema{}`},
		{`{pattern: '(?=a)'}`, `M: pattern "(?=a)" is not enforced: Go's regexp package does not support it`, `schema{}`},
		{`{properties: {a: {format: uri}}}`, `M/properties/a: format "uri" is not enforced`, ""},
		{`{items: {format: uri}}`, `M/items: format "uri" is not enforced`, ""},
		{`{additionalProperties: {format: uri}}`, `M/additionalProperties: format "uri" is not enforced`, ""},
		{`{allOf: [{}, {format: uri}]}`, `M/allOf/1: format "uri" is not enforced`, ""},
		{`{anyOf: [{format: uri}]}`, `M/anyOf/0: format "uri" is not enforced`, ""},
		{`{oneOf: [{format: uri}]}`, `M/oneOf/0: format "uri" is not enforced`, ""},
		{`{not: {format: uri}}`, `M/not: format "uri" is not enforced`, ""},
		{`{if: {format: uri}}`, `M/if: format "uri" is not enforced`, ""},
		{`{then: {format: uri}}`, `M/then: format "uri" is not enforced`, ""},
		{`{else: {format: uri}}`, `M/else: format "uri" is not enforced`, ""},
		{`{patternProperties: {a: {}}, additionalProperties: false}`, `M: additionalProperties next to patternProperties is not enforced`, `schema{}`},
		{`{patternProperties: {a: {}}, additionalProperties: {type: string}}`, `M: additionalProperties next to patternProperties is not enforced`, `schema{}`},
	}
	for _, kw := range unenforcedKeywords {
		tests = append(tests, struct{ payload, want, literal string }{`{` + kw + `: {}}`, `M: ` + kw + ` is not enforced`, `schema{}`})
	}
	for _, tt := range tests {
		_, literals, warnings, err := compileSchema(t, tt.payload)
		if err != nil {
			t.Errorf("%s: %v", tt.payload, err)
			continue
		}
		if !slices.Contains(warnings, tt.want) {
			t.Errorf("%s: warnings %q do not contain %q", tt.payload, warnings, tt.want)
		}
		if tt.literal != "" && literals["schemaM"] != tt.literal {
			t.Errorf("%s: literal = %s, want %s", tt.payload, literals["schemaM"], tt.literal)
		}
	}
	_, _, warnings, _ := compileSchema(t, `{patternProperties: {a: {}}}`)
	if len(warnings) != 1 {
		t.Errorf("patternProperties alone: warnings = %q, want one", warnings)
	}
}

func TestSchemaErrors(t *testing.T) {
	tests := []struct{ payload, want string }{
		{`{multipleOf: 0}`, "M: multipleOf must be greater than 0"},
		{`{multipleOf: -1}`, "M: multipleOf must be greater than 0"},
		{`{minimum: .inf}`, "M: minimum: must be finite, got +Inf"},
		{`{maximum: -.inf}`, "M: maximum: must be finite, got -Inf"},
		{`{multipleOf: .nan}`, "M: multipleOf: must be finite, got NaN"},
		{`{exclusiveMinimum: .nan}`, "M: exclusiveMinimum: must be finite, got NaN"},
		{`{exclusiveMaximum: .inf}`, "M: exclusiveMaximum: must be finite, got +Inf"},
		{`{exclusiveMinimum: true}`, "M: exclusiveMinimum: true needs a bound next to it"},
		{`{exclusiveMaximum: true}`, "M: exclusiveMaximum: true needs a bound next to it"},
		{`{exclusiveMinimum: x}`, "M: exclusiveMinimum: must be a number, got x"},
		{`{exclusiveMaximum: [1]}`, "M: exclusiveMaximum: must be a number, got [1]"},
		{`{enum: [.inf]}`, "M: enum: must be finite, got +Inf"},
		{`{enum: [[.nan]]}`, "M: enum: must be finite, got NaN"},
		{`{enum: [{a: .inf}]}`, "M: enum: must be finite, got +Inf"},
		{`{enum: [{1: a}]}`, "M: enum: map[1:a] (map[interface {}]interface {}) is not a JSON value"},
		{`{const: .inf}`, "M: const: must be finite, got +Inf"},
		{`{$ref: '#/components/schemas/Missing'}`, `"Missing" not found`},
		{`{$ref: '#/info/title'}`, "cannot unmarshal"},
		{`{$ref: '#/components/schemas/Bad'}`, "#/components/schemas/Bad: multipleOf must be greater than 0"},
		{`{schemaFormat: application/vnd.apache.avro;version=1.9.0, schema: {}}`, "unsupported schemaFormat"},
		{`{properties: {a: {schemaFormat: x, schema: {}}}}`, "unsupported schemaFormat"},
		{`{properties: {a: {multipleOf: 0}}}`, "M/properties/a: multipleOf must be greater than 0"},
		{`{additionalProperties: {type: [a, b]}}`, "cannot unmarshal"},
		{`{additionalProperties: {multipleOf: 0}}`, "M/additionalProperties: multipleOf"},
		{`{items: {multipleOf: 0}}`, "M/items: multipleOf"},
		{`{allOf: [{}, {multipleOf: 0}]}`, "M/allOf/1: multipleOf"},
		{`{anyOf: [{multipleOf: 0}]}`, "M/anyOf/0: multipleOf"},
		{`{oneOf: [{multipleOf: 0}]}`, "M/oneOf/0: multipleOf"},
		{`{not: {multipleOf: 0}}`, "M/not: multipleOf"},
		{`{if: {multipleOf: 0}}`, "M/if: multipleOf"},
		{`{then: {multipleOf: 0}}`, "M/then: multipleOf"},
		{`{else: {multipleOf: 0}}`, "M/else: multipleOf"},
	}
	for _, tt := range tests {
		_, _, _, err := compileSchema(t, tt.payload)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: err = %v, want it to contain %q", tt.payload, err, tt.want)
		}
	}
}

// TestValidationIsWired checks where the generated operations validate.
func TestValidationIsWired(t *testing.T) {
	out, _ := compile(t, allOperations)
	contains(t, out,
		// Producer and Client check what they send.
		"if err := schemaEvent.validate(body); err != nil {\n\t\treturn fmt.Errorf(\"Event: %w\", err)",
		"if err := schemaLookup.validate(body); err != nil {\n\t\treturn resp, fmt.Errorf(\"Lookup: %w\", err)",
		// Consumer and Server check what they receive.
		"decode(TicksChannel, schemaTick, msg)",
		"decode(LookupsChannel, schemaLookup, msg)",
		// Server checks its reply.
		"if err := schemaAnswer.validate(body); err != nil {\n\t\t\t\treturn nil, fmt.Errorf(\"Answer: %w\", err)",
		"*schemaTick = schema{Type: \"string\"}",
	)
	// Only the operations' payloads are compiled, once each.
	if n := strings.Count(out, "schemaLookup = &schema{}"); n != 1 || strings.Contains(out, "schemaLookup2") {
		t.Errorf("schemaLookup declared %d times, or again under another name", n)
	}

	// Payloads without a schema are not checked.
	out, _ = compile(t, head+`
channels:
  c: {address: a, messages: {m: {name: M}}}
  r: {address: null, messages: {m: {name: R}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
  get: {action: send, channel: {$ref: '#/channels/c'}, reply: {channel: {$ref: '#/channels/r'}}}
  serve: {action: receive, channel: {$ref: '#/channels/c'}, reply: {channel: {$ref: '#/channels/r'}}}
`)
	contains(t, out, "decode(CChannel, nil, msg)", "return json.Marshal(resp)")
	if strings.Contains(out, ".validate(body)") || strings.Contains(out, "func init()") {
		t.Errorf("payloads without a schema are checked:\n%s", out)
	}

	// The models alone have no validator.
	doc := mustParse(t, allOperations)
	models, err := Generate(doc, Options{Package: "p", Parts: []Part{PartModels}})
	if err != nil {
		t.Fatal(err)
	}
	typecheck(t, string(models))
	if strings.Contains(string(models), "schema") {
		t.Errorf("models contain the validator:\n%s", models)
	}

	// Errors in a payload schema are reported with the message.
	_, err = generate(t, payloadDoc(`{multipleOf: 0}`))
	if err == nil || !strings.Contains(err.Error(), "message M payload: M: multipleOf must be greater than 0") {
		t.Errorf("err = %v", err)
	}
}

// TestKeywordsCompile checks that every keyword compiles into valid code.
func TestKeywordsCompile(t *testing.T) {
	_, warnings := compile(t, payloadDoc(`{
		type: object, required: [a], additionalProperties: false, minProperties: 1, maxProperties: 9,
		properties: {
			a: {type: string, minLength: 1, maxLength: 9, pattern: '^a', format: uuid, enum: [a], const: a},
			b: {type: array, items: {type: integer}, minItems: 1, maxItems: 2, uniqueItems: true},
			c: {type: number, minimum: 0, maximum: 9, exclusiveMinimum: 0, exclusiveMaximum: 9, multipleOf: 0.5},
			d: {type: object, additionalProperties: {type: boolean}},
			e: {allOf: [{type: string}], not: {const: x}, if: {const: y}, then: {}, else: {}},
			f: {anyOf: [{type: string}]},
			g: {oneOf: [{type: string}]}
		}
	}`))
	for _, w := range warnings {
		if strings.Contains(w, "enforced") {
			t.Errorf("unexpected warning %q", w)
		}
	}
}
