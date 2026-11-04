package spec

import (
	"strings"
	"testing"
)

func payloadSchema(t *testing.T, payload, components string) string {
	t.Helper()
	doc, err := Parse([]byte(`
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  c:
    address: c
    messages:
      m:
        payload: ` + payload + `
components: ` + components))
	if err != nil {
		t.Fatal(err)
	}
	msg, _, err := doc.MessageByRef("#/channels/c/messages/m")
	if err != nil {
		t.Fatal(err)
	}
	got, err := doc.PayloadSchema(msg)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestPayloadSchema(t *testing.T) {
	for _, tc := range []struct {
		name, payload, components, want string
	}{
		{
			name:    "keywords are kept",
			payload: `{type: object, required: [a], properties: {a: {type: integer, minimum: 1, pattern: "<x>"}}}`,
			want:    `{"properties":{"a":{"minimum":1,"pattern":"<x>","type":"integer"}},"required":["a"],"type":"object"}`,
		},
		{
			name:       "a ref at the root",
			payload:    `{$ref: '#/components/schemas/A'}`,
			components: `{schemas: {A: {type: string}}}`,
			want:       `{"$ref":"#/definitions/A","definitions":{"A":{"type":"string"}}}`,
		},
		{
			name:       "refs are followed in every keyword holding schemas",
			payload:    `{properties: {p: {$ref: '#/components/schemas/A'}}, not: {items: [{$ref: '#/components/schemas/B'}], not: {$ref: '#/components/schemas/C'}}, if: {$ref: '#/components/schemas/D'}, allOf: [{$ref: '#/components/schemas/E'}]}`,
			components: `{schemas: {A: {type: string}, B: {type: string}, C: {type: string}, D: {type: string}, E: {type: string}}}`,
			want:       `{"allOf":[{"$ref":"#/definitions/E"}],"definitions":{"A":{"type":"string"},"B":{"type":"string"},"C":{"type":"string"},"D":{"type":"string"},"E":{"type":"string"}},"if":{"$ref":"#/definitions/D"},"not":{"items":[{"$ref":"#/definitions/B"}],"not":{"$ref":"#/definitions/C"}},"properties":{"p":{"$ref":"#/definitions/A"}}}`,
		},
		{
			name:       "recursive refs",
			payload:    `{$ref: '#/components/schemas/Node'}`,
			components: `{schemas: {Node: {properties: {next: {$ref: '#/components/schemas/Node'}}}}}`,
			want:       `{"$ref":"#/definitions/Node","definitions":{"Node":{"properties":{"next":{"$ref":"#/definitions/Node"}}}}}`,
		},
		{
			name:       "refs with the same last segment get their own definition",
			payload:    `{allOf: [{$ref: '#/components/schemas/A/properties/x'}, {$ref: '#/components/schemas/B/properties/x'}]}`,
			components: `{schemas: {A: {properties: {x: {type: string}}}, B: {properties: {x: {type: integer}}}}}`,
			want:       `{"allOf":[{"$ref":"#/definitions/x"},{"$ref":"#/definitions/x2"}],"definitions":{"x":{"type":"string"},"x2":{"type":"integer"}}}`,
		},
		{
			name:       "a name unusable in a pointer is replaced",
			payload:    `{$ref: '#/components/schemas/a~1b'}`,
			components: `{schemas: {"a/b": {type: string}}}`,
			want:       `{"$ref":"#/definitions/schema","definitions":{"schema":{"type":"string"}}}`,
		},
		{
			name:    "annotations and extensions are dropped",
			payload: `{type: string, title: t, description: d, default: x, examples: [x], deprecated: true, externalDocs: {url: u}, discriminator: k, x-go-type: time.Time, x-go-type-import: time, $id: 'https://example.com/s', $schema: 'http://json-schema.org/draft-07/schema#'}`,
			want:    `{"type":"string"}`,
		},
		{
			name:    "properties named like keywords are kept",
			payload: `{properties: {title: {type: string}, description: {type: string}, $ref: {type: string}, x-y: {type: string}}}`,
			want:    `{"properties":{"$ref":{"type":"string"},"description":{"type":"string"},"title":{"type":"string"},"x-y":{"type":"string"}}}`,
		},
		{
			name:    "values are not read as schemas",
			payload: `{enum: [{$ref: '#/nowhere'}, {title: t}], const: {$ref: '#/nowhere'}}`,
			want:    `{"const":{"$ref":"#/nowhere"},"enum":[{"$ref":"#/nowhere"},{"title":"t"}]}`,
		},
		{
			name:    "boolean schemas",
			payload: `{not: {additionalProperties: false, properties: {a: true}}}`,
			want:    `{"not":{"additionalProperties":false,"properties":{"a":true}}}`,
		},
		{
			name:    "dependencies hold schemas or names",
			payload: `{dependencies: {a: [b], c: {required: [d], title: t}}}`,
			want:    `{"dependencies":{"a":["b"],"c":{"required":["d"]}}}`,
		},
		{
			name:       "definitions are only reached through refs",
			payload:    `{definitions: {unused: {$ref: '#/nowhere'}}, $ref: '#/components/schemas/A'}`,
			components: `{schemas: {A: {type: string}}}`,
			want:       `{"$ref":"#/definitions/A","definitions":{"A":{"type":"string"}}}`,
		},
		{
			name:       "multi format schemas are unwrapped",
			payload:    `{schemaFormat: 'application/schema+yaml;version=draft-07', schema: {$ref: '#/components/schemas/A'}}`,
			components: `{schemas: {A: {schemaFormat: 'application/vnd.aai.asyncapi+yaml', schema: {type: string}}}}`,
			want:       `{"$ref":"#/definitions/A","definitions":{"A":{"type":"string"}}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := payloadSchema(t, tc.payload, tc.components); got != tc.want {
				t.Errorf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestPayloadSchemaIsDeterministic(t *testing.T) {
	const payload = `{allOf: [{$ref: '#/components/schemas/A/properties/x'}, {$ref: '#/components/schemas/B/properties/x'}], not: {$ref: '#/components/schemas/C/properties/x'}}`
	const components = `{schemas: {A: {properties: {x: {type: string}}}, B: {properties: {x: {type: integer}}}, C: {properties: {x: {type: boolean}}}}}`
	first := payloadSchema(t, payload, components)
	for range 20 {
		if got := payloadSchema(t, payload, components); got != first {
			t.Fatalf("got %s and %s", first, got)
		}
	}
}

func TestPayloadSchemaWithoutPayload(t *testing.T) {
	if got := payloadSchema(t, `null`, `{}`); got != "" {
		t.Errorf("got %s", got)
	}
}

func TestPayloadSchemaAppliesTraits(t *testing.T) {
	doc, err := Parse([]byte(`
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  c:
    address: c
    messages:
      m:
        payload: {type: string}
        traits: [{contentType: application/json}]
`))
	if err != nil {
		t.Fatal(err)
	}
	msg, _, err := doc.MessageByRef("#/channels/c/messages/m")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := doc.PayloadSchema(msg); err != nil || got != `{"type":"string"}` {
		t.Errorf("got %s, %v", got, err)
	}
}

func TestPayloadSchemaErrors(t *testing.T) {
	for _, tc := range []struct{ name, payload, components, want string }{
		{"remote ref", `{not: {$ref: 'https://example.com/s.json'}}`, `{}`, "only local refs"},
		{"missing ref", `{if: {$ref: '#/components/schemas/A'}}`, `{}`, "not found"},
		{"other format", `{not: {schemaFormat: application/vnd.apache.avro, schema: {}}}`, `{}`, "unsupported schemaFormat"},
		{"value without a JSON form", `{const: .nan}`, `{}`, "unsupported value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := Parse([]byte(`
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  c:
    address: c
    messages:
      m:
        payload: ` + tc.payload + `
components: ` + tc.components))
			if err != nil {
				t.Fatal(err)
			}
			msg, _, err := doc.MessageByRef("#/channels/c/messages/m")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := doc.PayloadSchema(msg); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}
