package codegen

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/jnszkr/go-asyncapi/broker/schemavalidate"
	"github.com/jnszkr/go-asyncapi/internal/spec"
)

// constant returns the value of a string constant in generated code.
func constant(t *testing.T, out, name string) string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "", out, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range f.Decls {
		d, ok := decl.(*ast.GenDecl)
		if !ok || d.Tok != token.CONST {
			continue
		}
		for _, sp := range d.Specs {
			vs := sp.(*ast.ValueSpec)
			if vs.Names[0].Name == name {
				v, err := strconv.Unquote(vs.Values[0].(*ast.BasicLit).Value)
				if err != nil {
					t.Fatal(err)
				}
				return v
			}
		}
	}
	t.Fatalf("no constant %s in:\n%s", name, out)
	return ""
}

// TestEmbeddedSchemaEnforcesKeywords checks the schema that ends up in the
// generated code, with keywords the generated types cannot express and refs
// inside them.
func TestEmbeddedSchemaEnforcesKeywords(t *testing.T) {
	out, _ := compile(t, head+`
channels:
  c: {address: a, messages: {m: {payload: {$ref: '#/components/schemas/Order'}}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
components:
  schemas:
    Order:
      type: object
      required: [id]
      properties:
        id: {type: string, pattern: '^o-', description: Identifier.}
        count: {type: integer, minimum: 1}
        kind: {$ref: '#/components/schemas/Kind'}
        state: {not: {$ref: '#/components/schemas/Bad'}}
        parent: {$ref: '#/components/schemas/Order'}
      if: {$ref: '#/components/schemas/Express'}
      then: {required: [courier]}
    Kind: {type: string, enum: [a, b]}
    Bad: {const: bad}
    Express: {required: [express]}
`)
	contains(t, out, "schemaM,")
	check, err := schemavalidate.New().Compile(constant(t, out, "schemaM"))
	if err != nil {
		t.Fatal(err)
	}
	for data, ok := range map[string]bool{
		`{"id":"o-1","count":1,"kind":"a","state":"ok","express":1,"courier":"x","parent":{"id":"o-2"}}`: true,
		`{}`:                         false,
		`{"id":"x"}`:                 false,
		`{"id":"o-1","count":0}`:     false,
		`{"id":"o-1","kind":"c"}`:    false,
		`{"id":"o-1","state":"bad"}`: false,
		`{"id":"o-1","express":1}`:   false,
		`{"id":"o-1","parent":{}}`:   false,
	} {
		if err := check([]byte(data)); (err == nil) != ok {
			t.Errorf("%s: err = %v, want valid = %v", data, err, ok)
		}
	}
}

func TestReplySchemaIsPerOperation(t *testing.T) {
	out, _ := compile(t, head+`
channels:
  c: {address: a, messages: {m: {payload: {type: string}}}}
  r: {address: null, messages: {r1: {payload: {type: integer}}, r2: {payload: {type: boolean}}}}
operations:
  one:
    action: send
    channel: {$ref: '#/channels/c'}
    reply: {channel: {$ref: '#/channels/r'}, messages: [{$ref: '#/channels/r/messages/r1'}]}
  two:
    action: receive
    channel: {$ref: '#/channels/c'}
    reply: {channel: {$ref: '#/channels/r'}, messages: [{$ref: '#/channels/r/messages/r2'}]}
`)
	contains(t, out, "CChannel.WithReplySchema(schemaR1)", "CChannel.WithReplySchema(schemaR2)")
	if got := constant(t, out, "schemaR2"); got != `{"type":"boolean"}` {
		t.Errorf("schemaR2 = %s", got)
	}
}

// TestModelsHaveNoSchemas keeps -generate models free of everything that only
// the broker uses.
func TestModelsHaveNoSchemas(t *testing.T) {
	doc, err := spec.Parse([]byte(payloadDoc(`{type: string}`)))
	if err != nil {
		t.Fatal(err)
	}
	out, err := Generate(doc, Options{Package: "p", Parts: []Part{PartModels}})
	if err != nil {
		t.Fatal(err)
	}
	for _, unwanted := range []string{"broker", "schemaM"} {
		if strings.Contains(string(out), unwanted) {
			t.Errorf("generated code contains %q:\n%s", unwanted, out)
		}
	}
}

// TestOnlySelectedSchemas checks that a schema is declared only when a selected
// operation refers to it, since an unused constant would be dead code.
func TestOnlySelectedSchemas(t *testing.T) {
	doc, err := spec.Parse([]byte(head + `
channels:
  a: {address: a, messages: {ma: {payload: {type: string}}}}
  b: {address: b, messages: {mb: {payload: {type: integer}}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/a'}}
  sub: {action: receive, channel: {$ref: '#/channels/b'}}
`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := Generate(doc, Options{Package: "p", Parts: []Part{PartProducer}})
	if err != nil {
		t.Fatal(err)
	}
	out := string(got)
	typecheck(t, out)
	contains(t, out, "schemaMa")
	if strings.Contains(out, "schemaMb") {
		t.Errorf("generated code declares an unused schema:\n%s", out)
	}
}

func TestSchemaWithBackquoteIsQuoted(t *testing.T) {
	out, _ := compile(t, payloadDoc("{type: string, pattern: 'a`b'}"))
	if got := constant(t, out, "schemaM"); got != "{\"pattern\":\"a`b\",\"type\":\"string\"}" {
		t.Errorf("schemaM = %s", got)
	}
}

func TestNonJSONMessageHasNoSchema(t *testing.T) {
	out, _ := compile(t, head+`
channels:
  c: {address: a, messages: {m: {contentType: application/xml, payload: {type: string}}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
`)
	if strings.Contains(out, "Schema") {
		t.Errorf("generated code has a schema:\n%s", out)
	}
}

func TestMessagesSharingTypeButNotFormat(t *testing.T) {
	out, warnings := compile(t, head+`
channels:
  c:
    address: a
    messages:
      j: {payload: {$ref: '#/components/schemas/S'}}
      x: {contentType: application/xml, payload: {$ref: '#/components/schemas/S'}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}, messages: [{$ref: '#/channels/c/messages/j'}]}
  sub: {action: receive, channel: {$ref: '#/channels/c'}, messages: [{$ref: '#/channels/c/messages/x'}]}
components:
  schemas:
    S: {type: object, properties: {a: {type: string}}}
`)
	if strings.Contains(out, "Schema") {
		t.Errorf("generated code has a schema:\n%s", out)
	}
	warned(t, warnings, "not validated")
}

// TestUnusableSchemaWarns checks that documents the generator accepted before
// schemas were embedded are still accepted, without validation.
func TestUnusableSchemaWarns(t *testing.T) {
	tests := map[string]struct{ payload, want string }{
		"ref outside the document": {`{type: object, properties: {a: {type: string}}, not: {$ref: 'https://example.com/s.json'}}`, "only local refs"},
		"pattern go cannot read":   {`{type: string, pattern: '^(?!x)'}`, "pattern"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			out, warnings := compile(t, payloadDoc(tc.payload))
			if strings.Contains(out, "Schema") {
				t.Errorf("generated code has a schema:\n%s", out)
			}
			warned(t, warnings, "not validated")
			warned(t, warnings, tc.want)
		})
	}
}
