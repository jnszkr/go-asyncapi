package codegen

import (
	"bytes"
	"flag"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

var update = flag.Bool("update", false, "rewrite golden files")

// TestGolden checks that the checked-in example output is up to date.
func TestGolden(t *testing.T) {
	examples := []struct{ spec, out, pkg string }{
		{"../../examples/orders/asyncapi.yaml", "../../examples/orders/gen/orders/asyncapi.gen.go", "orders"},
	}
	for _, ex := range examples {
		t.Run(ex.pkg, func(t *testing.T) {
			doc, err := spec.LoadFile(ex.spec)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Generate(doc, Options{Package: ex.pkg})
			if err != nil {
				t.Fatal(err)
			}
			if *update {
				if err := os.MkdirAll(filepath.Dir(ex.out), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(ex.out, got, 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(ex.out)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("%s is out of date; run: go test ./internal/codegen -update", ex.out)
			}
		})
	}
}

func generate(t *testing.T, src string) (string, error) {
	t.Helper()
	doc, err := spec.Parse([]byte(src))
	if err != nil {
		return "", err
	}
	out, err := Generate(doc, Options{Package: "p"})
	return string(out), err
}

func TestNoParamsPlainSubscribe(t *testing.T) {
	out, err := generate(t, `
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  ping:
    address: ping
    messages:
      ping:
        payload: {type: string}
operations:
  onPing:
    action: receive
    channel: {$ref: '#/channels/ping'}
`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`PingChannel = broker.Channel[Ping]{`,
		`type Ping = string`,
		`return broker.Subscribe(c.Client, PingChannel, h, opts...)`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("generated code does not contain %q:\n%s", want, out)
		}
	}
}

// typecheck fails the test if src does not compile on its own, e.g. because
// of an unused import.
func typecheck(t *testing.T, src string) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "gen.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	if _, err := conf.Check("p", fset, []*ast.File{f}, nil); err != nil {
		t.Fatalf("generated code does not compile: %v\n%s", err, src)
	}
}

func TestModelsOnly(t *testing.T) {
	tests := map[string]struct {
		src  string
		want []string
	}{
		"operations are dropped": {
			src: `
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  orders:
    address: orders.{region}
    parameters: {region: {enum: [eu]}}
    messages:
      order:
        headers:
          type: object
          properties: {trace: {type: string}}
        payload:
          type: object
          properties:
            at: {type: string, format: date-time}
operations:
  sendOrder:
    action: send
    channel: {$ref: '#/channels/orders'}
  onOrder:
    action: receive
    channel: {$ref: '#/channels/orders'}
`,
			want: []string{"type Order struct", `OrderHeaderTrace = "trace"`, `"time"`},
		},
		"nothing to import": {
			src: `
asyncapi: 3.0.0
info: {title: t, version: "1"}
components:
  schemas:
    Name: {type: string}
`,
			want: []string{"type Name = string"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			doc, err := spec.Parse([]byte(tc.src))
			if err != nil {
				t.Fatal(err)
			}
			got, err := Generate(doc, Options{Package: "p", ModelsOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			out := string(got)
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Errorf("generated code does not contain %q:\n%s", want, out)
				}
			}
			for _, unwanted := range []string{"broker", "Client", "Channel", "Endpoint", "Params", "context"} {
				if strings.Contains(out, unwanted) {
					t.Errorf("generated code contains %q:\n%s", unwanted, out)
				}
			}
			typecheck(t, out)
		})
	}
}

func TestModelsOnlyStillValidatesOperations(t *testing.T) {
	doc, err := spec.Parse([]byte(`
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  c:
    address: a.>
    messages: {m: {payload: {type: string}}}
operations:
  o: {action: send, channel: {$ref: '#/channels/c'}}
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Generate(doc, Options{Package: "p", ModelsOnly: true}); err == nil || !strings.Contains(err.Error(), "wildcards") {
		t.Fatalf("got error %v, want it to contain %q", err, "wildcards")
	}
}

func TestErrors(t *testing.T) {
	tests := map[string]struct{ src, wantErr string }{
		"partial token parameter": {`
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  c:
    address: user-{id}.x
    messages: {m: {payload: {type: string}}}
operations:
  o: {action: send, channel: {$ref: '#/channels/c'}}
`, "whole subject token"},
		"wildcard address": {`
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  c:
    address: a.>
    messages: {m: {payload: {type: string}}}
operations:
  o: {action: send, channel: {$ref: '#/channels/c'}}
`, "wildcards"},
		"multiple messages": {`
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  c:
    address: a
    messages: {m1: {payload: {type: string}}, m2: {payload: {type: string}}}
operations:
  o: {action: send, channel: {$ref: '#/channels/c'}}
`, "only one message"},
		"queue too long": {`
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  c:
    address: a
    messages: {m: {payload: {type: string}}}
operations:
  o:
    action: receive
    channel: {$ref: '#/channels/c'}
    bindings: {nats: {queue: ` + strings.Repeat("q", 256) + `}}
`, "exceeds 255"},
		"asyncapi 2": {`
asyncapi: 2.6.0
info: {title: t, version: "1"}
`, "only 3.x"},
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
