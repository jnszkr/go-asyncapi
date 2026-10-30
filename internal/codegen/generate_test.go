package codegen

import (
	"bytes"
	"flag"
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
