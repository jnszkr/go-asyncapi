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
	"slices"
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

const allOperations = `
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  events:
    address: events.{region}
    parameters: {region: {enum: [eu]}}
    messages:
      event:
        headers:
          type: object
          properties: {trace: {type: string}}
        payload:
          type: object
          properties:
            at: {type: string, format: date-time}
  ticks:
    address: ticks
    messages:
      tick:
        payload: {type: string}
  lookups:
    address: lookups
    messages:
      lookup:
        payload: {type: string}
  answers:
    address: null
    messages:
      answer:
        payload: {type: string}
operations:
  sendEvent:
    action: send
    channel: {$ref: '#/channels/events'}
  onTick:
    action: receive
    channel: {$ref: '#/channels/ticks'}
  getLookup:
    action: send
    channel: {$ref: '#/channels/lookups'}
    reply: {channel: {$ref: '#/channels/answers'}}
  serveLookup:
    action: receive
    channel: {$ref: '#/channels/lookups'}
    reply: {channel: {$ref: '#/channels/answers'}}
`

func TestParts(t *testing.T) {
	models := []string{"type Event struct", `EventHeaderTrace = "trace"`, `"time"`, "type Tick = string", "type Lookup = string", "type Answer = string"}
	producer := []string{"type Producer struct", "func NewProducer(", "func (c *Producer) SendEvent(", "EventsChannel", "EventsParams"}
	consumer := []string{"type Consumer struct", "func NewConsumer(", "func (c *Consumer) OnTick(", "TicksChannel", "OnTickEndpoint"}
	client := []string{"type Client struct", "func NewClient(", "func (c *Client) GetLookup(", "LookupsChannel"}
	server := []string{"type Server struct", "func NewServer(", "func (c *Server) ServeLookup(", "LookupsChannel", "ServeLookupEndpoint"}

	tests := map[string]struct {
		parts   []Part
		want    [][]string
		unwant  [][]string
		context bool
	}{
		"default":  {nil, [][]string{producer, consumer, client, server}, nil, true},
		"all":      {allParts, [][]string{producer, consumer, client, server}, nil, true},
		"models":   {[]Part{PartModels}, nil, [][]string{producer, consumer, client, server, {"broker"}}, false},
		"producer": {[]Part{PartProducer}, [][]string{producer}, [][]string{consumer, client, server}, true},
		"consumer": {[]Part{PartConsumer}, [][]string{consumer}, [][]string{producer, client, server}, false},
		"client":   {[]Part{PartClient}, [][]string{client}, [][]string{producer, consumer, {"type Server struct", "ServeLookupEndpoint"}}, true},
		"server":   {[]Part{PartServer}, [][]string{server}, [][]string{producer, consumer, {"type Client struct"}}, false},
		"consumer and client": {
			[]Part{PartClient, PartConsumer}, [][]string{consumer, client}, [][]string{producer, {"type Server struct"}}, true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			doc, err := spec.Parse([]byte(allOperations))
			if err != nil {
				t.Fatal(err)
			}
			got, err := Generate(doc, Options{Package: "p", Parts: tc.parts})
			if err != nil {
				t.Fatal(err)
			}
			out := string(got)
			for _, want := range append(slices.Concat(tc.want...), models...) {
				if !strings.Contains(out, want) {
					t.Errorf("generated code does not contain %q:\n%s", want, out)
				}
			}
			for _, unwanted := range slices.Concat(tc.unwant...) {
				// LookupsChannel and friends are shared, so only names
				// exclusive to an unselected part are meaningful here.
				if slices.Contains(slices.Concat(tc.want...), unwanted) {
					continue
				}
				if strings.Contains(out, unwanted) {
					t.Errorf("generated code contains %q:\n%s", unwanted, out)
				}
			}
			if strings.Contains(out, `"context"`) != tc.context {
				t.Errorf("imports context = %v, want %v:\n%s", !tc.context, tc.context, out)
			}
			typecheck(t, out)
		})
	}
}

func TestPartWithoutOperationsIsLeftOut(t *testing.T) {
	doc, err := spec.Parse([]byte(`
asyncapi: 3.0.0
info: {title: t, version: "1"}
components:
  schemas:
    Name: {type: string}
`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := Generate(doc, Options{Package: "p", Parts: []Part{PartProducer}})
	if err != nil {
		t.Fatal(err)
	}
	out := string(got)
	if !strings.Contains(out, "type Name = string") {
		t.Errorf("generated code does not contain the model:\n%s", out)
	}
	for _, unwanted := range []string{"broker", "Producer"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("generated code contains %q:\n%s", unwanted, out)
		}
	}
	typecheck(t, out)
}

func TestModelsStillValidateOperations(t *testing.T) {
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
	if _, err := Generate(doc, Options{Package: "p", Parts: []Part{PartModels}}); err == nil || !strings.Contains(err.Error(), "wildcards") {
		t.Fatalf("got error %v, want it to contain %q", err, "wildcards")
	}
}

func TestTypeNameClash(t *testing.T) {
	const src = `
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  c:
    address: a
    messages: {m: {payload: {type: string}}}
operations:
  o: {action: receive, channel: {$ref: '#/channels/c'}}
components:
  schemas:
    Consumer: {type: string}
`
	doc, err := spec.Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Generate(doc, Options{Package: "p"}); err == nil || !strings.Contains(err.Error(), "clashes") {
		t.Fatalf("got error %v, want it to contain %q", err, "clashes")
	}
	// Without the consumer part the schema's name is free.
	got, err := Generate(doc, Options{Package: "p", Parts: []Part{PartModels}})
	if err != nil {
		t.Fatal(err)
	}
	typecheck(t, string(got))
}

func TestParseParts(t *testing.T) {
	got, err := ParseParts("producer, client")
	if err != nil {
		t.Fatal(err)
	}
	if want := []Part{PartProducer, PartClient}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	for _, bad := range []string{"", "producer,", "publisher"} {
		if _, err := ParseParts(bad); err == nil || !strings.Contains(err.Error(), "unknown part") {
			t.Errorf("%q: got error %v, want it to contain %q", bad, err, "unknown part")
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

func TestGoTypeMapping(t *testing.T) {
	const src = `
asyncapi: 3.0.0
info: {title: t, version: "1"}
channels:
  events:
    address: events
    messages:
      event:
        payload: {$ref: '#/components/schemas/Event'}
  addrs:
    address: addrs
    messages:
      addr:
        payload: {x-go-type: netip.Addr, x-go-type-import: net/netip}
operations:
  sendEvent:
    action: send
    channel: {$ref: '#/channels/events'}
  onAddr:
    action: receive
    channel: {$ref: '#/channels/addrs'}
components:
  schemas:
    Event:
      type: object
      properties:
        link: {$ref: '#/components/schemas/Link'}
        timeout: {type: integer, x-go-type: time.Duration, x-go-type-import: time}
        target: {x-go-type: '*url.URL', x-go-type-import: net/url}
        name: {type: string, x-go-type: string}
    Link:
      type: object
      x-go-type: url.URL
      x-go-type-import: net/url
      properties:
        href: {type: string}
    Unused:
      type: string
      x-go-type: big.Int
      x-go-type-import: math/big
`
	doc, err := spec.Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	// Each part leaves out other operations' channels, and the imports must
	// still all be used for the generated code to compile.
	for _, parts := range [][]Part{nil, {PartModels}, {PartProducer}, {PartConsumer}} {
		got, err := Generate(doc, Options{Package: "p", Parts: parts})
		if err != nil {
			t.Fatal(err)
		}
		typecheck(t, string(got))
	}

	got, err := Generate(doc, Options{Package: "p"})
	if err != nil {
		t.Fatal(err)
	}
	out := string(got)
	for _, want := range []string{
		"type Link = url.URL",
		"type Unused = big.Int",
		"type Addr = netip.Addr",
		"Link    *Link",
		"Timeout *time.Duration",
		"Target  *url.URL",
		"Name    *string",
		`"net/url"`,
		"broker.Channel[Addr]",
		"broker.Channel[Event]",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output does not contain %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "type Link struct") {
		t.Errorf("generated a struct for a mapped schema:\n%s", out)
	}
}

func TestGoTypeErrors(t *testing.T) {
	tests := map[string]struct{ schema, wantErr string }{
		"no import":        {`{x-go-type: url.URL}`, "needs x-go-type-import"},
		"unused import":    {`{x-go-type: string, x-go-type-import: time}`, "exactly one package"},
		"two packages":     {`{x-go-type: 'map[a.K]b.V', x-go-type-import: a}`, "exactly one package"},
		"import only":      {`{x-go-type-import: time}`, "without x-go-type"},
		"not a type":       {`{x-go-type: 'os.Exit(1)', x-go-type-import: os}`, "not a Go type"},
		"unparsable":       {`{x-go-type: 'a b'}`, "not a Go type"},
		"bad import path":  {`{x-go-type: a.B, x-go-type-import: 'a"b'}`, "invalid x-go-type-import"},
		"refers to itself": {`{x-go-type: Thing}`, "refers to itself"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := generate(t, `
asyncapi: 3.0.0
info: {title: t, version: "1"}
components:
  schemas:
    Thing: `+tc.schema+"\n")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got error %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
