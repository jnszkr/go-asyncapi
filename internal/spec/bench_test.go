package spec

import (
	"os"
	"testing"
)

// Run with: go test ./internal/spec -run '^$' -bench . -benchmem

const benchSpec = "../../examples/orders/asyncapi.yaml"

func readBenchSpec(b *testing.B) []byte {
	b.Helper()
	src, err := os.ReadFile(benchSpec)
	if err != nil {
		b.Fatal(err)
	}
	return src
}

// BenchmarkParse measures decoding and validating the example document.
func BenchmarkParse(b *testing.B) {
	src := readBenchSpec(b)
	b.SetBytes(int64(len(src)))
	for b.Loop() {
		if _, err := Parse(src); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPayloadSchema measures bundling every component message's payload
// into a self-contained JSON Schema, as the generator does per message.
func BenchmarkPayloadSchema(b *testing.B) {
	doc, err := Parse(readBenchSpec(b))
	if err != nil {
		b.Fatal(err)
	}
	if len(doc.Components.Messages) == 0 {
		b.Fatal("example has no component messages")
	}
	for b.Loop() {
		for _, m := range doc.Components.Messages {
			msg, _, err := doc.ResolveMessage(m)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := doc.PayloadSchema(msg); err != nil {
				b.Fatal(err)
			}
		}
	}
}
