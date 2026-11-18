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
