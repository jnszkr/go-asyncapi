package codegen

import (
	"testing"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

// Run with: go test ./internal/codegen -run '^$' -bench . -benchmem

// BenchmarkGenerate measures generating the orders example from an already
// parsed document, including gofmt and the generated-name checks.
func BenchmarkGenerate(b *testing.B) {
	doc, err := spec.LoadFile("../../examples/orders/asyncapi.yaml")
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, err := Generate(doc, Options{Package: "orders"}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExportName(b *testing.B) {
	for b.Loop() {
		_ = exportName("order-status.reply_v2")
	}
}
