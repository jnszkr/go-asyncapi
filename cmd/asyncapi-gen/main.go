// Command asyncapi-gen generates typed Go NATS code from an AsyncAPI document.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jnszkr/go-asyncapi/internal/codegen"
	"github.com/jnszkr/go-asyncapi/internal/spec"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "asyncapi-gen:", err)
		os.Exit(1)
	}
}

func run() error {
	specPath := flag.String("spec", "", "path to the AsyncAPI document (YAML or JSON)")
	out := flag.String("out", "", "output directory for the generated package")
	pkg := flag.String("package", "", "name of the generated package (default: base name of -out)")
	file := flag.String("file", "asyncapi.gen.go", "name of the generated file")
	generate := flag.String("generate", "models,producer,consumer,client,server", "comma-separated parts to generate; models are always included")
	flag.Parse()

	if *specPath == "" || *out == "" {
		flag.Usage()
		return fmt.Errorf("-spec and -out are required")
	}
	if *pkg == "" {
		abs, err := filepath.Abs(*out)
		if err != nil {
			return err
		}
		*pkg = filepath.Base(abs)
	}

	parts, err := codegen.ParseParts(*generate)
	if err != nil {
		return fmt.Errorf("-generate: %w", err)
	}

	doc, err := spec.LoadFile(*specPath)
	if err != nil {
		return err
	}
	src, err := codegen.Generate(doc, codegen.Options{
		Package: *pkg,
		Parts:   parts,
		Warn:    func(w string) { fmt.Fprintln(os.Stderr, "asyncapi-gen: warning:", w) },
	})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*out, *file), src, 0o644)
}
