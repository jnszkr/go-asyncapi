// Package codegen generates models, channel definitions and a thin typed
// client on top of package broker from an AsyncAPI document.
package codegen

import (
	"bytes"
	_ "embed"
	"fmt"
	"go/format"
	"sort"
	"strings"
	"text/template"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

//go:embed template.go.tmpl
var tmplSrc string

// Options configures code generation.
type Options struct {
	// Package is the name of the generated Go package.
	Package string
}

// Generate renders the Go source for doc.
func Generate(doc *spec.Document, opts Options) ([]byte, error) {
	if opts.Package == "" {
		return nil, fmt.Errorf("package name is required")
	}
	m, err := buildModel(doc, opts.Package)
	if err != nil {
		return nil, err
	}
	m.Imports = imports(m)

	tmpl, err := template.New("gen").Funcs(template.FuncMap{
		"export": exportName,
		"lines":  func(s string) []string { return strings.Split(strings.TrimSpace(s), "\n") },
	}).Parse(tmplSrc)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, m); err != nil {
		return nil, err
	}
	src, err := format.Source(buf.Bytes())
	if err != nil {
		// Return the unformatted source to ease debugging of the template.
		return buf.Bytes(), fmt.Errorf("format generated code: %w", err)
	}
	return src, nil
}

// imports returns the stdlib imports used by the generated file.
func imports(m *model) []string {
	set := map[string]bool{}
	for _, imp := range m.Imports {
		set[imp] = true
	}
	for _, op := range m.Ops {
		if op.IsSend() {
			set["context"] = true
		}
	}
	out := make([]string, 0, len(set))
	for imp := range set {
		out = append(out, imp)
	}
	sort.Strings(out)
	return out
}
