// Package codegen generates models, channel definitions and thin typed
// wrappers on top of package broker from an AsyncAPI document.
package codegen

import (
	"bytes"
	_ "embed"
	"fmt"
	"go/format"
	"slices"
	"sort"
	"strings"
	"text/template"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

//go:embed template.go.tmpl
var tmplSrc string

// Part is a selectable part of the generated code.
type Part string

const (
	// PartModels is the types and header-name constants, which are always
	// generated. Selecting only this part leaves out everything that depends
	// on package broker.
	PartModels Part = "models"
	// PartProducer is a Producer type with the send operations.
	PartProducer Part = "producer"
	// PartConsumer is a Consumer type with the receive operations.
	PartConsumer Part = "consumer"
	// PartClient is a Client type with the send operations that have a reply.
	PartClient Part = "client"
	// PartServer is a Server type with the receive operations that have a reply.
	PartServer Part = "server"
)

var allParts = []Part{PartModels, PartProducer, PartConsumer, PartClient, PartServer}

// ParseParts parses a comma-separated list of parts.
func ParseParts(s string) ([]Part, error) {
	var parts []Part
	for name := range strings.SplitSeq(s, ",") {
		p := Part(strings.TrimSpace(name))
		if !slices.Contains(allParts, p) {
			return nil, fmt.Errorf("unknown part %q: must be one of %v", p, allParts)
		}
		parts = append(parts, p)
	}
	return parts, nil
}

// Options configures code generation.
type Options struct {
	// Package is the name of the generated Go package.
	Package string
	// Parts selects what to generate; none means all of them. Channels and
	// their helpers are generated for the operations of the selected parts,
	// and a part without operations is left out. All operations are
	// validated regardless.
	Parts []Part
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
	if err := m.selectParts(opts.Parts); err != nil {
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

// imports returns the imports used by the generated file, other than broker.
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
