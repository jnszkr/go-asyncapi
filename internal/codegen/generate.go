// Package codegen generates models, channel definitions and thin typed
// wrappers on top of package broker from an AsyncAPI document.
package codegen

import (
	"bytes"
	_ "embed"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"slices"
	"sort"
	"strconv"
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
	// Warn receives what the generated code cannot express, such as a oneOf
	// generated as json.RawMessage. It may be nil.
	Warn func(string)
}

// Generate renders the Go source for doc.
func Generate(doc *spec.Document, opts Options) ([]byte, error) {
	if opts.Package == "" {
		return nil, fmt.Errorf("package name is required")
	}
	m, err := buildModel(doc, opts.Package, opts.Warn)
	if err != nil {
		return nil, err
	}
	if err := m.selectParts(opts.Parts); err != nil {
		return nil, err
	}
	if len(m.Ops) > 0 {
		if err := requireNATS(doc); err != nil {
			return nil, err
		}
	}
	m.Imports = imports(m)

	tmpl, err := template.New("gen").Funcs(template.FuncMap{
		"lines": func(s string) []string { return strings.Split(strings.TrimSpace(s), "\n") },
		"literal": func(s string) string {
			if strconv.CanBackquote(s) {
				return "`" + s + "`"
			}
			return strconv.Quote(s)
		},
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
	if err := checkUniqueNames(src); err != nil {
		return nil, err
	}
	return src, nil
}

// requireNATS rejects documents that only describe servers of other protocols,
// since the operations are generated as NATS clients.
func requireNATS(doc *spec.Document) error {
	var others []string
	for _, name := range sortedKeys(doc.Servers) {
		server, _, err := doc.ResolveServer(doc.Servers[name])
		if err != nil {
			return fmt.Errorf("server %q: %w", name, err)
		}
		if strings.HasPrefix(strings.ToLower(server.Protocol), "nats") {
			return nil
		}
		others = append(others, name+" ("+server.Protocol+")")
	}
	if len(others) == 0 {
		return nil
	}
	return fmt.Errorf("the document has no NATS server, only %s: select only the models part (-generate models) to use its types", strings.Join(others, ", "))
}

// checkUniqueNames reports names in the document that map to the same
// generated identifier, which would otherwise surface as a compile error.
func checkUniqueNames(src []byte) error {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, parser.SkipObjectResolution)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	var dup string
	add := func(name string) {
		if seen[name] && dup == "" {
			dup = name
		}
		seen[name] = true
	}
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil {
				add(d.Name.Name)
			}
		case *ast.GenDecl:
			for _, sp := range d.Specs {
				switch sp := sp.(type) {
				case *ast.TypeSpec:
					add(sp.Name.Name)
				case *ast.ValueSpec:
					for _, n := range sp.Names {
						add(n.Name)
					}
				}
			}
		}
	}
	if dup != "" {
		return fmt.Errorf("%s would be generated twice: rename what it is generated from, since names that differ only in punctuation or case map to one Go name", dup)
	}
	return nil
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
