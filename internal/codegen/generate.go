// Package codegen generates a typed NATS client package from an AsyncAPI document.
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
		"export":       exportName,
		"lines":        func(s string) []string { return strings.Split(strings.TrimSpace(s), "\n") },
		"formatHeader": formatHeader,
		"parseHeader":  parseHeader,
		"fail":         failStmt,
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
	set := map[string]bool{"encoding/json": true, "fmt": true, "strings": true, "time": true, "context": true}
	for _, imp := range m.Imports {
		set[imp] = true
	}
	for _, h := range m.Headers {
		for _, f := range h.Fields {
			if f.Type != "string" {
				set["strconv"] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for imp := range set {
		out = append(out, imp)
	}
	sort.Strings(out)
	return out
}

func formatHeader(typ, expr string) string {
	switch typ {
	case "int64":
		return "strconv.FormatInt(" + expr + ", 10)"
	case "float64":
		return "strconv.FormatFloat(" + expr + ", 'g', -1, 64)"
	case "bool":
		return "strconv.FormatBool(" + expr + ")"
	}
	return expr
}

func parseHeader(typ, expr string) string {
	switch typ {
	case "int64":
		return "strconv.ParseInt(" + expr + ", 10, 64)"
	case "float64":
		return "strconv.ParseFloat(" + expr + ", 64)"
	case "bool":
		return "strconv.ParseBool(" + expr + ")"
	}
	return expr + ", error(nil)"
}

// failStmt is the statement a received-message callback runs when decoding fails.
func failStmt(op *operation) string {
	if op.Reply != nil {
		return `c.respondError(m, "400", err)
			return`
	}
	return fmt.Sprintf(`c.errorHandler(fmt.Errorf("%s %%s: %%w", m.Subject, err))
			return`, op.Key)
}
