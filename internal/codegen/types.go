package codegen

import (
	"fmt"
	"go/ast"
	"go/parser"
	"sort"
	"strconv"
	"strings"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

const componentSchemaPrefix = "#/components/schemas/"

// typeDecl is a named Go type generated from a JSON schema.
type typeDecl struct {
	Name    string
	Doc     string
	Kind    string // "struct", "enum", "alias"
	Fields  []field
	Under   string   // underlying type for "enum" and "alias"
	Values  []string // Go literals for "enum"
	Imports []string
}

type field struct {
	Name     string
	JSONName string
	Type     string
	Doc      string
	Required bool
}

// typeSet tracks generated named types and the imports they require.
type typeSet struct {
	doc     *spec.Document
	decls   []*typeDecl
	byName  map[string]*typeDecl
	byRef   map[string]string // component schema ref → Go type name
	imports map[string]bool
}

func newTypeSet(doc *spec.Document) *typeSet {
	return &typeSet{
		doc:     doc,
		byName:  map[string]*typeDecl{},
		byRef:   map[string]string{},
		imports: map[string]bool{},
	}
}

// uniqueName returns name, or name with a numeric suffix if it is taken.
func (ts *typeSet) uniqueName(name string) string {
	if _, taken := ts.byName[name]; !taken {
		return name
	}
	for i := 2; ; i++ {
		n := name + strconv.Itoa(i)
		if _, taken := ts.byName[n]; !taken {
			return n
		}
	}
}

func (ts *typeSet) add(d *typeDecl) {
	ts.byName[d.Name] = d
	ts.decls = append(ts.decls, d)
}

// namedType returns a named Go type for the schema, declaring one named
// nameHint if the schema is inline. Primitive inline schemas get an alias so
// every message payload has a stable, named type.
func (ts *typeSet) namedType(s *spec.Schema, nameHint string) (string, error) {
	if s != nil && strings.HasPrefix(s.Ref, componentSchemaPrefix) {
		return ts.goType(s, nameHint)
	}
	t, err := ts.goType(s, nameHint)
	if err != nil {
		return "", err
	}
	if _, declared := ts.byName[t]; declared {
		return t, nil
	}
	name := ts.uniqueName(nameHint)
	ts.add(&typeDecl{Name: name, Kind: "alias", Under: t, Doc: schemaDoc(s)})
	return name, nil
}

// goType returns the Go type expression for a schema, declaring named types
// for component schemas, inline objects and enums as needed.
func (ts *typeSet) goType(s *spec.Schema, nameHint string) (string, error) {
	if s == nil {
		return "any", nil
	}
	if s.Ref != "" {
		if !strings.HasPrefix(s.Ref, componentSchemaPrefix) {
			resolved, _, err := ts.doc.ResolveSchema(s)
			if err != nil {
				return "", err
			}
			return ts.goType(resolved, nameHint)
		}
		if name, ok := ts.byRef[s.Ref]; ok {
			return name, nil
		}
		resolved, refName, err := ts.doc.ResolveSchema(s)
		if err != nil {
			return "", err
		}
		name := ts.uniqueName(exportName(refName))
		// Register before descending so recursive schemas terminate.
		ts.byRef[s.Ref] = name
		if err := ts.declare(resolved, name); err != nil {
			return "", err
		}
		return name, nil
	}

	if isMapped(s) {
		return ts.mappedType(s)
	}

	if len(s.OneOf) > 0 || len(s.AnyOf) > 0 || len(s.AllOf) > 0 {
		// TODO(phase 2): composite schemas.
		ts.imports["encoding/json"] = true
		return "json.RawMessage", nil
	}

	switch schemaType(s) {
	case "object":
		if len(s.Properties) == 0 {
			if ap, ok := s.AdditionalProperties.(map[string]any); ok && len(ap) > 0 {
				var apSchema spec.Schema
				if err := remarshal(ap, &apSchema); err != nil {
					return "", err
				}
				elem, err := ts.goType(&apSchema, nameHint+"Value")
				if err != nil {
					return "", err
				}
				return "map[string]" + elem, nil
			}
			return "map[string]any", nil
		}
		name := ts.uniqueName(nameHint)
		if err := ts.declare(s, name); err != nil {
			return "", err
		}
		return name, nil
	case "array":
		elem, err := ts.goType(s.Items, nameHint+"Item")
		if err != nil {
			return "", err
		}
		return "[]" + elem, nil
	case "string":
		if len(s.Enum) > 0 {
			name := ts.uniqueName(nameHint)
			return name, ts.declare(s, name)
		}
		switch s.Format {
		case "date-time":
			ts.imports["time"] = true
			return "time.Time", nil
		case "byte", "binary":
			return "[]byte", nil
		}
		return "string", nil
	case "integer":
		switch s.Format {
		case "int32":
			return "int32", nil
		}
		return "int64", nil
	case "number":
		if s.Format == "float" {
			return "float32", nil
		}
		return "float64", nil
	case "boolean":
		return "bool", nil
	case "":
		return "any", nil
	}
	return "", fmt.Errorf("unsupported schema type %q", s.Type)
}

// declare emits a named type for a resolved (ref-free) schema.
func (ts *typeSet) declare(s *spec.Schema, name string) error {
	d := &typeDecl{Name: name, Doc: schemaDoc(s)}
	// Reserve the name before descending into properties.
	ts.add(d)

	switch {
	case isMapped(s):
		t, err := ts.mappedType(s)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if t == name {
			return fmt.Errorf("%s: x-go-type %q refers to itself: the generated package declares %s, so name a type in another package", name, t, name)
		}
		d.Kind, d.Under = "alias", t
	case schemaType(s) == "object" && len(s.Properties) > 0:
		d.Kind = "struct"
		required := map[string]bool{}
		for _, r := range s.Required {
			required[r] = true
		}
		props := make([]string, 0, len(s.Properties))
		for p := range s.Properties {
			props = append(props, p)
		}
		sort.Strings(props)
		for _, p := range props {
			ps := s.Properties[p]
			t, err := ts.goType(ps, name+exportName(p))
			if err != nil {
				return fmt.Errorf("%s.%s: %w", name, p, err)
			}
			req := required[p]
			if !req && needsPointer(t) {
				t = "*" + t
			}
			d.Fields = append(d.Fields, field{
				Name: exportName(p), JSONName: p, Type: t, Required: req, Doc: schemaDoc(ps),
			})
		}
	case schemaType(s) == "string" && len(s.Enum) > 0:
		d.Kind, d.Under = "enum", "string"
		for _, v := range s.Enum {
			str, ok := v.(string)
			if !ok {
				return fmt.Errorf("%s: non-string enum value %v", name, v)
			}
			d.Values = append(d.Values, str)
		}
	default:
		t, err := ts.goType(s, name+"Value")
		if err != nil {
			return err
		}
		d.Kind, d.Under = "alias", t
	}
	return nil
}

func isMapped(s *spec.Schema) bool {
	return s.GoType != "" || s.GoTypeImport != ""
}

// mappedType returns the Go type named by the x-go-type extension and records
// its import. The rest of the schema is ignored.
func (ts *typeSet) mappedType(s *spec.Schema) (string, error) {
	if s.GoType == "" {
		return "", fmt.Errorf("x-go-type-import %q without x-go-type", s.GoTypeImport)
	}
	expr, err := parser.ParseExpr(s.GoType)
	if err != nil {
		return "", fmt.Errorf("x-go-type %q is not a Go type", s.GoType)
	}
	pkgs := map[string]bool{}
	if !typePackages(expr, pkgs) {
		return "", fmt.Errorf("x-go-type %q is not a Go type", s.GoType)
	}
	if s.GoTypeImport == "" {
		if len(pkgs) > 0 {
			return "", fmt.Errorf("x-go-type %q needs x-go-type-import", s.GoType)
		}
		return s.GoType, nil
	}
	if strings.ContainsAny(s.GoTypeImport, " \t\r\n\"\\`") {
		return "", fmt.Errorf("invalid x-go-type-import %q", s.GoTypeImport)
	}
	if len(pkgs) != 1 {
		return "", fmt.Errorf("x-go-type %q must name exactly one package, the one x-go-type-import %q imports", s.GoType, s.GoTypeImport)
	}
	ts.imports[s.GoTypeImport] = true
	return s.GoType, nil
}

// typePackages adds the package qualifiers used in a type expression to pkgs.
// It reports false if e is not a type expression.
func typePackages(e ast.Expr, pkgs map[string]bool) bool {
	switch e := e.(type) {
	case *ast.Ident:
		return true
	case *ast.SelectorExpr:
		x, ok := e.X.(*ast.Ident)
		if ok {
			pkgs[x.Name] = true
		}
		return ok
	case *ast.StarExpr:
		return typePackages(e.X, pkgs)
	case *ast.ArrayType:
		if _, sized := e.Len.(*ast.BasicLit); e.Len != nil && !sized {
			return false
		}
		return typePackages(e.Elt, pkgs)
	case *ast.MapType:
		return typePackages(e.Key, pkgs) && typePackages(e.Value, pkgs)
	case *ast.IndexExpr:
		return typePackages(e.X, pkgs) && typePackages(e.Index, pkgs)
	case *ast.IndexListExpr:
		ok := typePackages(e.X, pkgs)
		for _, idx := range e.Indices {
			ok = ok && typePackages(idx, pkgs)
		}
		return ok
	}
	return false
}

func schemaType(s *spec.Schema) string {
	if s.Type == "" && len(s.Properties) > 0 {
		return "object"
	}
	return s.Type
}

func schemaDoc(s *spec.Schema) string {
	if s == nil {
		return ""
	}
	if s.Description != "" {
		return s.Description
	}
	return s.Title
}

// needsPointer reports whether an optional field of type t should be a
// pointer to distinguish "absent" from the zero value.
func needsPointer(t string) bool {
	return !strings.HasPrefix(t, "[]") && !strings.HasPrefix(t, "map[") && !strings.HasPrefix(t, "*") &&
		t != "any" && t != "json.RawMessage"
}
