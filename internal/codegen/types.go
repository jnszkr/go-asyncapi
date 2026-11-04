package codegen

import (
	"fmt"
	"go/ast"
	"go/parser"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

const componentSchemaPrefix = "#/components/schemas/"

// typeDecl is a named Go type generated from a JSON schema.
type typeDecl struct {
	Name    string
	Doc     string
	Kind    string // "struct", "enum", "alias"
	Fields  []field
	Under   string // underlying type for "enum" and "alias"
	Values  []enumValue
	Imports []string
}

// enumValue is a constant of an "enum" type.
type enumValue struct {
	Name    string // appended to the type's name
	Literal string // Go literal
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
	onWarn  func(string)
	warned  map[string]bool
}

func newTypeSet(doc *spec.Document, onWarn func(string)) *typeSet {
	return &typeSet{
		doc:     doc,
		byName:  map[string]*typeDecl{},
		byRef:   map[string]string{},
		imports: map[string]bool{},
		onWarn:  onWarn,
		warned:  map[string]bool{},
	}
}

// warn reports something the generated code cannot express, once.
func (ts *typeSet) warn(format string, args ...any) {
	w := fmt.Sprintf(format, args...)
	if ts.onWarn != nil && !ts.warned[w] {
		ts.onWarn(w)
	}
	ts.warned[w] = true
}

// warnIgnored warns about keywords that have no effect on the generated type.
func (ts *typeSet) warnIgnored(s *spec.Schema, name string) {
	if s.Not != nil {
		ts.warn("%s: not is ignored", name)
	}
	if s.If != nil {
		ts.warn("%s: if is ignored", name)
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
	if s != nil {
		var err error
		if s, err = s.Unwrap(); err != nil {
			return "", err
		}
	}
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
	s, err := s.Unwrap()
	if err != nil {
		return "", err
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

	if len(s.AllOf) > 0 {
		merged, err := ts.flatten(s, 0)
		if err != nil {
			return "", err
		}
		return ts.goType(merged, nameHint)
	}
	ts.warnIgnored(s, nameHint)

	if len(s.OneOf) > 0 || len(s.AnyOf) > 0 {
		// TODO(phase 2): typed unions.
		keyword := "oneOf"
		if len(s.OneOf) == 0 {
			keyword = "anyOf"
		}
		ts.warn("%s: %s is generated as json.RawMessage", nameHint, keyword)
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
	case "integer", "number":
		if len(s.Enum) > 0 {
			name := ts.uniqueName(nameHint)
			return name, ts.declare(s, name)
		}
		return numericType(s), nil
	case "boolean":
		if len(s.Enum) > 0 {
			ts.warn("%s: enum is only generated for strings and numbers", nameHint)
		}
		return "bool", nil
	case "":
		if len(s.Enum) > 0 {
			ts.warn("%s: enum with values of different types is generated as any", nameHint)
		}
		return "any", nil
	}
	return "", fmt.Errorf("unsupported schema type %q", s.Type)
}

// declare emits a named type for a resolved (ref-free) schema.
func (ts *typeSet) declare(s *spec.Schema, name string) error {
	if len(s.AllOf) > 0 && !isMapped(s) {
		merged, err := ts.flatten(s, 0)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		s = merged
	}
	ts.warnIgnored(s, name)
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
		if ap, ok := s.AdditionalProperties.(map[string]any); ok && len(ap) > 0 {
			ts.warn("%s: additionalProperties is ignored on an object with properties", name)
		}
		fields := map[string]bool{}
		for _, p := range props {
			if !validJSONName(p) {
				return fmt.Errorf("%s: property %q cannot be written in a json struct tag", name, p)
			}
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
				Name: uniqueIn(fields, exportName(p)), JSONName: p, Type: t, Required: req, Doc: schemaDoc(ps),
			})
		}
	case len(s.Enum) > 0 && slices.Contains([]string{"string", "integer", "number"}, schemaType(s)):
		d.Kind, d.Under = "enum", "string"
		if schemaType(s) != "string" {
			d.Under = numericType(s)
		}
		values := map[string]bool{}
		for _, v := range s.Enum {
			lit, ok := enumLiteral(schemaType(s), v)
			if !ok {
				return fmt.Errorf("%s: enum value %v is not of type %s", name, v, schemaType(s))
			}
			d.Values = append(d.Values, enumValue{Name: uniqueIn(values, exportName(fmt.Sprint(v))), Literal: lit})
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

// flatten merges the members of an allOf into one schema. An allOf of a
// single member and nothing else is that member, so a $ref keeps its type.
func (ts *typeSet) flatten(s *spec.Schema, depth int) (*spec.Schema, error) {
	if depth == 32 {
		return nil, fmt.Errorf("allOf nested too deep (cycle?)")
	}
	bare := s.Type == "" && len(s.Properties) == 0 && len(s.Required) == 0 && s.Items == nil &&
		len(s.Enum) == 0 && s.Const == nil && len(s.OneOf) == 0 && len(s.AnyOf) == 0 && s.AdditionalProperties == nil
	if len(s.AllOf) == 1 && bare {
		return s.AllOf[0], nil
	}
	out := *s
	out.AllOf = nil
	out.Properties = maps.Clone(s.Properties)
	for _, m := range s.AllOf {
		member, _, err := ts.doc.ResolveSchema(m)
		if err != nil {
			return nil, err
		}
		for len(member.AllOf) > 0 {
			if member, err = ts.flatten(member, depth+1); err != nil {
				return nil, err
			}
			if member, _, err = ts.doc.ResolveSchema(member); err != nil {
				return nil, err
			}
		}
		if isMapped(member) {
			return nil, fmt.Errorf("allOf member with x-go-type cannot be merged")
		}
		if err := mergeSchema(&out, member); err != nil {
			return nil, err
		}
	}
	return &out, nil
}

// mergeSchema adds an allOf member to out. A member can only narrow a schema,
// so keywords that differ cannot both be expressed by one Go type.
func mergeSchema(out, m *spec.Schema) error {
	for _, kw := range []struct {
		name     string
		dst, src any
		set      func()
	}{
		{"type", out.Type, m.Type, func() { out.Type = m.Type }},
		{"format", out.Format, m.Format, func() { out.Format = m.Format }},
		{"items", out.Items, m.Items, func() { out.Items = m.Items }},
		{"enum", out.Enum, m.Enum, func() { out.Enum = m.Enum }},
		{"const", out.Const, m.Const, func() { out.Const = m.Const }},
		{"additionalProperties", out.AdditionalProperties, m.AdditionalProperties, func() { out.AdditionalProperties = m.AdditionalProperties }},
	} {
		switch {
		case isZero(kw.src):
		case isZero(kw.dst):
			kw.set()
		case !reflect.DeepEqual(kw.dst, kw.src):
			return fmt.Errorf("allOf: conflicting %s: %v and %v", kw.name, kw.dst, kw.src)
		}
	}
	for k, v := range m.Properties {
		if have, ok := out.Properties[k]; ok && !reflect.DeepEqual(have, v) {
			return fmt.Errorf("allOf: conflicting definitions of property %q", k)
		}
		if out.Properties == nil {
			out.Properties = map[string]*spec.Schema{}
		}
		out.Properties[k] = v
	}
	out.Required = append(out.Required, m.Required...)
	out.OneOf = append(out.OneOf, m.OneOf...)
	out.AnyOf = append(out.AnyOf, m.AnyOf...)
	if out.Not == nil {
		out.Not = m.Not
	}
	if out.If == nil {
		out.If = m.If
	}
	if out.Description == "" {
		out.Description = m.Description
	}
	if out.Title == "" {
		out.Title = m.Title
	}
	return nil
}

func isZero(v any) bool {
	return v == nil || reflect.ValueOf(v).IsZero()
}

// numericType returns the Go type of an integer or number schema.
func numericType(s *spec.Schema) string {
	switch {
	case schemaType(s) == "integer" && s.Format == "int32":
		return "int32"
	case schemaType(s) == "integer":
		return "int64"
	case s.Format == "float":
		return "float32"
	}
	return "float64"
}

// enumLiteral returns the Go literal of an enum value of the given type.
func enumLiteral(typ string, v any) (string, bool) {
	switch v := v.(type) {
	case string:
		return strconv.Quote(v), typ == "string"
	case int, int64, uint64:
		return fmt.Sprint(v), typ == "integer" || typ == "number"
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), typ == "number"
	}
	return "", false
}

// literalType returns the schema type of a value in a document, or "".
func literalType(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case int, int64, uint64:
		return "integer"
	case float64:
		return "number"
	case bool:
		return "boolean"
	}
	return ""
}

// uniqueIn returns name, with a numeric suffix if it is already in used, and
// adds the result to used.
func uniqueIn(used map[string]bool, name string) string {
	unique := name
	for i := 2; used[unique]; i++ {
		unique = name + strconv.Itoa(i)
	}
	used[unique] = true
	return unique
}

// validJSONName reports whether encoding/json honours name in a struct tag.
func validJSONName(name string) bool {
	if name == "" || name == "-" {
		return false
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", r) {
			return false
		}
	}
	return true
}

// breakRecursion makes fields pointers where structs would otherwise contain
// themselves, which Go does not allow even when the field is required.
func breakRecursion(decls []*typeDecl) {
	byName := map[string]*typeDecl{}
	for _, d := range decls {
		byName[d.Name] = d
	}
	target := func(t string) *typeDecl {
		d := byName[t]
		for range decls {
			if d == nil || d.Kind != "alias" {
				break
			}
			d = byName[d.Under]
		}
		return d
	}
	const visiting, done = 1, 2
	state := map[*typeDecl]int{}
	var visit func(d *typeDecl)
	visit = func(d *typeDecl) {
		state[d] = visiting
		for i := range d.Fields {
			f := &d.Fields[i]
			t := target(f.Type)
			if t == nil || t.Kind != "struct" {
				continue
			}
			switch state[t] {
			case visiting:
				f.Type = "*" + f.Type
			case 0:
				visit(t)
			}
		}
		state[d] = done
	}
	for _, d := range decls {
		if state[d] == 0 {
			visit(d)
		}
	}
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
	switch {
	case s.Type != "":
		return s.Type
	case len(s.Properties) > 0:
		return "object"
	case s.Const != nil:
		return literalType(s.Const)
	case len(s.Enum) > 0:
		t := literalType(s.Enum[0])
		for _, v := range s.Enum {
			if literalType(v) != t {
				return ""
			}
		}
		return t
	}
	return ""
}

func schemaDoc(s *spec.Schema) string {
	if s == nil {
		return ""
	}
	doc := s.Description
	if doc == "" {
		doc = s.Title
	}
	if s.Const != nil {
		value := fmt.Sprint(s.Const)
		if str, ok := s.Const.(string); ok {
			value = strconv.Quote(str)
		}
		doc = strings.TrimSpace(doc + "\nMust be " + value + ".")
	}
	return doc
}

// needsPointer reports whether an optional field of type t should be a
// pointer to distinguish "absent" from the zero value.
func needsPointer(t string) bool {
	return !strings.HasPrefix(t, "[]") && !strings.HasPrefix(t, "map[") && !strings.HasPrefix(t, "*") &&
		t != "any" && t != "json.RawMessage"
}
