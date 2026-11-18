package codegen

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

// schemaVar is a package variable holding a compiled schema: one per $ref
// target and per inline message payload. The variables are declared empty
// and filled in init, so a $ref is a pointer and a recursive schema is a
// pointer cycle.
type schemaVar struct {
	Name    string
	Literal string // a schema{...} composite literal
}

// enforcedFormats are the formats the generated validator checks. The
// template's schemaFormats map has exactly these keys.
var enforcedFormats = []string{"date", "date-time", "email", "ipv4", "ipv6", "time", "uuid"}

// typeFormats only choose the Go type, so they have nothing to enforce.
var typeFormats = []string{"binary", "byte", "double", "float", "int32", "int64", "password"}

// unenforcedKeywords are validation keywords the generated validator does not
// check.
var unenforcedKeywords = []string{"additionalItems", "contains", "dependencies", "patternProperties", "propertyNames"}

// schemaSet compiles the schemas of message payloads into Go literals.
type schemaSet struct {
	doc   *spec.Document
	warn  func(format string, args ...any)
	vars  []*schemaVar
	byRef map[string]*schemaVar // canonical $ref → variable
	names map[string]bool
}

func newSchemaSet(doc *spec.Document, warn func(format string, args ...any)) *schemaSet {
	return &schemaSet{doc: doc, warn: warn, byRef: map[string]*schemaVar{}, names: map[string]bool{}}
}

// payload returns the variable holding the schema of a message payload, or
// "" for a message without one.
func (ss *schemaSet) payload(s *spec.Schema, msgName string) (string, error) {
	if s == nil {
		return "", nil
	}
	s, err := s.Unwrap()
	if err != nil {
		return "", err
	}
	if s.Ref != "" {
		return ss.ref(s.Ref)
	}
	v := &schemaVar{Name: uniqueIn(ss.names, "schema"+msgName)}
	ss.vars = append(ss.vars, v)
	body, err := ss.fields(s, msgName)
	if err != nil {
		return "", err
	}
	v.Literal = "schema" + body
	return v.Name, nil
}

// ref returns the variable of the schema a $ref points to, declaring it on
// first use.
func (ss *schemaSet) ref(ref string) (string, error) {
	canonical, err := ss.doc.CanonicalRef(ref)
	if err != nil {
		return "", err
	}
	if v, ok := ss.byRef[canonical]; ok {
		return v.Name, nil
	}
	resolved, _, err := ss.doc.ResolveSchema(&spec.Schema{Ref: canonical})
	if err != nil {
		return "", err
	}
	v := &schemaVar{Name: uniqueIn(ss.names, "schema"+exportName(spec.RefName(canonical)))}
	// Register before descending so recursive schemas terminate.
	ss.byRef[canonical] = v
	ss.vars = append(ss.vars, v)
	body, err := ss.fields(resolved, canonical)
	if err != nil {
		return "", err
	}
	v.Literal = "schema" + body
	return v.Name, nil
}

// expr returns the Go expression of a subschema: the variable of a $ref, or
// an inline literal without its type, as composite literals in a slice or map
// of *schema may leave it out.
func (ss *schemaSet) expr(s *spec.Schema, at string) (string, error) {
	s, err := s.Unwrap()
	if err != nil {
		return "", err
	}
	if s.Ref != "" {
		return ss.ref(s.Ref)
	}
	return ss.fields(s, at)
}

// pointer is expr for a field of type *schema, where an inline literal needs
// its type.
func (ss *schemaSet) pointer(s *spec.Schema, at string) (string, error) {
	e, err := ss.expr(s, at)
	if err != nil || strings.HasPrefix(e, "schema") {
		return e, err
	}
	return "&schema" + e, nil
}

// fields returns the braced fields of the schema literal for a schema
// without $ref. at names the schema in warnings.
func (ss *schemaSet) fields(s *spec.Schema, at string) (string, error) {
	var fs []string
	add := func(name, value string) { fs = append(fs, name+": "+value) }
	sub := func(name string, s *spec.Schema, path string) error {
		if s == nil {
			return nil
		}
		e, err := ss.pointer(s, at+"/"+path)
		if err != nil {
			return err
		}
		add(name, e)
		return nil
	}
	list := func(name string, ss2 []*spec.Schema) error {
		if len(ss2) == 0 {
			return nil
		}
		elems := make([]string, len(ss2))
		for i, m := range ss2 {
			e, err := ss.expr(m, fmt.Sprintf("%s/%s/%d", at, name, i))
			if err != nil {
				return err
			}
			elems[i] = e
		}
		add(exportName(name), "[]*schema{\n"+strings.Join(elems, ",\n")+",\n}")
		return nil
	}

	for _, kw := range unenforcedKeywords {
		if s.Has(kw) {
			ss.warn("%s: %s is not enforced", at, kw)
		}
	}

	if s.Type != "" {
		add("Type", strconv.Quote(s.Type))
	}
	if s.Has("enum") {
		lits := make([]string, len(s.Enum))
		for i, v := range s.Enum {
			lit, err := jsonLiteral(v)
			if err != nil {
				return "", fmt.Errorf("%s: enum: %w", at, err)
			}
			lits[i] = lit
		}
		add("Enum", "[]any{"+strings.Join(lits, ", ")+"}")
	}
	if s.Has("const") {
		lit, err := jsonLiteral(s.Const)
		if err != nil {
			return "", fmt.Errorf("%s: const: %w", at, err)
		}
		add("Const", lit)
		add("HasConst", "true")
	}

	// Objects.
	if len(s.Required) > 0 {
		quoted := make([]string, len(s.Required))
		for i, r := range s.Required {
			quoted[i] = strconv.Quote(r)
		}
		add("Required", "[]string{"+strings.Join(quoted, ", ")+"}")
	}
	if len(s.Properties) > 0 {
		names := sortedKeys(s.Properties)
		entries := make([]string, len(names))
		for i, name := range names {
			e, err := ss.expr(s.Properties[name], at+"/properties/"+name)
			if err != nil {
				return "", err
			}
			entries[i] = strconv.Quote(name) + ": " + e
		}
		add("Properties", "map[string]*schema{\n"+strings.Join(entries, ",\n")+",\n}")
	}
	switch ap := s.AdditionalProperties.(type) {
	case bool:
		if !ap && !s.Has("patternProperties") {
			add("NoAdditional", "true")
		}
	case map[string]any:
		if len(ap) > 0 && !s.Has("patternProperties") {
			var apSchema spec.Schema
			if err := remarshal(ap, &apSchema); err != nil {
				return "", err
			}
			if err := sub("Additional", &apSchema, "additionalProperties"); err != nil {
				return "", err
			}
		}
	}
	if s.AdditionalProperties != nil && s.Has("patternProperties") {
		ss.warn("%s: additionalProperties next to patternProperties is not enforced", at)
	}
	count(add, "MinProperties", s.MinProperties)
	count(add, "MaxProperties", s.MaxProperties)

	// Arrays.
	if err := sub("Items", s.Items, "items"); err != nil {
		return "", err
	}
	count(add, "MinItems", s.MinItems)
	count(add, "MaxItems", s.MaxItems)
	if s.UniqueItems {
		add("UniqueItems", "true")
	}

	// Strings.
	count(add, "MinLength", s.MinLength)
	count(add, "MaxLength", s.MaxLength)
	if s.Pattern != "" {
		if _, err := regexp.Compile(s.Pattern); err != nil {
			ss.warn("%s: pattern %q is not enforced: Go's regexp package does not support it", at, s.Pattern)
		} else {
			add("Pattern", "regexp.MustCompile("+strconv.Quote(s.Pattern)+")")
		}
	}
	switch {
	case s.Format == "":
	case slices.Contains(enforcedFormats, s.Format):
		add("Format", strconv.Quote(s.Format))
	case !slices.Contains(typeFormats, s.Format):
		ss.warn("%s: format %q is not enforced", at, s.Format)
	}

	// Numbers.
	minimum, maximum := s.Minimum, s.Maximum
	exclMin, err := exclusive(s.ExclusiveMinimum, &minimum)
	if err != nil {
		return "", fmt.Errorf("%s: exclusiveMinimum: %w", at, err)
	}
	exclMax, err := exclusive(s.ExclusiveMaximum, &maximum)
	if err != nil {
		return "", fmt.Errorf("%s: exclusiveMaximum: %w", at, err)
	}
	for _, b := range []struct {
		name string
		v    *float64
	}{
		{"Minimum", minimum}, {"Maximum", maximum},
		{"ExclusiveMinimum", exclMin}, {"ExclusiveMaximum", exclMax},
		{"MultipleOf", s.MultipleOf},
	} {
		if b.v == nil {
			continue
		}
		lit, err := floatLiteral(*b.v)
		if err != nil {
			return "", fmt.Errorf("%s: %s: %w", at, strings.ToLower(b.name[:1])+b.name[1:], err)
		}
		add(b.name, "bound("+lit+")")
	}
	if s.MultipleOf != nil && *s.MultipleOf <= 0 {
		return "", fmt.Errorf("%s: multipleOf must be greater than 0", at)
	}

	// Combinators.
	for _, l := range []struct {
		name string
		ss   []*spec.Schema
	}{{"allOf", s.AllOf}, {"anyOf", s.AnyOf}, {"oneOf", s.OneOf}} {
		if err := list(l.name, l.ss); err != nil {
			return "", err
		}
	}
	for _, c := range []struct {
		name, path string
		s          *spec.Schema
	}{{"Not", "not", s.Not}, {"If", "if", s.If}, {"Then", "then", s.Then}, {"Else", "else", s.Else}} {
		if err := sub(c.name, c.s, c.path); err != nil {
			return "", err
		}
	}

	switch len(fs) {
	case 0:
		return "{}", nil
	case 1:
		if !strings.Contains(fs[0], "\n") {
			return "{" + fs[0] + "}", nil
		}
	}
	return "{\n" + strings.Join(fs, ",\n") + ",\n}", nil
}

// count adds a field holding a *int.
func count(add func(name, value string), name string, n *int) {
	if n != nil {
		add(name, "bound("+strconv.Itoa(*n)+")")
	}
}

// exclusive returns the bound of an exclusiveMinimum or exclusiveMaximum. A
// boolean (draft-04) makes the inclusive bound exclusive instead.
func exclusive(v any, inclusive **float64) (*float64, error) {
	switch v := v.(type) {
	case nil:
		return nil, nil
	case bool:
		if !v {
			return nil, nil
		}
		if *inclusive == nil {
			return nil, fmt.Errorf("true needs a bound next to it")
		}
		b := *inclusive
		*inclusive = nil
		return b, nil
	case int:
		f := float64(v)
		return &f, nil
	case float64:
		return &v, nil
	}
	return nil, fmt.Errorf("must be a number, got %v", v)
}

// floatLiteral returns an untyped float constant for f, so that bound infers
// float64.
func floatLiteral(f float64) (string, error) {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return "", fmt.Errorf("must be finite, got %v", f)
	}
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, ".e") {
		s += ".0"
	}
	return s, nil
}

// jsonLiteral returns the Go literal of a value in the document as
// encoding/json decodes it into any: numbers as float64, objects as
// map[string]any and arrays as []any.
func jsonLiteral(v any) (string, error) {
	switch v := v.(type) {
	case nil:
		return "nil", nil
	case bool:
		return strconv.FormatBool(v), nil
	case string:
		return strconv.Quote(v), nil
	case int:
		return floatLiteral(float64(v))
	case float64:
		return floatLiteral(v)
	case []any:
		elems := make([]string, len(v))
		for i, e := range v {
			lit, err := jsonLiteral(e)
			if err != nil {
				return "", err
			}
			elems[i] = lit
		}
		return "[]any{" + strings.Join(elems, ", ") + "}", nil
	case map[string]any:
		keys := sortedKeys(v)
		entries := make([]string, len(keys))
		for i, k := range keys {
			lit, err := jsonLiteral(v[k])
			if err != nil {
				return "", err
			}
			entries[i] = strconv.Quote(k) + ": " + lit
		}
		return "map[string]any{" + strings.Join(entries, ", ") + "}", nil
	}
	return "", fmt.Errorf("%v (%T) is not a JSON value", v, v)
}
