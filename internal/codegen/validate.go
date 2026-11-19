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

// Validation is generated as Go code checking the constraints of a schema on
// the Go value of its type: a Validate method on every struct and enum type,
// and a validateX function for every alias whose schema constrains its values.

// stringFormats are the formats checked on strings: the helper function of
// the template checking each, and the imports it needs.
var stringFormats = map[string]struct {
	helper  string
	imports []string
}{
	"date":  {"isDate", []string{"time"}},
	"email": {"isEmail", []string{"strings"}},
	"ipv4":  {"isIPv4", []string{"net/netip"}},
	"ipv6":  {"isIPv6", []string{"net/netip"}},
	"time":  {"isTime", []string{"time"}},
	"uuid":  {"isUUID", nil},
}

// typeFormats only choose the Go type, so they have nothing to check.
var typeFormats = []string{"binary", "byte", "date-time", "double", "float", "int32", "int64", "password"}

// constraintKeywords are the keywords that constrain values. One that a
// schema has but no generated code checks is reported as not enforced.
var constraintKeywords = []string{
	"additionalItems", "const", "contains", "dependencies", "else", "exclusiveMaximum", "exclusiveMinimum",
	"if", "maxItems", "maxLength", "maxProperties", "maximum", "minItems", "minLength", "minProperties",
	"minimum", "multipleOf", "not", "pattern", "patternProperties", "propertyNames", "then", "uniqueItems",
}

// pattern is a package variable holding a compiled regular expression.
type pattern struct {
	Name   string
	Source string
}

// Literal is the Go literal of the source.
func (p pattern) Literal() string { return strconv.Quote(p.Source) }

// validators generates the checks of the declared types.
type validators struct {
	ts       *typeSet
	helpers  map[string]bool
	patterns []pattern
	bySource map[string]string // regexp source → pattern variable
}

func newValidators(ts *typeSet) *validators {
	return &validators{ts: ts, helpers: map[string]bool{}, bySource: map[string]string{}}
}

// run generates the Validate methods and functions of all declared types.
func (vs *validators) run() error {
	for _, d := range vs.ts.decls {
		var err error
		switch d.Kind {
		case "struct":
			err = vs.structChecks(d)
		case "enum":
			vs.enumChecks(d)
		case "alias":
			_, err = vs.aliasFunc(d)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", d.Name, err)
		}
	}
	return nil
}

// generates reports whether any Validate method or function is generated.
func (vs *validators) generates() bool {
	return slices.ContainsFunc(vs.ts.decls, func(d *typeDecl) bool { return d.Kind != "alias" || d.Func != "" })
}

// use records a helper of the template and the imports it needs.
func (vs *validators) use(helper string, imports ...string) {
	vs.helpers[helper] = true
	for _, imp := range imports {
		vs.ts.imports[imp] = true
	}
}

func (vs *validators) structChecks(d *typeDecl) error {
	for _, f := range d.Fields {
		if f.Required && vs.nillable(f.Type) {
			d.Checks = append(d.Checks, fmt.Sprintf("if m.%s == nil {\np.add(\"/\", \"missing required property %%q\", %q)\n}", f.Name, f.JSONName))
		}
		checks, err := vs.check(f.schema, f.Type, "m."+f.Name, path{}.prop(f.JSONName), d.Name+"."+f.JSONName)
		if err != nil {
			return err
		}
		d.Checks = append(d.Checks, checks...)
	}
	if s := d.schema; s != nil {
		switch ap := s.AdditionalProperties.(type) {
		case bool:
			if !ap {
				vs.ts.warn("%s: additionalProperties is not enforced", d.Name)
			}
		case map[string]any:
			if len(ap) > 0 {
				vs.ts.warn("%s: additionalProperties is not enforced", d.Name)
			}
		}
		vs.unenforced(s, d.Name, nil)
	}
	return nil
}

func (vs *validators) enumChecks(d *typeDecl) {
	seen := map[string]bool{}
	for _, v := range d.Values {
		if !seen[v.Literal] {
			seen[v.Literal] = true
			d.Cases = append(d.Cases, d.Name+v.Name)
		}
	}
	d.OneOf = strconv.Quote("/: must be one of " + fmt.Sprint(d.schema.Enum))
}

// aliasFunc returns the name of the function validating the values of an
// alias, or "" if its schema does not constrain them.
func (vs *validators) aliasFunc(d *typeDecl) (string, error) {
	if d.checked {
		return d.Func, nil
	}
	d.checked = true
	if d.schema == nil || vs.ts.byName[d.Under] != nil {
		// x-go-type, or an alias of a declared type, which validates itself.
		return "", nil
	}
	checks, err := vs.check(d.schema, d.Under, "v", path{}, d.Name)
	if err != nil || len(checks) == 0 {
		return "", err
	}
	d.Checks, d.Func = checks, "validate"+d.Name
	return d.Func, nil
}

// call returns the call validating e, a value of type t, or "" if t is not
// a declared type with constraints. e may dereference a pointer.
func (vs *validators) call(t, e string) (string, error) {
	d := vs.ts.byName[t]
	if d == nil {
		return "", nil
	}
	switch d.Kind {
	case "struct", "enum":
		return strings.TrimPrefix(e, "*") + ".Validate()", nil
	}
	if c, err := vs.call(d.Under, e); c != "" || err != nil {
		return c, err
	}
	fn, err := vs.aliasFunc(d)
	if fn == "" || err != nil {
		return "", err
	}
	return fn + "(" + e + ")", nil
}

// funcExpr returns a func(t) error validating values of the declared type t,
// or "" if nothing constrains them.
func (vs *validators) funcExpr(t string) string {
	d := vs.ts.byName[t]
	for d != nil && d.Kind == "alias" && d.Func == "" {
		d = vs.ts.byName[d.Under]
	}
	switch {
	case d == nil:
		return ""
	case d.Kind == "alias":
		return d.Func
	}
	return d.Name + ".Validate"
}

// nillable reports whether a value of Go type t can be nil, which is how an
// absent required property shows.
func (vs *validators) nillable(t string) bool {
	for d := vs.ts.byName[t]; d != nil && d.Kind == "alias"; d = vs.ts.byName[t] {
		t = d.Under
	}
	return strings.HasPrefix(t, "*") || strings.HasPrefix(t, "[]") || strings.HasPrefix(t, "map[") ||
		t == "any" || t == "json.RawMessage"
}

// comparable reports whether values of Go type t can be compared with ==
// and mean the same JSON value when they are equal.
func (vs *validators) comparable(t string) bool {
	for d := vs.ts.byName[t]; d != nil; d = vs.ts.byName[t] {
		switch d.Kind {
		case "enum":
			return true
		case "struct":
			return false
		}
		t = d.Under
	}
	return slices.Contains([]string{"string", "bool", "int32", "int64", "float32", "float64"}, t)
}

// resolve follows the $refs and allOfs of s to the schema they describe.
func (vs *validators) resolve(s *spec.Schema) (*spec.Schema, error) {
	for range 32 {
		if s == nil {
			return nil, nil
		}
		var err error
		if s, err = s.Unwrap(); err != nil {
			return nil, err
		}
		switch {
		case s.Ref != "":
			if s, _, err = vs.ts.doc.ResolveSchema(s); err != nil {
				return nil, err
			}
		case len(s.AllOf) > 0 && !isMapped(s):
			if s, err = vs.ts.flatten(s, 0); err != nil {
				return nil, err
			}
		default:
			return s, nil
		}
	}
	return nil, fmt.Errorf("$ref and allOf nested too deep (cycle?)")
}

// check returns statements adding the violations of e, a value of Go type t
// generated from schema s, to the violations p, reported at path at. name
// locates s in warnings.
func (vs *validators) check(s *spec.Schema, t, e string, at path, name string) ([]string, error) {
	if inner, ok := strings.CutPrefix(t, "*"); ok {
		body, err := vs.check(s, inner, "*"+e, at, name)
		if err != nil || len(body) == 0 {
			return nil, err
		}
		return []string{"if " + e + " != nil {\n" + strings.Join(body, "\n") + "\n}"}, nil
	}
	call, err := vs.call(t, e)
	if err != nil {
		return nil, err
	}
	if call != "" {
		return []string{"if err := " + call + "; err != nil {\np.nest(" + at.code() + ", err)\n}"}, nil
	}
	if _, declared := vs.ts.byName[t]; declared {
		return nil, nil // a declared type without constraints
	}
	if s, err = vs.resolve(s); s == nil || err != nil || isMapped(s) {
		return nil, err
	}
	c := &checker{vs: vs, s: s, e: e, at: at, name: name, handled: map[string]bool{}}
	switch {
	case t == "[]byte":
	case strings.HasPrefix(t, "[]"):
		err = c.array(t[2:])
	case strings.HasPrefix(t, "map[string]"):
		err = c.object(t[len("map[string]"):])
	case t == "string":
		c.string()
	case slices.Contains([]string{"int32", "int64", "float32", "float64"}, t):
		err = c.number(t)
	case t == "bool":
		c.bool()
	}
	if err != nil {
		return nil, err
	}
	vs.unenforced(s, name, c.handled)
	if c.counted {
		// nil is an absent property, which a required check reports.
		return []string{"if " + e + " != nil {\n" + strings.Join(c.out, "\n") + "\n}"}, nil
	}
	return c.out, nil
}

// unenforced warns about the constraints of s that no generated code checks.
func (vs *validators) unenforced(s *spec.Schema, name string, handled map[string]bool) {
	for _, kw := range constraintKeywords {
		if s.Has(kw) && !handled[kw] {
			vs.ts.warn("%s: %s is not enforced", name, kw)
		}
	}
	if s.Format != "" && !handled["format"] && !slices.Contains(typeFormats, s.Format) {
		vs.ts.warn("%s: format %q is not enforced", name, s.Format)
	}
}

// checker generates the checks of one value.
type checker struct {
	vs      *validators
	s       *spec.Schema
	e       string // the Go expression of the value
	at      path
	name    string
	handled map[string]bool // keywords checked
	out     []string
	// counted says whether the checks count the items or properties of a
	// collection, which are not there when it is nil.
	counted bool
}

// fail adds the statement "if cond { p.add(at, format, args...) }". init,
// if not empty, is a simple statement run before cond.
func (c *checker) fail(init, cond, format string, args ...string) {
	if init != "" {
		init += "; "
	}
	call := "p.add(" + c.at.code() + ", " + strconv.Quote(format)
	for _, a := range args {
		call += ", " + a
	}
	c.out = append(c.out, "if "+init+cond+" {\n"+call+")\n}")
}

func (c *checker) array(elem string) error {
	s := c.s
	if s.MinItems != nil || s.MaxItems != nil {
		c.handled["minItems"], c.handled["maxItems"] = true, true
		c.counts("len("+c.e+")", s.MinItems, s.MaxItems, "items")
	}
	c.handled["uniqueItems"] = true
	if s.UniqueItems {
		if c.vs.comparable(elem) {
			c.vs.use("duplicate")
			c.counted = true
			c.fail("i, j, dup := duplicate("+c.e+")", "dup", "items %d and %d are equal", "i", "j")
		} else {
			c.vs.ts.warn("%s: uniqueItems is not enforced on items of type %s", c.name, elem)
		}
	}
	n := strconv.Itoa(c.at.depth() + 1)
	body, err := c.vs.check(s.Items, elem, "v"+n, c.at.index("strconv.Itoa(i"+n+")"), c.name+"[]")
	if err != nil {
		return err
	}
	if len(body) > 0 {
		c.vs.ts.imports["strconv"] = true
		c.out = append(c.out, "for i"+n+", v"+n+" := range "+c.e+" {\n"+strings.Join(body, "\n")+"\n}")
	}
	return nil
}

func (c *checker) object(elem string) error {
	s := c.s
	if s.MinProperties != nil || s.MaxProperties != nil {
		c.handled["minProperties"], c.handled["maxProperties"] = true, true
		c.counts("len("+c.e+")", s.MinProperties, s.MaxProperties, "properties")
	}
	ap, ok := s.AdditionalProperties.(map[string]any)
	if !ok || len(ap) == 0 {
		return nil
	}
	var apSchema spec.Schema
	if err := remarshal(ap, &apSchema); err != nil {
		return err
	}
	n := strconv.Itoa(c.at.depth() + 1)
	body, err := c.vs.check(&apSchema, elem, "v"+n, c.at.index("pointerToken(k"+n+")"), c.name+"[]")
	if err != nil {
		return err
	}
	if len(body) > 0 {
		c.vs.use("pointerToken", "strings")
		c.out = append(c.out, "for k"+n+", v"+n+" := range "+c.e+" {\n"+strings.Join(body, "\n")+"\n}")
	}
	return nil
}

// counts checks a minimum and maximum count of things, of which there are n.
func (c *checker) counts(n string, lo, hi *int, things string) {
	c.counted = things != "characters"
	var cond []string
	if lo != nil {
		cond = append(cond, fmt.Sprintf("n < %d {\np.add(%s, \"want at least %d %s, got %%d\", n)\n}", *lo, c.at.code(), *lo, things))
	}
	if hi != nil {
		cond = append(cond, fmt.Sprintf("n > %d {\np.add(%s, \"want at most %d %s, got %%d\", n)\n}", *hi, c.at.code(), *hi, things))
	}
	c.out = append(c.out, "if n := "+n+"; "+strings.Join(cond, " else if "))
}

func (c *checker) string() {
	s := c.s
	if s.MinLength != nil || s.MaxLength != nil {
		c.handled["minLength"], c.handled["maxLength"] = true, true
		c.vs.ts.imports["unicode/utf8"] = true
		c.counts("utf8.RuneCountInString("+c.e+")", s.MinLength, s.MaxLength, "characters")
	}
	if s.Pattern != "" {
		c.handled["pattern"] = true
		if _, err := regexp.Compile(s.Pattern); err != nil {
			c.vs.ts.warn("%s: pattern %q is not enforced: Go's regexp package does not support it", c.name, s.Pattern)
		} else {
			v := c.vs.pattern(s.Pattern)
			c.fail("", "!"+v+".MatchString("+c.e+")", "does not match %s", v)
		}
	}
	if f, ok := stringFormats[s.Format]; ok {
		c.handled["format"] = true
		c.vs.use(f.helper, f.imports...)
		c.fail("", "!"+f.helper+"("+c.e+")", "not a valid "+s.Format)
	}
	if str, ok := s.Const.(string); ok {
		c.handled["const"] = true
		c.fail("", c.e+" != "+strconv.Quote(str), "must be %q", strconv.Quote(str))
	}
}

// pattern returns the variable of a compiled regular expression.
func (vs *validators) pattern(source string) string {
	if v, ok := vs.bySource[source]; ok {
		return v
	}
	v := "pattern" + strconv.Itoa(len(vs.patterns)+1)
	vs.bySource[source] = v
	vs.patterns = append(vs.patterns, pattern{Name: v, Source: source})
	vs.ts.imports["regexp"] = true
	return v
}

func (c *checker) number(t string) error {
	s := c.s
	minimum, maximum := s.Minimum, s.Maximum
	exclMin, err := exclusive(s.ExclusiveMinimum, &minimum)
	if err != nil {
		return fmt.Errorf("%s: exclusiveMinimum: %w", c.name, err)
	}
	exclMax, err := exclusive(s.ExclusiveMaximum, &maximum)
	if err != nil {
		return fmt.Errorf("%s: exclusiveMaximum: %w", c.name, err)
	}
	for _, b := range []struct {
		keyword, op, want string
		v                 *float64
	}{
		{"minimum", "<", ">=", minimum},
		{"maximum", ">", "<=", maximum},
		{"exclusiveMinimum", "<=", ">", exclMin},
		{"exclusiveMaximum", ">=", "<", exclMax},
	} {
		if b.v == nil {
			continue
		}
		if err := finite(*b.v); err != nil {
			return fmt.Errorf("%s: %s: %w", c.name, b.keyword, err)
		}
		x, lit := operands(t, c.e, *b.v)
		c.fail("", x+" "+b.op+" "+lit, "must be "+b.want+" "+formatFloat(*b.v))
	}
	// A boolean exclusive bound is checked with the bound next to it.
	for _, kw := range []string{"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"} {
		c.handled[kw] = true
	}
	if m := s.MultipleOf; m != nil {
		c.handled["multipleOf"] = true
		if err := finite(*m); err != nil || *m <= 0 {
			return fmt.Errorf("%s: multipleOf must be a number greater than 0", c.name)
		}
		if x, lit := operands(t, c.e, *m); x == c.e && strings.HasPrefix(t, "int") {
			c.fail("", x+"%"+lit+" != 0", "must be a multiple of "+formatFloat(*m))
		} else {
			c.vs.use("multipleOf", "math")
			c.fail("", "!multipleOf("+x+", "+lit+")", "must be a multiple of "+formatFloat(*m))
		}
	}
	if s.Const != nil {
		if f, ok := number(s.Const); ok {
			c.handled["const"] = true
			x, lit := operands(t, c.e, f)
			c.fail("", x+" != "+lit, "must be "+formatFloat(f))
		}
	}
	return nil
}

func (c *checker) bool() {
	if b, ok := c.s.Const.(bool); ok {
		c.handled["const"] = true
		cond := c.e
		if b {
			cond = "!" + c.e
		}
		c.fail("", cond, "must be "+strconv.FormatBool(b))
	}
}

// operands returns e and the constant b to compare it with. e is converted
// to float64 where b is not a constant of type t.
func operands(t, e string, b float64) (x, lit string) {
	lit = formatFloat(b)
	switch t {
	case "int32", "int64":
		limit := math.Ldexp(1, 31)
		if t == "int64" {
			limit = math.Ldexp(1, 63)
		}
		if b == math.Trunc(b) && b >= -limit && b < limit {
			return e, lit
		}
	case "float64":
		return e, lit
	}
	return "float64(" + e + ")", lit
}

// formatFloat formats f as Go and JSON Schema write it: whole numbers
// without an exponent unless they are huge.
func formatFloat(f float64) string {
	if f == math.Trunc(f) && math.Abs(f) < 1e21 {
		return strconv.FormatFloat(f, 'f', 0, 64) // exact, as -1 may round
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func finite(f float64) error {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return fmt.Errorf("must be finite, got %v", f)
	}
	return nil
}

// number returns a number in the document as a float64.
func number(v any) (float64, bool) {
	switch v := v.(type) {
	case int:
		return float64(v), true
	case float64:
		return v, true
	}
	return 0, false
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
	}
	if f, ok := number(v); ok {
		return &f, nil
	}
	return nil, fmt.Errorf("must be a number, got %v", v)
}

// path is a JSON Pointer into a validated value, as segments of Go code.
type path []segment

// segment is a reference token: a constant, or an expression computing one.
type segment struct{ token, expr string }

func (p path) prop(name string) path {
	return append(slices.Clip(p), segment{token: strings.ReplaceAll(strings.ReplaceAll(name, "~", "~0"), "/", "~1")})
}

func (p path) index(expr string) path { return append(slices.Clip(p), segment{expr: expr}) }

// depth returns the number of computed tokens, which is how many loops the
// value is in.
func (p path) depth() int {
	n := 0
	for _, s := range p {
		if s.expr != "" {
			n++
		}
	}
	return n
}

// code returns the Go expression of the pointer.
func (p path) code() string {
	if len(p) == 0 {
		return `"/"`
	}
	var parts []string
	lit := ""
	for _, s := range p {
		lit += "/" + s.token
		if s.expr != "" {
			parts = append(parts, strconv.Quote(lit), s.expr)
			lit = ""
		}
	}
	if lit != "" {
		parts = append(parts, strconv.Quote(lit))
	}
	return strings.Join(parts, "+")
}
