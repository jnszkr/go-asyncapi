package codegen

import (
	"slices"
	"strings"
	"testing"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

// models builds the model of src and returns its types by name, with the
// warnings.
func models(t *testing.T, src string) (map[string]*typeDecl, *model, []string, error) {
	t.Helper()
	doc := mustParse(t, src)
	var warnings []string
	m, err := buildModel(doc, "p", func(w string) { warnings = append(warnings, w) })
	if err != nil {
		return nil, nil, warnings, err
	}
	byName := map[string]*typeDecl{}
	for _, d := range m.Types {
		byName[d.Name] = d
	}
	return byName, m, warnings, nil
}

func mustParse(t *testing.T, src string) *spec.Document {
	t.Helper()
	doc, err := spec.Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

// checks returns the checks generated for the payload M of payloadDoc,
// followed by more of the document.
func checks(t *testing.T, payload string, more ...string) string {
	t.Helper()
	decls, _, warnings, err := models(t, payloadDoc(payload)+strings.Join(more, ""))
	if err != nil {
		t.Fatalf("%s: %v", payload, err)
	}
	if len(warnings) > 0 {
		t.Errorf("%s: unexpected warnings %q", payload, warnings)
	}
	return strings.Join(decls["M"].Checks, "\n")
}

// fails is the code of a check of value v that adds a problem at "/".
func fails(cond, problem string) string {
	return "if " + cond + " {\np.add(\"/\", " + problem + ")\n}"
}

func TestScalarChecks(t *testing.T) {
	tests := []struct{ payload, want string }{
		{`{type: string}`, ""},
		{`{type: string, minLength: 1}`, "if n := utf8.RuneCountInString(v); n < 1 {\np.add(\"/\", \"want at least 1 characters, got %d\", n)\n}"},
		{`{type: string, maxLength: 3}`, "if n := utf8.RuneCountInString(v); n > 3 {\np.add(\"/\", \"want at most 3 characters, got %d\", n)\n}"},
		{`{type: string, minLength: 1, maxLength: 3}`, "if n := utf8.RuneCountInString(v); n < 1 {\np.add(\"/\", \"want at least 1 characters, got %d\", n)\n} else if n > 3 {\np.add(\"/\", \"want at most 3 characters, got %d\", n)\n}"},
		{`{type: string, pattern: '^a\d$'}`, fails("!pattern1.MatchString(v)", `"does not match %s", pattern1`)},
		{`{type: string, format: date}`, fails("!isDate(v)", `"not a valid date"`)},
		{`{type: string, format: time}`, fails("!isTime(v)", `"not a valid time"`)},
		{`{type: string, format: email}`, fails("!isEmail(v)", `"not a valid email"`)},
		{`{type: string, format: ipv4}`, fails("!isIPv4(v)", `"not a valid ipv4"`)},
		{`{type: string, format: ipv6}`, fails("!isIPv6(v)", `"not a valid ipv6"`)},
		{`{type: string, format: uuid}`, fails("!isUUID(v)", `"not a valid uuid"`)},
		{`{type: string, format: password}`, ""},
		{`{const: a}`, fails(`v != "a"`, `"must be %q", "a"`)},
		{`{type: integer, minimum: 1}`, fails("v < 1", `"must be >= 1"`)},
		{`{type: integer, maximum: 1}`, fails("v > 1", `"must be <= 1"`)},
		{`{type: integer, exclusiveMinimum: 1}`, fails("v <= 1", `"must be > 1"`)},
		{`{type: integer, exclusiveMaximum: 1}`, fails("v >= 1", `"must be < 1"`)},
		{`{type: integer, minimum: 1, exclusiveMinimum: true}`, fails("v <= 1", `"must be > 1"`)},
		{`{type: integer, maximum: 1, exclusiveMaximum: true}`, fails("v >= 1", `"must be < 1"`)},
		{`{type: integer, minimum: 1, exclusiveMinimum: false}`, fails("v < 1", `"must be >= 1"`)},
		{`{type: integer, minimum: 1.5}`, fails("float64(v) < 1.5", `"must be >= 1.5"`)},
		{`{type: integer, minimum: -1e21}`, fails("float64(v) < -1e+21", `"must be >= -1e+21"`)},
		{`{type: integer, minimum: 1e6}`, fails("v < 1000000", `"must be >= 1000000"`)},
		{`{type: number, minimum: 1.5e-7}`, fails("v < 1.5e-07", `"must be >= 1.5e-07"`)},
		{`{type: integer, maximum: 9223372036854775807}`, fails("float64(v) > 9223372036854775808", `"must be <= 9223372036854775808"`)},
		{`{type: integer, minimum: -9223372036854775808}`, fails("v < -9223372036854775808", `"must be >= -9223372036854775808"`)},
		{`{type: integer, format: int32, maximum: 2147483647}`, fails("v > 2147483647", `"must be <= 2147483647"`)},
		{`{type: integer, format: int32, maximum: 2147483648}`, fails("float64(v) > 2147483648", `"must be <= 2147483648"`)},
		{`{type: integer, format: int32, minimum: -2147483648}`, fails("v < -2147483648", `"must be >= -2147483648"`)},
		{`{type: integer, format: int32, minimum: -2147483649}`, fails("float64(v) < -2147483649", `"must be >= -2147483649"`)},
		{`{type: number, minimum: 0.5}`, fails("v < 0.5", `"must be >= 0.5"`)},
		{`{type: number, format: float, minimum: 0.5}`, fails("float64(v) < 0.5", `"must be >= 0.5"`)},
		{`{type: integer, multipleOf: 3}`, fails("v%3 != 0", `"must be a multiple of 3"`)},
		{`{type: integer, multipleOf: 0.5}`, fails("!multipleOf(float64(v), 0.5)", `"must be a multiple of 0.5"`)},
		{`{type: number, multipleOf: 3}`, fails("!multipleOf(v, 3)", `"must be a multiple of 3"`)},
		{`{type: number, format: float, multipleOf: 3}`, fails("!multipleOf(float64(v), 3)", `"must be a multiple of 3"`)},
		{`{type: integer, const: 3}`, fails("v != 3", `"must be 3"`)},
		{`{type: number, const: 1.5}`, fails("v != 1.5", `"must be 1.5"`)},
		{`{const: true}`, fails("!v", `"must be true"`)},
		{`{const: false}`, fails("v", `"must be false"`)},
		{`{type: integer, minimum: 1, maximum: 9}`, fails("v < 1", `"must be >= 1"`) + "\n" + fails("v > 9", `"must be <= 9"`)},
		{`{allOf: [{type: string}, {minLength: 2}]}`, "if n := utf8.RuneCountInString(v); n < 2 {\np.add(\"/\", \"want at least 2 characters, got %d\", n)\n}"},
	}
	for _, tt := range tests {
		if got := checks(t, tt.payload); got != tt.want {
			t.Errorf("%s:\n%s\nwant\n%s", tt.payload, got, tt.want)
		}
	}
}

func TestCollectionChecks(t *testing.T) {
	tests := []struct{ payload, want string }{
		{`{type: array, items: {type: string}}`, ""},
		{`{type: array, minItems: 1}`, "if v != nil {\nif n := len(v); n < 1 {\np.add(\"/\", \"want at least 1 items, got %d\", n)\n}\n}"},
		{`{type: array, maxItems: 2}`, "if v != nil {\nif n := len(v); n > 2 {\np.add(\"/\", \"want at most 2 items, got %d\", n)\n}\n}"},
		{`{type: array, uniqueItems: true, items: {type: integer}}`, "if v != nil {\nif i, j, dup := duplicate(v); dup {\np.add(\"/\", \"items %d and %d are equal\", i, j)\n}\n}"},
		{`{type: array, uniqueItems: false, items: {type: integer}}`, ""},
		{`{type: array, items: {type: string, minLength: 1}}`, "for i1, v1 := range v {\nif n := utf8.RuneCountInString(v1); n < 1 {\np.add(\"/\"+strconv.Itoa(i1), \"want at least 1 characters, got %d\", n)\n}\n}"},
		{`{type: array, items: {type: array, items: {type: integer, minimum: 0}}}`, "for i1, v1 := range v {\nfor i2, v2 := range v1 {\nif v2 < 0 {\np.add(\"/\"+strconv.Itoa(i1)+\"/\"+strconv.Itoa(i2), \"must be >= 0\")\n}\n}\n}"},
		{`{type: object, minProperties: 1}`, "if v != nil {\nif n := len(v); n < 1 {\np.add(\"/\", \"want at least 1 properties, got %d\", n)\n}\n}"},
		{`{type: object, maxProperties: 1}`, "if v != nil {\nif n := len(v); n > 1 {\np.add(\"/\", \"want at most 1 properties, got %d\", n)\n}\n}"},
		{`{type: object, additionalProperties: {type: integer}}`, ""},
		{`{type: object, additionalProperties: {type: integer, minimum: 0}}`, "for k1, v1 := range v {\nif v1 < 0 {\np.add(\"/\"+pointerToken(k1), \"must be >= 0\")\n}\n}"},
		{`{type: array, items: {$ref: '#/components/schemas/S'}}`, "for i1, v1 := range v {\nif err := v1.Validate(); err != nil {\np.nest(\"/\"+strconv.Itoa(i1), err)\n}\n}"},
	}
	for _, tt := range tests {
		got := checks(t, tt.payload, "components: {schemas: {S: {type: object, properties: {a: {type: string}}}}}")
		if got != tt.want {
			t.Errorf("%s:\n%s\nwant\n%s", tt.payload, got, tt.want)
		}
	}
}

func TestStructChecks(t *testing.T) {
	decls, m, warnings, err := models(t, head+`
components:
  schemas:
    S:
      type: object
      required: [list, req, tags, map, any, e, ref, n]
      properties:
        list: {type: array, items: {type: string}}
        opt: {type: string, minLength: 1}
        req: {type: string, minLength: 1}
        tags: {$ref: '#/components/schemas/Tags'}
        map: {type: object, additionalProperties: {type: string}}
        any: {}
        a/b~c: {type: integer, minimum: 0}
        e: {$ref: '#/components/schemas/E'}
        optE: {$ref: '#/components/schemas/E'}
        ref: {$ref: '#/components/schemas/T'}
        n: {$ref: '#/components/schemas/Name'}
        rec: {$ref: '#/components/schemas/S'}
        validate: {type: string}
    T: {type: object, properties: {x: {type: string}}}
    Tags: {type: array, maxItems: 1, items: {$ref: '#/components/schemas/Name'}}
    Name: {type: string}
    E: {enum: [a, b, a]}
    F: {type: number, enum: [1, 1.0, 2.5]}
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) > 0 {
		t.Errorf("unexpected warnings %q", warnings)
	}
	got := strings.Join(decls["S"].Checks, "\n")
	for _, c := range []string{
		"if m.ABC != nil {\nif *m.ABC < 0 {\np.add(\"/a~1b~0c\", \"must be >= 0\")\n}\n}",
		"if m.Any == nil {\np.add(\"/\", \"missing required property %q\", \"any\")\n}",
		"if err := m.E.Validate(); err != nil {\np.nest(\"/e\", err)\n}",
		"if m.List == nil {\np.add(\"/\", \"missing required property %q\", \"list\")\n}",
		"if m.Map == nil {\np.add(\"/\", \"missing required property %q\", \"map\")\n}",
		"if m.Opt != nil {\nif n := utf8.RuneCountInString(*m.Opt); n < 1 {\np.add(\"/opt\", \"want at least 1 characters, got %d\", n)\n}\n}",
		"if m.OptE != nil {\nif err := m.OptE.Validate(); err != nil {\np.nest(\"/optE\", err)\n}\n}",
		"if m.Rec != nil {\nif err := m.Rec.Validate(); err != nil {\np.nest(\"/rec\", err)\n}\n}",
		"if err := m.Ref.Validate(); err != nil {\np.nest(\"/ref\", err)\n}",
		"if n := utf8.RuneCountInString(m.Req); n < 1 {\np.add(\"/req\", \"want at least 1 characters, got %d\", n)\n}",
		"if m.Tags == nil {\np.add(\"/\", \"missing required property %q\", \"tags\")\n}",
		"if err := validateTags(m.Tags); err != nil {\np.nest(\"/tags\", err)\n}",
	} {
		if !strings.Contains(got, c) {
			t.Errorf("S checks do not contain\n%s\nin\n%s", c, got)
		}
	}
	for _, absent := range []string{"m.N ", "m.N.", "m.Req ==", "m.Ref ==", "m.E ==", "m.N ==", "m.Validate2"} {
		if strings.Contains(got, absent) {
			t.Errorf("S checks contain %q:\n%s", absent, got)
		}
	}
	if !slices.ContainsFunc(decls["S"].Fields, func(f field) bool { return f.Name == "Validate2" && f.JSONName == "validate" }) {
		t.Errorf("property validate is not field Validate2: %+v", decls["S"].Fields)
	}
	if got := decls["T"].Checks; got != nil {
		t.Errorf("T checks = %q", got)
	}
	if d := decls["Name"]; d.Func != "" || d.Checks != nil {
		t.Errorf("Name has a validator: %+v", d)
	}
	if d := decls["Tags"]; d.Func != "validateTags" {
		t.Errorf("Tags func = %q", d.Func)
	}
	if d := decls["E"]; !slices.Equal(d.Cases, []string{"EA", "EB"}) || d.OneOf != `"/: must be one of [a b a]"` {
		t.Errorf("E cases = %q, one of %s", d.Cases, d.OneOf)
	}
	if d := decls["F"]; !slices.Equal(d.Cases, []string{"FX1", "FX25"}) || d.OneOf != `"/: must be one of [1 1 2.5]"` {
		t.Errorf("F cases = %q, one of %s", d.Cases, d.OneOf)
	}
	if !m.Validates || !slices.Contains(m.Imports, "unicode/utf8") || !slices.Contains(m.Imports, "fmt") {
		t.Errorf("validates = %v, imports %q", m.Validates, m.Imports)
	}
}

func TestValidatorOfPayload(t *testing.T) {
	tests := []struct{ payload, validator, check string }{
		{`{type: object, properties: {a: {type: string}}}`, "M.Validate", "msg.Validate()"},
		{`{enum: [a]}`, "M.Validate", "msg.Validate()"},
		{`{type: string, minLength: 1}`, "validateM", "validateM(msg)"},
		{`{type: string}`, "", ""},
		{`{$ref: '#/components/schemas/Name'}`, "", ""},
		{`{$ref: '#/components/schemas/Short'}`, "validateShort", "validateShort(msg)"},
		{`{type: array, items: {$ref: '#/components/schemas/S'}}`, "validateM", "validateM(msg)"},
		{`{x-go-type: int}`, "", ""},
		// An alias of a struct validates as the struct.
		{`{$ref: '#/components/schemas/A'}`, "S.Validate", "msg.Validate()"},
	}
	for _, tt := range tests {
		_, m, _, err := models(t, payloadDoc(tt.payload)+`
components:
  schemas:
    Name: {type: string}
    Short: {type: string, maxLength: 1}
    S: {type: object, properties: {a: {type: string}}}
    A: {allOf: [{$ref: '#/components/schemas/S'}]}
`)
		if err != nil {
			t.Fatal(err)
		}
		msg := m.Ops[0].Message
		if msg.Validator != tt.validator || msg.Check("msg") != tt.check {
			t.Errorf("%s: validator %q, check %q; want %q, %q", tt.payload, msg.Validator, msg.Check("msg"), tt.validator, tt.check)
		}
	}
	if got := (&message{Validator: ".Validate"}).Check("x"); got != ".Validate(x)" {
		t.Errorf("Check = %q", got)
	}
}

func TestValidationWarnings(t *testing.T) {
	tests := []struct{ payload, want string }{
		{`{type: string, format: uri}`, `M: format "uri" is not enforced`},
		{`{type: integer, format: date}`, `M: format "date" is not enforced`},
		{`{type: string, pattern: '(?=a)'}`, `M: pattern "(?=a)" is not enforced: Go's regexp package does not support it`},
		{`{type: integer, minLength: 1}`, "M: minLength is not enforced"},
		{`{type: string, minimum: 1}`, "M: minimum is not enforced"},
		{`{type: string, const: 1}`, "M: const is not enforced"},
		{`{type: integer, const: a}`, "M: const is not enforced"},
		{`{type: boolean, const: 1}`, "M: const is not enforced"},
		{`{type: string, format: date-time, minLength: 1}`, "M: minLength is not enforced"},
		{`{type: string, format: byte, maxLength: 1}`, "M: maxLength is not enforced"},
		{`{minLength: 1}`, "M: minLength is not enforced"},
		{`{type: object, properties: {a: {type: string}}, minProperties: 1}`, "M: minProperties is not enforced"},
		{`{type: object, properties: {a: {type: string}}, maxProperties: 1}`, "M: maxProperties is not enforced"},
		{`{type: object, properties: {a: {type: string}}, additionalProperties: false}`, "M: additionalProperties is not enforced"},
		{`{type: object, properties: {a: {type: string}}, additionalProperties: {type: string}}`, "M: additionalProperties is not enforced"},
		{`{type: object, properties: {a: {type: string, not: {const: x}}}}`, "M.a: not is not enforced"},
		{`{type: object, properties: {a: {type: array, items: {type: string, if: {}}}}}`, "M.a[]: if is not enforced"},
		{`{type: object, additionalProperties: {type: string, then: {}}}`, "M[]: then is not enforced"},
		{`{type: array, uniqueItems: true, items: {type: object, properties: {a: {type: string}}}}`, "M: uniqueItems is not enforced on items of type MItem"},
		{`{type: array, uniqueItems: true}`, "M: uniqueItems is not enforced on items of type any"},
		{`{type: array, uniqueItems: true, items: {type: string, format: date-time}}`, "M: uniqueItems is not enforced on items of type time.Time"},
		{`{oneOf: [{type: string}]}`, "M: oneOf is generated as json.RawMessage and not validated"},
	}
	for _, kw := range constraintKeywords {
		value := "{}"
		switch kw {
		case "maxItems", "maxLength", "maxProperties", "maximum", "minItems", "minLength", "minProperties", "minimum", "multipleOf",
			"exclusiveMaximum", "exclusiveMinimum":
			value = "1"
		case "uniqueItems":
			value = "true"
		case "pattern":
			value = "x"
		}
		tests = append(tests, struct{ payload, want string }{`{type: object, properties: {a: {}}, ` + kw + `: ` + value + `}`, "M: " + kw + " is not enforced"})
	}
	for _, tt := range tests {
		_, _, warnings, err := models(t, payloadDoc(tt.payload))
		if err != nil {
			t.Errorf("%s: %v", tt.payload, err)
			continue
		}
		if !slices.Contains(warnings, tt.want) {
			t.Errorf("%s: warnings %q do not contain %q", tt.payload, warnings, tt.want)
		}
	}
	// x-go-type ignores the rest of the schema without a word.
	if _, _, warnings, _ := models(t, payloadDoc(`{x-go-type: string, minLength: 1}`)); len(warnings) > 0 {
		t.Errorf("x-go-type: warnings %q", warnings)
	}
	for _, payload := range []string{
		`{type: object, properties: {a: {type: string}}, additionalProperties: true}`,
		`{type: object, properties: {a: {type: string}}, additionalProperties: {}}`,
		`{type: array, uniqueItems: true, items: {enum: [a]}}`,
		`{type: array, uniqueItems: true, items: {type: boolean}}`,
		`{type: array, uniqueItems: true, items: {$ref: '#/components/schemas/N'}}`,
	} {
		_, _, warnings, err := models(t, payloadDoc(payload)+"components: {schemas: {N: {type: number}}}")
		if err != nil || len(warnings) > 0 {
			t.Errorf("%s: warnings %q, %v", payload, warnings, err)
		}
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct{ payload, want string }{
		{`{type: integer, multipleOf: 0}`, "M: multipleOf must be a number greater than 0"},
		{`{type: integer, multipleOf: -1}`, "M: multipleOf must be a number greater than 0"},
		{`{type: number, multipleOf: .nan}`, "M: multipleOf must be a number greater than 0"},
		{`{type: number, multipleOf: .inf}`, "M: multipleOf must be a number greater than 0"},
		{`{type: number, minimum: .inf}`, "M: minimum: must be finite, got +Inf"},
		{`{type: number, maximum: -.inf}`, "M: maximum: must be finite, got -Inf"},
		{`{type: number, exclusiveMinimum: .nan}`, "M: exclusiveMinimum: must be finite, got NaN"},
		{`{type: number, exclusiveMaximum: .inf}`, "M: exclusiveMaximum: must be finite, got +Inf"},
		{`{type: number, exclusiveMinimum: true}`, "M: exclusiveMinimum: true needs a bound next to it"},
		{`{type: number, exclusiveMaximum: true}`, "M: exclusiveMaximum: true needs a bound next to it"},
		{`{type: number, exclusiveMinimum: x}`, "M: exclusiveMinimum: must be a number, got x"},
		{`{type: number, exclusiveMaximum: [1]}`, "M: exclusiveMaximum: must be a number, got [1]"},
		{`{type: object, properties: {a: {type: integer, multipleOf: 0}}}`, "M: M.a: multipleOf"},
		{`{type: array, items: {type: integer, multipleOf: 0}}`, "M: M[]: multipleOf"},
		{`{type: object, additionalProperties: {type: integer, multipleOf: 0}}`, "M: M[]: multipleOf"},
		{`{type: object, additionalProperties: {type: [a, b]}}`, "cannot unmarshal"},
		{`{type: array, items: {$ref: '#/components/schemas/Bad'}}`, "Bad: Bad: multipleOf must be a number greater than 0"},
	}
	for _, tt := range tests {
		more := ""
		if strings.Contains(tt.payload, "Bad") {
			more = "components: {schemas: {Bad: {type: integer, multipleOf: 0}}}"
		}
		_, _, _, err := models(t, payloadDoc(tt.payload)+more)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: err = %v, want it to contain %q", tt.payload, err, tt.want)
		}
	}
}

func TestResolve(t *testing.T) {
	vs := newValidators(newTypeSet(mustParse(t, head+`
components:
  schemas:
    A: {$ref: '#/components/schemas/B'}
    B: {allOf: [{$ref: '#/components/schemas/C'}]}
    C: {type: string, minLength: 1}
    Loop: {$ref: '#/components/schemas/Loop'}
`), nil))
	s, err := vs.resolve(&spec.Schema{Ref: "#/components/schemas/A"})
	if err != nil || s.Type != "string" || *s.MinLength != 1 {
		t.Errorf("resolve = %+v, %v", s, err)
	}
	if s, err := vs.resolve(nil); s != nil || err != nil {
		t.Errorf("resolve(nil) = %v, %v", s, err)
	}
	if _, err := vs.resolve(&spec.Schema{Ref: "#/components/schemas/Missing"}); err == nil {
		t.Error("missing ref resolved")
	}
	if _, err := vs.resolve(&spec.Schema{AllOf: []*spec.Schema{{Type: "string"}, {Type: "integer"}}}); err == nil {
		t.Error("conflicting allOf resolved")
	}
	if _, err := vs.resolve(&spec.Schema{SchemaFormat: "x"}); err == nil {
		t.Error("unsupported schemaFormat resolved")
	}
}

func TestPath(t *testing.T) {
	tests := []struct {
		p     path
		code  string
		depth int
	}{
		{path{}, `"/"`, 0},
		{path{}.prop("a"), `"/a"`, 0},
		{path{}.prop("a/b~c").prop("d"), `"/a~1b~0c/d"`, 0},
		{path{}.index("i"), `"/"+i`, 1},
		{path{}.prop("a").index("i").prop("b").index("j"), `"/a/"+i+"/b/"+j`, 2},
	}
	for _, tt := range tests {
		if got := tt.p.code(); got != tt.code || tt.p.depth() != tt.depth {
			t.Errorf("%v: code %s, depth %d; want %s, %d", tt.p, got, tt.p.depth(), tt.code, tt.depth)
		}
	}
	// Extending a path leaves it as it was.
	base := make(path, 1, 4)
	base[0] = segment{token: "a"}
	b, c := base.prop("b"), base.prop("c")
	if b.code() != `"/a/b"` || c.code() != `"/a/c"` {
		t.Errorf("shared prefix: %s, %s", b.code(), c.code())
	}
}

func TestPatternsAreShared(t *testing.T) {
	_, m, _, err := models(t, payloadDoc(`{type: object, properties: {a: {type: string, pattern: x}, b: {type: string, pattern: x}, c: {type: string, pattern: y}}}`))
	if err != nil {
		t.Fatal(err)
	}
	want := []pattern{{"pattern1", "x"}, {"pattern2", "y"}}
	if !slices.Equal(m.Patterns, want) || m.Patterns[0].Literal() != `"x"` {
		t.Errorf("patterns = %+v, want %+v", m.Patterns, want)
	}
}

// TestValidationIsWired checks where the generated operations validate.
func TestValidationIsWired(t *testing.T) {
	out, _ := compile(t, strings.ReplaceAll(allOperations, "payload: {type: string}", "payload: {type: string, minLength: 1}"))
	contains(t, out,
		// Producer and Client check what they send.
		"if err := msg.Validate(); err != nil {\n\t\treturn fmt.Errorf(\"Event: %w\", err)",
		"if err := validateLookup(req); err != nil {\n\t\treturn resp, fmt.Errorf(\"Lookup: %w\", err)",
		// Consumer and Server check what they receive.
		"decode(TicksChannel, validateTick, msg)",
		"decode(LookupsChannel, validateLookup, msg)",
		// Server checks its reply.
		"if err := validateAnswer(resp); err != nil {\n\t\t\t\treturn nil, fmt.Errorf(\"Answer: %w\", err)",
		"func (m Event) Validate() error {\n\treturn nil\n}",
	)

	// Payloads without constraints are not checked.
	out, _ = compile(t, head+`
channels:
  c: {address: a, messages: {m: {name: M}}}
  r: {address: null, messages: {m: {name: R}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
  get: {action: send, channel: {$ref: '#/channels/c'}, reply: {channel: {$ref: '#/channels/r'}}}
  serve: {action: receive, channel: {$ref: '#/channels/c'}, reply: {channel: {$ref: '#/channels/r'}}}
`)
	contains(t, out, "decode(CChannel, nil, msg)", "return json.Marshal(resp)")
	if strings.Contains(out, "Validate") || strings.Contains(out, "ValidationError") {
		t.Errorf("payloads without constraints are checked:\n%s", out)
	}

	// The models alone have their Validate methods.
	doc := mustParse(t, allOperations)
	models, err := Generate(doc, Options{Package: "p", Parts: []Part{PartModels}})
	if err != nil {
		t.Fatal(err)
	}
	typecheck(t, string(models))
	contains(t, string(models), "func (m Event) Validate() error", "type ValidationError struct")
	if strings.Contains(string(models), "decode") {
		t.Errorf("models contain the runtime:\n%s", models)
	}
}

// TestValidationCompiles checks that every enforced keyword compiles into
// valid code, alone and in the models part.
func TestValidationCompiles(t *testing.T) {
	src := payloadDoc(`{
		type: object, required: [a, b, d],
		properties: {
			a: {type: string, minLength: 1, maxLength: 9, pattern: '^a', format: uuid, const: a},
			b: {type: array, items: {type: integer, format: int32, maximum: 1e12}, minItems: 1, maxItems: 2, uniqueItems: true},
			c: {type: number, format: float, minimum: 0, maximum: 9, exclusiveMinimum: 0, exclusiveMaximum: 9, multipleOf: 0.5},
			d: {type: object, minProperties: 1, additionalProperties: {type: boolean, const: true}},
			e: {type: array, items: {type: array, items: {type: string, format: date}}},
			f: {type: string, format: time}, g: {type: string, format: email},
			h: {type: string, format: ipv4}, i: {type: string, format: ipv6},
			j: {type: integer, multipleOf: 2, const: 4},
			k: {$ref: '#/components/schemas/K'}
		}
	}`) + `
components:
  schemas:
    K: {type: array, items: {type: string, pattern: '^a'}}`
	out, warnings := compile(t, src)
	if len(warnings) > 0 {
		t.Errorf("unexpected warnings %q", warnings)
	}
	contains(t, out, "func validateK(v K) error", "func isUUID(", "func isDate(", "func isTime(", "func isEmail(",
		"func isIPv4(", "func isIPv6(", "func duplicate[", "func pointerToken(", "func multipleOf(")
	if strings.Count(out, "regexp.MustCompile") != 1 {
		t.Errorf("patterns are compiled more than once:\n%s", out)
	}
	models, err := Generate(mustParse(t, src), Options{Package: "p", Parts: []Part{PartModels}})
	if err != nil {
		t.Fatal(err)
	}
	typecheck(t, string(models))

	// Helpers that are not used are left out.
	out, _ = compile(t, payloadDoc(`{type: string, minLength: 1}`))
	for _, helper := range []string{"isUUID", "duplicate", "pointerToken", "multipleOf", "regexp"} {
		if strings.Contains(out, helper) {
			t.Errorf("unused %s generated:\n%s", helper, out)
		}
	}
}

// TestAllOfMergesConstraints checks that allOf members contribute their
// constraints, and that a $ref with constraints next to it is not taken as
// the referenced type alone.
func TestAllOfMergesConstraints(t *testing.T) {
	tests := []struct{ payload, want string }{
		{`{allOf: [{type: integer, minimum: 1}, {maximum: 9}]}`, fails("v < 1", `"must be >= 1"`) + "\n" + fails("v > 9", `"must be <= 9"`)},
		{`{allOf: [{type: integer, exclusiveMinimum: 1}, {exclusiveMaximum: 9}]}`, fails("v <= 1", `"must be > 1"`) + "\n" + fails("v >= 9", `"must be < 9"`)},
		{`{allOf: [{type: integer}, {multipleOf: 2}]}`, fails("v%2 != 0", `"must be a multiple of 2"`)},
		{`{allOf: [{type: string}, {maxLength: 2}]}`, "if n := utf8.RuneCountInString(v); n > 2 {\np.add(\"/\", \"want at most 2 characters, got %d\", n)\n}"},
		{`{allOf: [{type: string}, {pattern: x}]}`, fails("!pattern1.MatchString(v)", `"does not match %s", pattern1`)},
		{`{allOf: [{type: array}, {minItems: 1}]}`, "if v != nil {\nif n := len(v); n < 1 {\np.add(\"/\", \"want at least 1 items, got %d\", n)\n}\n}"},
		{`{allOf: [{type: array}, {maxItems: 1}]}`, "if v != nil {\nif n := len(v); n > 1 {\np.add(\"/\", \"want at most 1 items, got %d\", n)\n}\n}"},
		{`{allOf: [{type: array, items: {type: string}}, {uniqueItems: true}]}`, "if v != nil {\nif i, j, dup := duplicate(v); dup {\np.add(\"/\", \"items %d and %d are equal\", i, j)\n}\n}"},
		{`{allOf: [{type: object}, {minProperties: 1}]}`, "if v != nil {\nif n := len(v); n < 1 {\np.add(\"/\", \"want at least 1 properties, got %d\", n)\n}\n}"},
		{`{allOf: [{type: object}, {maxProperties: 1}]}`, "if v != nil {\nif n := len(v); n > 1 {\np.add(\"/\", \"want at most 1 properties, got %d\", n)\n}\n}"},
		{`{allOf: [{$ref: '#/components/schemas/N'}], maximum: 9}`, fails("v > 9", `"must be <= 9"`)},
	}
	for _, tt := range tests {
		if got := checks(t, tt.payload, "components: {schemas: {N: {type: integer}}}"); got != tt.want {
			t.Errorf("%s:\n%s\nwant\n%s", tt.payload, got, tt.want)
		}
	}
	// A constraint next to a lone $ref keeps M from being N.
	_, m, warnings, err := models(t, payloadDoc(`{allOf: [{$ref: '#/components/schemas/N'}], minLength: 1}`)+"components: {schemas: {N: {type: integer}}}")
	if err != nil || m.Ops[0].Message.Type != "M" || !slices.Contains(warnings, "M: minLength is not enforced") {
		t.Errorf("type %s, warnings %q, %v", m.Ops[0].Message.Type, warnings, err)
	}
	_, m, _, _ = models(t, payloadDoc(`{allOf: [{$ref: '#/components/schemas/N'}], description: d}`)+"components: {schemas: {N: {type: integer}}}")
	if m.Ops[0].Message.Type != "N" {
		t.Errorf("lone $ref: type %s, want N", m.Ops[0].Message.Type)
	}

	for _, payload := range []string{
		`{allOf: [{type: integer, minimum: 1}, {minimum: 2}]}`,
		`{allOf: [{type: string, pattern: a}, {pattern: b}]}`,
	} {
		if _, _, _, err := models(t, payloadDoc(payload)); err == nil || !strings.Contains(err.Error(), "allOf: conflicting") {
			t.Errorf("%s: err = %v", payload, err)
		}
	}
}
