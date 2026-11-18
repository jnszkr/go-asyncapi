package validation

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// check validates a Sample made of the given properties and returns the
// problems found.
func check(t *testing.T, props string) []string {
	t.Helper()
	err := schemaSample.validate([]byte("{" + props + "}"))
	if err == nil {
		return nil
	}
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("err = %v, want a ValidationError", err)
	}
	return verr.Problems
}

func TestKeywords(t *testing.T) {
	tests := []struct {
		props string
		want  string // the only problem, or "" for none
	}{
		{`"int": 9`, ""},
		{`"int": 9.0`, ""},
		{`"int": 10`, "/int: must be a multiple of 3"},
		{`"int": 1.5`, "/int: want integer, got number"},
		{`"int": "9"`, "/int: want integer, got string"},
		{`"dec": 0.3`, ""},
		{`"dec": 0.9`, ""},
		{`"dec": 0.35`, "/dec: must be a multiple of 0.1"},
		{`"dec": 0`, "/dec: must be > 0"},
		{`"dec": 1`, "/dec: must be < 1"},
		{`"draft4": 0.5`, ""},
		{`"draft4": 9.5`, ""},
		{`"draft4": 0`, "/draft4: must be > 0"},
		{`"draft4": 10`, "/draft4: must be < 10"},
		{`"bool": false`, ""},
		{`"bool": null`, "/bool: want boolean, got null"},
		{`"bool": {}`, "/bool: want boolean, got object"},
		{`"bool": []`, "/bool: want boolean, got array"},
		{`"str": "ab"`, ""},
		{`"str": "äöü"`, ""},
		{`"str": "a"`, "/str: want at least 2 characters, got 1"},
		{`"str": "abcd"`, "/str: want at most 3 characters, got 4"},
		{`"date": "2026-11-18"`, ""},
		{`"date": "2026-11-31"`, "/date: not a valid date"},
		{`"time": "10:00:00Z"`, ""},
		{`"time": "10:00:00.5+01:00"`, ""},
		{`"time": "10:00"`, "/time: not a valid time"},
		{`"dateTime": "2026-11-18T10:00:00.123Z"`, ""},
		{`"dateTime": "2026-11-18 10:00"`, "/dateTime: not a valid date-time"},
		{`"uuid": "3f2b8c1e-1a2b-4c3d-8e9f-0a1b2c3d4e5f"`, ""},
		{`"uuid": "3f2b8c1e1a2b4c3d8e9f0a1b2c3d4e5f"`, "/uuid: not a valid uuid"},
		{`"email": "a@b.c"`, ""},
		{`"email": "a.b.c"`, "/email: not a valid email"},
		{`"email": "@b.c"`, "/email: not a valid email"},
		{`"email": "a@"`, "/email: not a valid email"},
		{`"email": "a@b@c"`, "/email: not a valid email"},
		{`"email": "a@b c"`, "/email: not a valid email"},
		{`"ipv4": "10.0.0.1"`, ""},
		{`"ipv4": "::1"`, "/ipv4: not a valid ipv4"},
		{`"ipv4": "10.0.0"`, "/ipv4: not a valid ipv4"},
		{`"ipv6": "::1"`, ""},
		{`"ipv6": "10.0.0.1"`, "/ipv6: not a valid ipv6"},
		{`"ipv6": "fe80::1%eth0"`, "/ipv6: not a valid ipv6"},
		{`"ipv6": "x"`, "/ipv6: not a valid ipv6"},
		{`"tags": [1, "1", [1], {"a": 1}]`, "/tags: want at most 3 items, got 4"},
		{`"tags": [1, "1", 2]`, ""},
		{`"tags": [{"a": [1]}, {"a": [1.0]}]`, "/tags: items 0 and 1 are equal"},
		{`"tags": [{"a": 1}, {"a": 1, "b": 1}]`, ""},
		{`"tags": [{"a": 1}, {"b": 1}]`, ""},
		{`"tags": [[1, 2], [1, 3]]`, ""},
		{`"tags": [[1], [1, 2]]`, ""},
		{`"tags": [1, 2, 1]`, "/tags: items 0 and 2 are equal"},
		{`"tags": []`, "/tags: want at least 1 items, got 0"},
		{`"values": 1.0`, ""},
		{`"values": 2.5`, ""},
		{`"values": "x"`, ""},
		{`"values": true`, ""},
		{`"values": null`, ""},
		{`"values": [1]`, ""},
		{`"values": {"a": 1}`, ""},
		{`"values": 2`, "/values: must be one of [1 2.5 x true <nil> [1] map[a:1]]"},
		{`"values": [1, 1]`, "/values: must be one of [1 2.5 x true <nil> [1] map[a:1]]"},
		{`"values": {"a": 2}`, "/values: must be one of [1 2.5 x true <nil> [1] map[a:1]]"},
		{`"values": "1"`, "/values: must be one of [1 2.5 x true <nil> [1] map[a:1]]"},
		{`"constObject": {"a": [1, 2.0]}`, ""},
		{`"constObject": {"a": [2, 1]}`, "/constObject: must be map[a:[1 2]]"},
		{`"constNull": null`, ""},
		{`"constNull": false`, "/constNull: must be <nil>"},
		{`"counts": {"a": 1, "b": 2}`, ""},
		{`"counts": {}`, "/counts: want at least 1 properties, got 0"},
		{`"counts": {"a": 1, "b": 2, "c": 3}`, "/counts: want at most 2 properties, got 3"},
		{`"counts": {"a": "1"}`, "/counts/a: want integer, got string"},
		{`"anyOf": "x"`, ""},
		{`"anyOf": 1`, ""},
		{`"anyOf": true`, "/anyOf: matches none of anyOf"},
		{`"oneOf": 3`, ""},
		{`"oneOf": 5.5`, ""},
		{`"oneOf": 7`, "/oneOf: must match exactly one of oneOf, matches several"},
		{`"oneOf": 2.5`, "/oneOf: must match exactly one of oneOf, matches none"},
		{`"allOf": "ab"`, ""},
		{`"allOf": "a"`, "/allOf: want at least 2 characters, got 1"},
		{`"allOf": 1`, "/allOf: want string, got number"},
		{`"not": 1`, ""},
		{`"not": "x"`, "/not: must not match the not schema"},
		{`"ifThenElse": "abc"`, ""},
		{`"ifThenElse": "ab"`, "/ifThenElse: want at least 3 characters, got 2"},
		{`"ifThenElse": 1`, ""},
		{`"ifThenElse": 1.5`, "/ifThenElse: want integer, got number"},
		{`"tree": {"value": 1, "children": [{"children": [{"value": 2}]}]}`, ""},
		{`"tree": {"children": [{"children": [{"value": "x"}]}]}`, "/tree/children/0/children/0/value: want integer, got string"},
		{`"a/b~c": 1`, ""},
		{`"a/b~c": "x"`, "/a~1b~0c: want integer, got string"},
		{`"other": 1`, `/: unexpected property "other"`},
	}
	for _, tt := range tests {
		got := check(t, tt.props)
		var want []string
		if tt.want != "" {
			want = []string{tt.want}
		}
		if !slices.Equal(got, want) {
			t.Errorf("{%s}: problems = %q, want %q", tt.props, got, want)
		}
	}
}

func TestRootProblems(t *testing.T) {
	if err := schemaSample.validate([]byte(`[]`)); err == nil || err.Error() != "invalid payload: /: want object, got array" {
		t.Errorf("err = %v", err)
	}
	many := `"zz": 1, "int": 1, "str": "a"`
	got := check(t, many)
	want := []string{`/: unexpected property "zz"`, "/int: must be a multiple of 3", "/str: want at least 2 characters, got 1"}
	if !slices.Equal(got, want) {
		t.Errorf("problems = %q, want %q", got, want)
	}
}

func TestValidateDecodeErrors(t *testing.T) {
	for _, body := range []string{``, `{`, `{"int": }`} {
		err := schemaSample.validate([]byte(body))
		var verr *ValidationError
		if err == nil || !strings.HasPrefix(err.Error(), "decode: ") || errors.As(err, &verr) {
			t.Errorf("%q: err = %v, want a decode error", body, err)
		}
	}
	var none *schema
	if err := none.validate([]byte("{")); err != nil {
		t.Errorf("nil schema: err = %v", err)
	}
}

type recorder struct{ bodies [][]byte }

func (r *recorder) Publish(_ context.Context, _ string, _ map[string][]string, body []byte) error {
	r.bodies = append(r.bodies, body)
	return nil
}

func TestPublishValidates(t *testing.T) {
	r := &recorder{}
	p := NewProducer(r)
	three, four := int64(3), int64(4)
	if err := p.PublishSample(context.Background(), nil, Sample{Int: &three}); err != nil {
		t.Fatal(err)
	}
	err := p.PublishSample(context.Background(), nil, Sample{Int: &four})
	if err == nil || err.Error() != "Sample: invalid payload: /int: must be a multiple of 3" {
		t.Errorf("err = %v", err)
	}
	if len(r.bodies) != 1 || string(r.bodies[0]) != `{"int":3}` {
		t.Errorf("published %q", r.bodies)
	}
}
