package validation

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
)

// check decodes a Sample made of the given properties, with valid tags unless
// they are among them, and returns the problems Validate finds.
func check(t *testing.T, props string) []string {
	t.Helper()
	if !strings.Contains(props, `"tags"`) {
		props = `"tags": ["a"], ` + props
	}
	var s Sample
	if err := json.Unmarshal([]byte("{"+props+"}"), &s); err != nil {
		t.Fatalf("{%s}: %v", props, err)
	}
	return problems(t, s.Validate())
}

func problems(t *testing.T, err error) []string {
	t.Helper()
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
		{`"int": 0`, ""},
		{`"int": -3`, ""},
		{`"int": 10`, "/int: must be a multiple of 3"},
		{`"int32": -5`, ""},
		{`"int32": 2147483647`, ""},
		{`"int32": -6`, "/int32: must be >= -5"},
		{`"frac": 2`, ""},
		{`"frac": 1`, "/frac: must be > 1.5"},
		{`"dec": 0.3`, ""},
		{`"dec": 0.9`, ""},
		{`"dec": 0.35`, "/dec: must be a multiple of 0.1"},
		{`"dec": 0`, "/dec: must be > 0"},
		{`"dec": 1`, "/dec: must be < 1"},
		{`"float": 0.5`, ""},
		{`"float": 0.25`, "/float: must be >= 0.5"},
		{`"draft4": 0.5`, ""},
		{`"draft4": 9.5`, ""},
		{`"draft4": 0`, "/draft4: must be > 0"},
		{`"draft4": 10`, "/draft4: must be < 10"},
		{`"str": "ab"`, ""},
		{`"str": "abc"`, ""},
		{`"str": "äöü"`, ""},
		{`"str": "a"`, "/str: want at least 2 characters, got 1"},
		{`"str": "abcd"`, "/str: want at most 3 characters, got 4"},
		{`"min": "a"`, ""},
		{`"min": ""`, "/min: want at least 1 characters, got 0"},
		{`"max": "ä"`, ""},
		{`"max": "ab"`, "/max: want at most 1 characters, got 2"},
		{`"date": "2026-11-18"`, ""},
		{`"date": "2026-11-31"`, "/date: not a valid date"},
		{`"time": "10:00:00Z"`, ""},
		{`"time": "10:00:00.5+01:00"`, ""},
		{`"time": "10:00"`, "/time: not a valid time"},
		{`"dateTime": "2026-11-18T10:00:00.123Z"`, ""},
		{`"uuid": "3f2b8c1e-1a2b-4c3d-8e9f-0a1b2c3d4E5F"`, ""},
		{`"uuid": "3f2b8c1e1a2b4c3d8e9f0a1b2c3d4e5f"`, "/uuid: not a valid uuid"},
		{`"uuid": "3f2b8c1e-1a2b-4c3d-8e9f-0a1b2c3d4e5"`, "/uuid: not a valid uuid"},
		{`"uuid": "3f2b8c1e-1a2b-4c3d-8e9f_0a1b2c3d4e5f"`, "/uuid: not a valid uuid"},
		{`"uuid": "3f2b8c1e-1a2b-4c3d-8e9f-0a1b2c3d4e5g"`, "/uuid: not a valid uuid"},
		{`"uuid": "3f2b8c1e-1a2b-4c3d-8e9f-0a1b2c3d4e5/"`, "/uuid: not a valid uuid"},
		{`"uuid": "3f2b8c1e-1a2b-4c3d-8e9f-0a1b2c3d4e5:"`, "/uuid: not a valid uuid"},
		{`"uuid": "3f2b8c1e-1a2b-4c3d-8e9f-0a1b2c3d4e5@"`, "/uuid: not a valid uuid"},
		{`"uuid": "3f2b8c1e-1a2b-4c3d-8e9f-0a1b2c3d4e5G"`, "/uuid: not a valid uuid"},
		{`"uuid": "3f2b8c1e-1a2b-4c3d-8e9f-0a1b2c3d4e5` + "`" + `"`, "/uuid: not a valid uuid"},
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
		{`"tags": ["a", "b", "c"]`, ""},
		{`"tags": ["a", "b", "c", "d"]`, "/tags: want at most 3 items, got 4"},
		{`"tags": []`, "/tags: want at least 1 items, got 0"},
		{`"tags": ["a", "b", "a"]`, "/tags: items 0 and 2 are equal"},
		{`"tags": ["a", "b", "b"]`, "/tags: items 1 and 2 are equal"},
		{`"codes": ["AB", "CD"]`, ""},
		{`"codes": []`, ""},
		{`"codes": ["AB", "AB"]`, "/codes: items 0 and 1 are equal"},
		{`"codes": ["AB", "CD", "EF"]`, "/codes: want at most 2 items, got 3"},
		{`"codes": ["AB", "c"]`, "/codes/1: does not match ^[A-Z]{2}$"},
		{`"counts": {"a": 1, "b": 0}`, ""},
		{`"counts": {}`, "/counts: want at least 1 properties, got 0"},
		{`"counts": {"a": 1, "b": 2, "c": 3}`, "/counts: want at most 2 properties, got 3"},
		{`"counts": {"a/~b": -1}`, "/counts/a~1~0b: must be >= 0"},
		{`"matrix": [[0, 1], [2]]`, ""},
		{`"matrix": [[0], [1, -2]]`, "/matrix/1/1: must be >= 0"},
		{`"constStr": "x"`, ""},
		{`"constStr": "y"`, `/constStr: must be "x"`},
		{`"constNum": 1.5`, ""},
		{`"constNum": 1`, "/constNum: must be 1.5"},
		{`"constInt": 3`, ""},
		{`"constInt": 4`, "/constInt: must be 3"},
		{`"constTrue": true`, ""},
		{`"constTrue": false`, "/constTrue: must be true"},
		{`"constFalse": false`, ""},
		{`"constFalse": true`, "/constFalse: must be false"},
		{`"allOf": "ab"`, ""},
		{`"allOf": "a"`, "/allOf: want at least 2 characters, got 1"},
		{`"color": "red"`, ""},
		{`"color": "green"`, ""},
		{`"color": "blue"`, "/color: must be one of [red green]"},
		{`"tree": {"value": 1, "children": [{"children": [{"value": 2}]}]}`, ""},
		{`"tree": {"children": [{"children": [{"value": -1}]}]}`, "/tree/children/0/children/0/value: must be >= 0"},
		{`"tree": {"value": -1}`, "/tree/value: must be >= 0"},
		{`"a/b~c": 1`, ""},
		{`"a/b~c": -1`, "/a~1b~0c: must be >= 0"},
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

func TestProblemsAreSortedAndJoined(t *testing.T) {
	str, frac := "a", int64(1)
	err := Sample{Str: &str, Frac: &frac}.Validate()
	want := []string{`/: missing required property "tags"`, "/frac: must be > 1.5", "/str: want at least 2 characters, got 1"}
	if got := problems(t, err); !slices.Equal(got, want) {
		t.Errorf("problems = %q, want %q", got, want)
	}
	if err.Error() != "invalid payload: "+strings.Join(want, "; ") {
		t.Errorf("Error() = %q", err.Error())
	}
}

func TestValidators(t *testing.T) {
	if err := validateCodes(nil); err != nil {
		t.Errorf("no codes: %v", err)
	}
	if got := problems(t, validateCode("a")); !slices.Equal(got, []string{"/: does not match ^[A-Z]{2}$"}) {
		t.Errorf("code problems = %q", got)
	}
	if got := problems(t, Color("x").Validate()); !slices.Equal(got, []string{"/: must be one of [red green]"}) {
		t.Errorf("color problems = %q", got)
	}
	var p violations
	p.nest("/a", errors.New("boom"))
	p.nest("/c", &ValidationError{Problems: []string{"/: x", "/d: y"}})
	if want := (violations{"/a: boom", "/c: x", "/c/d: y"}); !slices.Equal(p, want) {
		t.Errorf("nest: %q, want %q", p, want)
	}
	if !multipleOf(0.3, 0.1) || multipleOf(0.35, 0.1) || !multipleOf(-6, 3) || !multipleOf(1e20, 1e10) {
		t.Error("multipleOf")
	}
	if i, j, ok := duplicate([]int{1, 2, 3}); ok || i != 0 || j != 0 {
		t.Errorf("duplicate = %d, %d, %v", i, j, ok)
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
	if err := p.PublishSample(context.Background(), nil, Sample{Tags: []string{"a"}, Int: &three}); err != nil {
		t.Fatal(err)
	}
	err := p.PublishSample(context.Background(), nil, Sample{Tags: []string{"a"}, Int: &four})
	if err == nil || err.Error() != "Sample: invalid payload: /int: must be a multiple of 3" {
		t.Errorf("err = %v", err)
	}
	if err := p.PublishCodes(context.Background(), nil, Codes{"AB"}); err != nil {
		t.Fatal(err)
	}
	err = p.PublishCodes(context.Background(), nil, Codes{"AB", "x"})
	if err == nil || err.Error() != "Codes: invalid payload: /1: does not match ^[A-Z]{2}$" {
		t.Errorf("err = %v", err)
	}
	if want := []string{`{"int":3,"tags":["a"]}`, `["AB"]`}; len(r.bodies) != 2 || string(r.bodies[0]) != want[0] || string(r.bodies[1]) != want[1] {
		t.Errorf("published %q, want %q", r.bodies, want)
	}
}

type inbox struct{ data string }

func (i inbox) Subject() string              { return "codes" }
func (i inbox) Headers() map[string][]string { return nil }
func (i inbox) Data() []byte                 { return []byte(i.data) }

type subscriber struct {
	h func(context.Context, Inbox) (Settlement, error)
}

func (s *subscriber) Subscribe(_, _ string, h func(context.Context, Inbox) (Settlement, error)) (Subscription, error) {
	s.h = h
	return nil, nil
}

func TestConsumeValidates(t *testing.T) {
	s := &subscriber{}
	var got []Codes
	if _, err := NewConsumer(s).OnCodes(func(_ context.Context, m Received[Codes]) error {
		got = append(got, m.Payload)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if st, err := s.h(context.Background(), inbox{`["AB"]`}); st != Ack || err != nil {
		t.Errorf("valid: %v, %v", st, err)
	}
	st, err := s.h(context.Background(), inbox{`["AB", "AB"]`})
	var malformed *MalformedError
	if st != Term || !errors.As(err, &malformed) || err.Error() != "malformed message: codes: invalid payload: /: items 0 and 1 are equal" {
		t.Errorf("invalid: %v, %v", st, err)
	}
	if len(got) != 1 || !slices.Equal(got[0], Codes{"AB"}) {
		t.Errorf("handled %q", got)
	}
}
