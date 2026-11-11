package codegen

import (
	"testing"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

func TestUniqueIn(t *testing.T) {
	used := map[string]bool{}
	var got []string
	for range 3 {
		got = append(got, uniqueIn(used, "x"))
	}
	if want := []string{"x", "x2", "x3"}; got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("uniqueIn = %q, want %q", got, want)
	}
}

func TestEnumLiteral(t *testing.T) {
	tests := []struct {
		typ  string
		v    any
		lit  string
		fits bool
	}{
		{"string", "a", `"a"`, true},
		{"integer", "a", `"a"`, false},
		{"integer", 3, "3", true},
		{"number", int64(3), "3", true},
		{"string", 3, "3", false},
		{"boolean", uint64(3), "3", false},
		{"number", 2.25, "2.25", true},
		{"number", -0.125, "-0.125", true},
		{"integer", 2.25, "2.25", false},
		{"string", true, "", false},
	}
	for _, tt := range tests {
		lit, fits := enumLiteral(tt.typ, tt.v)
		if lit != tt.lit || fits != tt.fits {
			t.Errorf("enumLiteral(%q, %v) = %q, %v; want %q, %v", tt.typ, tt.v, lit, fits, tt.lit, tt.fits)
		}
	}
}

func TestMergeSchemaKeepsOwnAndFillsMissing(t *testing.T) {
	not, ifs := &spec.Schema{Type: "null"}, &spec.Schema{Type: "string"}
	member := &spec.Schema{Not: not, If: ifs, Description: "member", Title: "Member"}

	var empty spec.Schema
	if err := mergeSchema(&empty, member); err != nil {
		t.Fatal(err)
	}
	if empty.Not != not || empty.If != ifs || empty.Description != "member" || empty.Title != "Member" {
		t.Errorf("missing keywords not taken from member: %+v", empty)
	}

	ownNot, ownIf := &spec.Schema{Type: "integer"}, &spec.Schema{Type: "boolean"}
	own := spec.Schema{Not: ownNot, If: ownIf, Description: "own", Title: "Own"}
	if err := mergeSchema(&own, member); err != nil {
		t.Fatal(err)
	}
	if own.Not != ownNot || own.If != ownIf || own.Description != "own" || own.Title != "Own" {
		t.Errorf("own keywords overridden by member: %+v", own)
	}
}

func TestParameterLocationWarns(t *testing.T) {
	_, warnings := compile(t, head+`
channels:
  c:
    address: 'a.{id}'
    parameters: {id: {location: '$message.payload#/id'}}
    messages: {m: {payload: {type: string}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
`)
	warned(t, warnings, "location is ignored")

	_, warnings = compile(t, head+`
channels:
  c:
    address: 'a.{id}'
    parameters: {id: {description: an id}}
    messages: {m: {payload: {type: string}}}
operations:
  pub: {action: send, channel: {$ref: '#/channels/c'}}
`)
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %q", warnings)
	}
}
