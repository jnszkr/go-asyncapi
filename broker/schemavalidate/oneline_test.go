package schemavalidate

import (
	"errors"
	"testing"
)

func TestOneLine(t *testing.T) {
	if err := oneLine(nil); err != nil {
		t.Errorf("oneLine(nil) = %v", err)
	}
	// A single line has no schema header to drop.
	if got := oneLine(errors.New("bad")).Error(); got != "bad" {
		t.Errorf("oneLine(single line) = %q, want bad", got)
	}
	multi := errors.New("jsonschema validation failed with 'schema.json#'\n  - at '/a': got string\n  - at '/b': missing")
	if got := oneLine(multi).Error(); got != "at '/a': got string; at '/b': missing" {
		t.Errorf("oneLine(multi) = %q", got)
	}
}
