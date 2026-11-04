// Package schemavalidate implements broker.Validator with JSON Schema draft-07.
//
//	consumer := orders.NewConsumer(b, broker.WithValidator(schemavalidate.New()))
//
// It lives apart from package broker so that the JSON Schema library is only
// linked into programs that validate.
package schemavalidate

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/jnszkr/go-asyncapi/broker"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Validator is a broker.Validator for JSON Schema draft-07. Patterns use the
// syntax of Go's regexp package, and format is checked for the formats draft-07
// defines.
type Validator struct{}

var _ broker.Validator = Validator{}

// New returns a Validator.
func New() Validator { return Validator{} }

// Compile implements broker.Validator. Errors of the returned function fit on
// one line, since responders send them back in a NATS header.
func (Validator) Compile(schema string) (func([]byte) error, error) {
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(schema))
	if err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}
	const url = "schema.json"
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft7)
	if err := c.AddResource(url, doc); err != nil {
		return nil, err
	}
	sch, err := c.Compile(url)
	if err != nil {
		return nil, err
	}
	return func(data []byte) error {
		v, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("parse payload: %w", err)
		}
		return oneLine(sch.Validate(v))
	}, nil
}

// oneLine joins the violations of a validation error, dropping the first line,
// which only names the schema.
func oneLine(err error) error {
	if err == nil {
		return nil
	}
	lines := strings.Split(err.Error(), "\n")
	if len(lines) > 1 {
		lines = lines[1:]
	}
	for i, line := range lines {
		lines[i] = strings.TrimPrefix(strings.TrimSpace(line), "- ")
	}
	return fmt.Errorf("%s", strings.Join(lines, "; "))
}
