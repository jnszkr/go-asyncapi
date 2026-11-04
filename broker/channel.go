package broker

import (
	"fmt"
	"slices"
	"strings"
)

// Params are the values of a channel's address parameters, keyed by name.
type Params map[string]string

// Channel describes where messages with payload T are sent.
type Channel[T any] struct {
	// Address is the subject, with parameters in braces: "orders.{region}.created".
	// Each parameter must span a whole dot-separated token.
	Address string
	// Allowed restricts parameter values; parameters not listed accept any value.
	Allowed map[string][]string
	// Defaults are used for parameters without a value.
	Defaults map[string]string
	// Schema is the JSON Schema of the payload, as a self-contained document.
	// A Client with a Validator checks payloads against it; empty means no
	// check.
	Schema string
	// ReplySchema is Schema for the replies of Request and Respond.
	ReplySchema string
}

// WithReplySchema returns a copy of c with ReplySchema set. Generated code
// calls it per operation, since operations on one channel can differ in their
// reply.
func (c Channel[T]) WithReplySchema(schema string) Channel[T] {
	c.ReplySchema = schema
	return c
}

// Subject returns the concrete subject for p.
func (c Channel[T]) Subject(p Params) (string, error) {
	tokens := strings.Split(c.Address, ".")
	for i, tok := range tokens {
		name, ok := paramName(tok)
		if !ok {
			continue
		}
		v := p[name]
		if v == "" {
			v = c.Defaults[name]
		}
		v, err := c.value(name, v)
		if err != nil {
			return "", err
		}
		tokens[i] = v
	}
	return strings.Join(tokens, "."), nil
}

// Pattern returns the address with every parameter replaced by "*".
func (c Channel[T]) Pattern() string {
	tokens := strings.Split(c.Address, ".")
	for i, tok := range tokens {
		if _, ok := paramName(tok); ok {
			tokens[i] = "*"
		}
	}
	return strings.Join(tokens, ".")
}

// ParseSubject extracts the parameters from a concrete subject.
func (c Channel[T]) ParseSubject(subject string) (Params, error) {
	want := strings.Split(c.Address, ".")
	got := strings.Split(subject, ".")
	if len(want) != len(got) {
		return nil, fmt.Errorf("subject %q does not match %s", subject, c.Address)
	}
	p := Params{}
	for i, tok := range want {
		if name, ok := paramName(tok); ok {
			p[name] = got[i]
		} else if tok != got[i] {
			return nil, fmt.Errorf("subject %q does not match %s", subject, c.Address)
		}
	}
	return p, nil
}

func (c Channel[T]) value(name, v string) (string, error) {
	if v == "" {
		return "", fmt.Errorf("%s: parameter %s must not be empty", c.Address, name)
	}
	if strings.ContainsAny(v, ".*> \t\r\n") {
		return "", fmt.Errorf("%s: parameter %s: %q is not a valid subject token", c.Address, name, v)
	}
	if allowed, ok := c.Allowed[name]; ok && !slices.Contains(allowed, v) {
		return "", fmt.Errorf("%s: parameter %s: %q is not one of %v", c.Address, name, v, allowed)
	}
	return v, nil
}

func paramName(tok string) (string, bool) {
	if len(tok) > 2 && tok[0] == '{' && tok[len(tok)-1] == '}' {
		return tok[1 : len(tok)-1], true
	}
	return "", false
}
