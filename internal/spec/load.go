package spec

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// maxQueueLen is the maximum length of a NATS queue name per the binding spec.
const maxQueueLen = 255

// LoadFile reads and parses an AsyncAPI document from a YAML or JSON file.
func LoadFile(path string) (*Document, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Parse parses an AsyncAPI document from YAML or JSON bytes (JSON is valid YAML).
func Parse(b []byte) (*Document, error) {
	var doc Document
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("parse document: %w", err)
	}
	if err := yaml.Unmarshal(b, &doc.raw); err != nil {
		return nil, fmt.Errorf("parse document: %w", err)
	}
	if err := doc.Validate(); err != nil {
		return nil, err
	}
	return &doc, nil
}

// Validate checks the parts of the document the generator relies on.
func (d *Document) Validate() error {
	var errs []error
	if !strings.HasPrefix(d.AsyncAPI, "3.") {
		errs = append(errs, fmt.Errorf("unsupported asyncapi version %q: only 3.x is supported", d.AsyncAPI))
	}
	for name, op := range d.Operations {
		op, _, err := d.ResolveOperation(op)
		if err != nil {
			errs = append(errs, fmt.Errorf("operation %q: %w", name, err))
			continue
		}
		if op.Action != ActionSend && op.Action != ActionReceive {
			errs = append(errs, fmt.Errorf("operation %q: action must be %q or %q, got %q", name, ActionSend, ActionReceive, op.Action))
		}
		if op.Channel == nil || op.Channel.Ref == "" {
			errs = append(errs, fmt.Errorf("operation %q: channel is required", name))
		}
		if op.Bindings != nil && op.Bindings.NATS != nil {
			if q := op.Bindings.NATS.Queue; len(q) > maxQueueLen {
				errs = append(errs, fmt.Errorf("operation %q: nats queue exceeds %d characters", name, maxQueueLen))
			} else if strings.ContainsAny(q, " \t\r\n") {
				errs = append(errs, fmt.Errorf("operation %q: nats queue %q must not contain whitespace", name, q))
			}
		}
	}
	return errors.Join(errs...)
}

// lookup returns the raw node addressed by a local JSON pointer ref ("#/a/b").
func (d *Document) lookup(ref string) (any, error) {
	if !strings.HasPrefix(ref, "#/") {
		return nil, fmt.Errorf("unsupported $ref %q: only local refs (#/...) are supported", ref)
	}
	node := d.raw
	for tok := range strings.SplitSeq(ref[2:], "/") {
		tok, err := url.PathUnescape(tok)
		if err != nil {
			return nil, fmt.Errorf("$ref %q: %w", ref, err)
		}
		tok = strings.NewReplacer("~1", "/", "~0", "~").Replace(tok)
		switch n := node.(type) {
		case map[string]any:
			v, ok := n[tok]
			if !ok {
				return nil, fmt.Errorf("$ref %q: %q not found", ref, tok)
			}
			node = v
		case []any:
			i, err := strconv.Atoi(tok)
			if err != nil || i < 0 || i >= len(n) {
				return nil, fmt.Errorf("$ref %q: bad index %q", ref, tok)
			}
			node = n[i]
		default:
			return nil, fmt.Errorf("$ref %q: cannot descend into %q", ref, tok)
		}
	}
	return node, nil
}

// RefName returns the last path segment of a ref, which is used as the
// referenced object's name.
func RefName(ref string) string {
	return ref[strings.LastIndex(ref, "/")+1:]
}

const maxRefDepth = 32

// resolve follows $ref chains starting at v until it reaches an object
// without a $ref. It returns the object and the name of the last ref
// followed (empty if v had no ref).
func resolve[T any](d *Document, v *T, refOf func(*T) string) (*T, string, error) {
	name := ""
	for i := 0; v != nil && refOf(v) != ""; i++ {
		if i == maxRefDepth {
			return nil, "", fmt.Errorf("$ref chain too deep (cycle?)")
		}
		ref := refOf(v)
		node, err := d.lookup(ref)
		if err != nil {
			return nil, "", err
		}
		b, err := yaml.Marshal(node)
		if err != nil {
			return nil, "", err
		}
		next := new(T)
		if err := yaml.Unmarshal(b, next); err != nil {
			return nil, "", fmt.Errorf("$ref %q: %w", ref, err)
		}
		v, name = next, RefName(ref)
	}
	return v, name, nil
}

func (d *Document) ResolveChannel(c *Channel) (*Channel, string, error) {
	return resolve(d, c, func(c *Channel) string { return c.Ref })
}

func (d *Document) ResolveOperation(o *Operation) (*Operation, string, error) {
	return resolve(d, o, func(o *Operation) string { return o.Ref })
}

func (d *Document) ResolveMessage(m *Message) (*Message, string, error) {
	return resolve(d, m, func(m *Message) string { return m.Ref })
}

func (d *Document) ResolveSchema(s *Schema) (*Schema, string, error) {
	return resolve(d, s, func(s *Schema) string { return s.Ref })
}

func (d *Document) ResolveParameter(p *Parameter) (*Parameter, string, error) {
	return resolve(d, p, func(p *Parameter) string { return p.Ref })
}

func (d *Document) ResolveReply(r *OperationReply) (*OperationReply, string, error) {
	return resolve(d, r, func(r *OperationReply) string { return r.Ref })
}

// ChannelByRef resolves a reference such as "#/channels/userSignup".
func (d *Document) ChannelByRef(ref string) (*Channel, string, error) {
	c, name, err := d.ResolveChannel(&Channel{Ref: ref})
	if err != nil {
		return nil, "", err
	}
	// Prefer the key under #/channels as the channel name.
	if strings.HasPrefix(ref, "#/channels/") {
		name = RefName(ref)
	}
	return c, name, nil
}

// MessageByRef resolves a reference such as "#/channels/x/messages/y".
func (d *Document) MessageByRef(ref string) (*Message, string, error) {
	return d.ResolveMessage(&Message{Ref: ref})
}
