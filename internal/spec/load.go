package spec

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
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
	if !versionRe.MatchString(d.AsyncAPI) {
		errs = append(errs, fmt.Errorf("unsupported asyncapi version %q: only 3.0 and 3.1 are supported", d.AsyncAPI))
	}
	if d.Info.Title == "" {
		errs = append(errs, errors.New("info.title is required"))
	}
	if d.Info.Version == "" {
		errs = append(errs, errors.New("info.version is required"))
	}
	errs = append(errs, checkFields(d.raw, documentShape, "")...)
	errs = append(errs, d.validateParameters()...)
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
		if err := d.validateMessages(op); err != nil {
			errs = append(errs, fmt.Errorf("operation %q: %w", name, err))
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

var (
	versionRe = regexp.MustCompile(`^3\.[01]\.\d+`)
	paramRe   = regexp.MustCompile(`\{([^{}]+)\}`)
)

// validateParameters checks that channel parameters match their addresses,
// which the specification requires in both directions.
func (d *Document) validateParameters() []error {
	var errs []error
	check := func(prefix string, channels map[string]*Channel) {
		for name, raw := range channels {
			ch, _, err := d.ResolveChannel(raw)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s %q: %w", prefix, name, err))
				continue
			}
			if ch.Address == nil {
				continue
			}
			used := map[string]bool{}
			for _, m := range paramRe.FindAllStringSubmatch(*ch.Address, -1) {
				used[m[1]] = true
				if _, ok := ch.Parameters[m[1]]; !ok {
					errs = append(errs, fmt.Errorf("%s %q: address uses parameter %q, which parameters does not declare", prefix, name, m[1]))
				}
			}
			for p := range ch.Parameters {
				if !used[p] {
					errs = append(errs, fmt.Errorf("%s %q: parameter %q is not used in the address", prefix, name, p))
				}
			}
		}
	}
	check("channel", d.Channels)
	check("component channel", d.Components.Channels)
	return errs
}

// validateMessages checks that the messages of an operation and its reply
// belong to their channels.
func (d *Document) validateMessages(op *Operation) error {
	var errs []error
	if op.Channel != nil {
		errs = append(errs, d.checkChannelMessages(op.Channel.Ref, op.Messages))
	}
	if op.Reply != nil {
		reply, _, err := d.ResolveReply(op.Reply)
		if err != nil {
			errs = append(errs, err)
		} else if reply.Channel != nil {
			errs = append(errs, d.checkChannelMessages(reply.Channel.Ref, reply.Messages))
		}
	}
	return errors.Join(errs...)
}

func (d *Document) checkChannelMessages(channelRef string, refs []*Reference) error {
	if channelRef == "" || len(refs) == 0 {
		return nil
	}
	ch, _, err := d.ChannelByRef(channelRef)
	if err != nil {
		return err
	}
	base, err := d.CanonicalRef(channelRef)
	if err != nil {
		return err
	}
	var own []string
	for key := range ch.Messages {
		c, err := d.CanonicalRef(base + "/messages/" + key)
		if err != nil {
			return err
		}
		own = append(own, c)
	}
	for _, ref := range refs {
		c, err := d.CanonicalRef(ref.Ref)
		if err != nil {
			return err
		}
		if !slices.Contains(own, c) {
			return fmt.Errorf("%q is not a message of its channel %q", ref.Ref, channelRef)
		}
	}
	return nil
}

// CanonicalRef follows a chain of $refs and returns the last one, which
// identifies the referenced object however it was reached.
func (d *Document) CanonicalRef(ref string) (string, error) {
	for range maxRefDepth {
		node, err := d.lookup(ref)
		if err != nil {
			return "", err
		}
		m, ok := node.(map[string]any)
		if !ok {
			return ref, nil
		}
		next, ok := m["$ref"].(string)
		if !ok {
			return ref, nil
		}
		ref = next
	}
	return "", fmt.Errorf("$ref %q: chain too deep (cycle?)", ref)
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

// ResolveOperation resolves $refs and applies the operation's traits.
func (d *Document) ResolveOperation(o *Operation) (*Operation, string, error) {
	o, name, err := resolve(d, o, func(o *Operation) string { return o.Ref })
	if err != nil || o == nil || len(o.Traits) == 0 {
		return o, name, err
	}
	merged, err := mergeTraits[Operation](d, o.raw, o.Traits)
	return merged, name, err
}

// ResolveMessage resolves $refs and applies the message's traits.
func (d *Document) ResolveMessage(m *Message) (*Message, string, error) {
	m, name, err := resolve(d, m, func(m *Message) string { return m.Ref })
	if err != nil || m == nil || len(m.Traits) == 0 {
		return m, name, err
	}
	merged, err := mergeTraits[Message](d, m.raw, m.Traits)
	return merged, name, err
}

// mergeTraits applies the Traits Merge Mechanism: traits are merged in order
// with JSON Merge Patch, and never override a property of the target itself.
func mergeTraits[T any](d *Document, target map[string]any, traits []any) (*T, error) {
	var merged any = map[string]any{}
	for _, trait := range traits {
		for i := 0; ; i++ {
			m, ok := trait.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("trait must be an object, got %v", trait)
			}
			ref, ok := m["$ref"].(string)
			if !ok {
				break
			}
			if i == maxRefDepth {
				return nil, fmt.Errorf("trait $ref %q: chain too deep (cycle?)", ref)
			}
			var err error
			if trait, err = d.lookup(ref); err != nil {
				return nil, err
			}
		}
		merged = mergePatch(merged, trait)
	}
	own := make(map[string]any, len(target))
	for k, v := range target {
		if k != "traits" {
			own[k] = v
		}
	}
	merged = mergePatch(merged, own)
	b, err := yaml.Marshal(merged)
	if err != nil {
		return nil, err
	}
	out := new(T)
	if err := yaml.Unmarshal(b, out); err != nil {
		return nil, fmt.Errorf("apply traits: %w", err)
	}
	return out, nil
}

// mergePatch implements JSON Merge Patch (RFC 7386).
func mergePatch(target, patch any) any {
	p, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	t, ok := target.(map[string]any)
	if !ok {
		t = nil
	}
	out := make(map[string]any, len(t)+len(p))
	for k, v := range t {
		out[k] = v
	}
	for k, v := range p {
		if v == nil {
			delete(out, k)
		} else {
			out[k] = mergePatch(out[k], v)
		}
	}
	return out
}

// ResolveSchema follows $refs and Multi Format Schema wrappers until it
// reaches a plain schema.
func (d *Document) ResolveSchema(s *Schema) (*Schema, string, error) {
	name := ""
	for range maxRefDepth {
		next, n, err := resolve(d, s, func(s *Schema) string { return s.Ref })
		if err != nil {
			return nil, "", err
		}
		if n != "" {
			name = n
		}
		if !next.IsMultiFormat() {
			return next, name, nil
		}
		if s, err = next.Unwrap(); err != nil {
			return nil, "", err
		}
	}
	return nil, "", fmt.Errorf("schema nested too deep (cycle?)")
}

// IsMultiFormat reports whether s is a Multi Format Schema wrapper.
func (s *Schema) IsMultiFormat() bool {
	return s != nil && (s.SchemaFormat != "" || !s.Inner.IsZero())
}

// Unwrap returns the schema inside a Multi Format Schema wrapper, or s itself
// if it is not one. Formats other than JSON Schema are an error.
func (s *Schema) Unwrap() (*Schema, error) {
	if !s.IsMultiFormat() {
		return s, nil
	}
	if !supportedSchemaFormat(s.SchemaFormat) {
		return nil, fmt.Errorf("unsupported schemaFormat %q: only AsyncAPI and JSON Schema draft-07 schemas are supported", s.SchemaFormat)
	}
	inner := new(Schema)
	if err := s.Inner.Decode(inner); err != nil {
		return nil, fmt.Errorf("schemaFormat %q: %w", s.SchemaFormat, err)
	}
	return inner, nil
}

// supportedSchemaFormat reports whether format is one every implementation
// must support. An empty format means the default, the AsyncAPI schema.
func supportedSchemaFormat(format string) bool {
	media, params, _ := strings.Cut(strings.ReplaceAll(format, " ", ""), ";")
	switch media {
	case "":
		return format == ""
	case "application/vnd.aai.asyncapi", "application/vnd.aai.asyncapi+json", "application/vnd.aai.asyncapi+yaml":
		return true
	case "application/schema+json", "application/schema+yaml":
		return params == "version=draft-07"
	}
	return false
}

func (d *Document) ResolveParameter(p *Parameter) (*Parameter, string, error) {
	return resolve(d, p, func(p *Parameter) string { return p.Ref })
}

func (d *Document) ResolveServer(s *Server) (*Server, string, error) {
	return resolve(d, s, func(s *Server) string { return s.Ref })
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
