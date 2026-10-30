package codegen

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/jnszkr/go-asyncapi/internal/spec"
	"gopkg.in/yaml.v3"
)

// model is the template input: the AsyncAPI document reduced to what the
// generated NATS client needs.
type model struct {
	Package  string
	Title    string
	Version  string
	Imports  []string
	Types    []*typeDecl
	Headers  []*headerDecl
	Channels []*channel
	Ops      []*operation
}

type channel struct {
	Name    string // Go identifier prefix, e.g. "UserSignup"
	Key     string // key under #/channels
	Doc     string
	Address string
	Pattern string // address with parameters replaced by "*"
	Tokens  []subjectToken
	Params  []param
}

func (c *channel) HasParams() bool { return len(c.Params) > 0 }

// subjectToken is one dot-separated token of a subject; either a literal or
// a parameter.
type subjectToken struct {
	Literal string
	Param   *param
}

type param struct {
	Name  string // Go field name
	Key   string // name in the address
	Doc   string
	Enum  []string
	Index int // token position in the subject
}

type message struct {
	Name        string
	Type        string // Go payload type
	Headers     *headerDecl
	ContentType string
}

type headerDecl struct {
	Name   string
	Doc    string
	Fields []headerField
}

type headerField struct {
	Name     string // Go field name
	Key      string // NATS header key
	Type     string // Go type
	Required bool
	Doc      string
}

type operation struct {
	Name    string
	Key     string
	Doc     string
	Action  spec.Action
	Channel *channel
	Message *message
	Reply   *message // nil unless request/reply
	Queue   string
}

func (o *operation) IsSend() bool    { return o.Action == spec.ActionSend }
func (o *operation) IsReceive() bool { return o.Action == spec.ActionReceive }

var paramRe = regexp.MustCompile(`\{([^{}]+)\}`)

type builder struct {
	doc      *spec.Document
	types    *typeSet
	channels map[string]*channel
	messages map[string]*message
	headers  []*headerDecl
}

func buildModel(doc *spec.Document, pkg string) (*model, error) {
	b := &builder{
		doc:      doc,
		types:    newTypeSet(doc),
		channels: map[string]*channel{},
		messages: map[string]*message{},
	}
	m := &model{Package: pkg, Title: doc.Info.Title, Version: doc.Info.Version}

	opKeys := sortedKeys(doc.Operations)
	for _, key := range opKeys {
		op, err := b.operation(key, doc.Operations[key])
		if err != nil {
			return nil, fmt.Errorf("operation %q: %w", key, err)
		}
		m.Ops = append(m.Ops, op)
	}
	// Component schemas are emitted even when no operation uses them.
	for _, name := range sortedKeys(doc.Components.Schemas) {
		if _, err := b.types.goType(&spec.Schema{Ref: componentSchemaPrefix + name}, exportName(name)); err != nil {
			return nil, fmt.Errorf("schema %q: %w", name, err)
		}
	}

	for _, key := range sortedKeys(b.channels) {
		m.Channels = append(m.Channels, b.channels[key])
	}
	sort.Slice(b.types.decls, func(i, j int) bool { return b.types.decls[i].Name < b.types.decls[j].Name })
	m.Types = b.types.decls
	m.Headers = b.headers
	for imp := range b.types.imports {
		m.Imports = append(m.Imports, imp)
	}
	sort.Strings(m.Imports)
	return m, nil
}

func (b *builder) operation(key string, raw *spec.Operation) (*operation, error) {
	op, _, err := b.doc.ResolveOperation(raw)
	if err != nil {
		return nil, err
	}
	ch, err := b.channel(op.Channel.Ref)
	if err != nil {
		return nil, err
	}
	o := &operation{
		Name:    exportName(key),
		Key:     key,
		Doc:     firstNonEmpty(op.Description, op.Summary, op.Title),
		Action:  op.Action,
		Channel: ch,
	}
	if op.Bindings != nil && op.Bindings.NATS != nil {
		o.Queue = op.Bindings.NATS.Queue
	}
	if o.Message, err = b.singleMessage(op.Messages, op.Channel.Ref); err != nil {
		return nil, err
	}
	if op.Reply != nil {
		reply, _, err := b.doc.ResolveReply(op.Reply)
		if err != nil {
			return nil, fmt.Errorf("reply: %w", err)
		}
		chRef := ""
		if reply.Channel != nil {
			chRef = reply.Channel.Ref
		}
		if o.Reply, err = b.singleMessage(reply.Messages, chRef); err != nil {
			return nil, fmt.Errorf("reply: %w", err)
		}
	}
	return o, nil
}

// singleMessage resolves the message of an operation. When refs is empty all
// messages of the channel apply. Exactly one message is supported for now.
func (b *builder) singleMessage(refs []*spec.Reference, channelRef string) (*message, error) {
	if len(refs) == 0 && channelRef != "" {
		ch, _, err := b.doc.ChannelByRef(channelRef)
		if err != nil {
			return nil, err
		}
		for _, k := range sortedKeys(ch.Messages) {
			refs = append(refs, &spec.Reference{Ref: channelRef + "/messages/" + k})
		}
	}
	switch len(refs) {
	case 0:
		return nil, fmt.Errorf("no messages defined")
	case 1:
		return b.message(refs[0].Ref)
	default:
		// TODO(phase 2): multiple messages per operation.
		return nil, fmt.Errorf("%d messages defined: only one message per operation is supported", len(refs))
	}
}

func (b *builder) message(ref string) (*message, error) {
	msg, name, err := b.doc.MessageByRef(ref)
	if err != nil {
		return nil, err
	}
	if msg.Name != "" {
		name = msg.Name
	}
	goName := exportName(name)
	if m, ok := b.messages[goName]; ok {
		return m, nil
	}
	m := &message{Name: goName, ContentType: firstNonEmpty(msg.ContentType, b.doc.DefaultContentType, "application/json")}
	if m.Type, err = b.types.namedType(msg.Payload, goName); err != nil {
		return nil, fmt.Errorf("message %q payload: %w", name, err)
	}
	if msg.Headers != nil {
		if m.Headers, err = b.headerDecl(msg.Headers, goName+"Headers"); err != nil {
			return nil, fmt.Errorf("message %q headers: %w", name, err)
		}
	}
	b.messages[goName] = m
	return m, nil
}

// headerDecl maps an object schema to a struct of primitive fields that is
// carried in NATS message headers.
func (b *builder) headerDecl(s *spec.Schema, name string) (*headerDecl, error) {
	s, _, err := b.doc.ResolveSchema(s)
	if err != nil {
		return nil, err
	}
	if schemaType(s) != "object" {
		return nil, fmt.Errorf("headers must be an object schema")
	}
	h := &headerDecl{Name: name, Doc: schemaDoc(s)}
	required := map[string]bool{}
	for _, r := range s.Required {
		required[r] = true
	}
	for _, key := range sortedKeys(s.Properties) {
		ps, _, err := b.doc.ResolveSchema(s.Properties[key])
		if err != nil {
			return nil, err
		}
		var t string
		switch ps.Type {
		case "string":
			t = "string"
		case "integer":
			t = "int64"
		case "number":
			t = "float64"
		case "boolean":
			t = "bool"
		default:
			return nil, fmt.Errorf("header %q: only string, integer, number and boolean headers are supported", key)
		}
		h.Fields = append(h.Fields, headerField{
			Name: exportName(key), Key: key, Type: t, Required: required[key], Doc: schemaDoc(ps),
		})
	}
	b.headers = append(b.headers, h)
	return h, nil
}

func (b *builder) channel(ref string) (*channel, error) {
	ch, key, err := b.doc.ChannelByRef(ref)
	if err != nil {
		return nil, err
	}
	if c, ok := b.channels[key]; ok {
		return c, nil
	}
	if ch.Address == nil {
		return nil, fmt.Errorf("channel %q: dynamic (null) addresses are only supported for replies", key)
	}
	c := &channel{
		Name:    exportName(key),
		Key:     key,
		Doc:     firstNonEmpty(ch.Description, ch.Summary, ch.Title),
		Address: *ch.Address,
	}
	var pattern []string
	for i, tok := range strings.Split(c.Address, ".") {
		if tok == "" {
			return nil, fmt.Errorf("channel %q: empty token in subject %q", key, c.Address)
		}
		sm := paramRe.FindAllStringSubmatchIndex(tok, -1)
		if len(sm) == 0 {
			if strings.ContainsAny(tok, "*> \t") {
				return nil, fmt.Errorf("channel %q: wildcards in addresses are not supported, use parameters", key)
			}
			c.Tokens = append(c.Tokens, subjectToken{Literal: tok})
			pattern = append(pattern, tok)
			continue
		}
		if len(sm) > 1 || sm[0][0] != 0 || sm[0][1] != len(tok) {
			return nil, fmt.Errorf("channel %q: parameter must span a whole subject token, got %q", key, tok)
		}
		pkey := tok[sm[0][2]:sm[0][3]]
		p := param{Name: exportName(pkey), Key: pkey, Index: i}
		if raw, ok := ch.Parameters[pkey]; ok {
			def, _, err := b.doc.ResolveParameter(raw)
			if err != nil {
				return nil, fmt.Errorf("channel %q parameter %q: %w", key, pkey, err)
			}
			p.Doc, p.Enum = def.Description, def.Enum
		}
		c.Params = append(c.Params, p)
		c.Tokens = append(c.Tokens, subjectToken{Param: &c.Params[len(c.Params)-1]})
		pattern = append(pattern, "*")
	}
	// c.Params may have been reallocated; re-point tokens at the final slice.
	for i, j := 0, 0; i < len(c.Tokens); i++ {
		if c.Tokens[i].Param != nil {
			c.Tokens[i].Param = &c.Params[j]
			j++
		}
	}
	c.Pattern = strings.Join(pattern, ".")
	b.channels[key] = c
	return c, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

func remarshal(in, out any) error {
	b, err := yaml.Marshal(in)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, out)
}
