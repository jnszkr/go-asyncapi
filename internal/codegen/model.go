package codegen

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/jnszkr/go-asyncapi/internal/spec"
	"gopkg.in/yaml.v3"
)

// model is the template input: the AsyncAPI document reduced to models,
// channel definitions and the operations of the thin generated client.
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
	Payload string // Go type of the messages on this channel
	Params  []param
}

func (c *channel) HasParams() bool { return len(c.Params) > 0 }

// Pattern is the address with every parameter replaced by "*", the subject
// receive operations subscribe to.
func (c *channel) Pattern() string { return paramRe.ReplaceAllString(c.Address, "*") }

// HasAllowed reports whether any parameter restricts its values.
func (c *channel) HasAllowed() bool {
	for _, p := range c.Params {
		if len(p.Enum) > 0 {
			return true
		}
	}
	return false
}

type param struct {
	Arg  string // Go parameter name
	Key  string // name in the address
	Doc  string
	Enum []string
}

type message struct {
	Name        string
	Type        string // Go payload type
	ContentType string
}

// headerDecl lists the header names of a message as constants.
type headerDecl struct {
	Message string
	Fields  []headerField
}

type headerField struct {
	Name string // Go constant name
	Key  string // header key
	Doc  string
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

// HasReceive reports whether any operation subscribes.
func (m *model) HasReceive() bool {
	for _, o := range m.Ops {
		if !o.IsSend() {
			return true
		}
	}
	return false
}
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
	switch ch.Payload {
	case "":
		ch.Payload = o.Message.Type
	case o.Message.Type:
	default:
		// TODO(phase 2): channels carrying several message types.
		return nil, fmt.Errorf("channel %q carries both %s and %s: one message type per channel is supported", ch.Key, ch.Payload, o.Message.Type)
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
		if err := b.headerDecl(msg.Headers, goName); err != nil {
			return nil, fmt.Errorf("message %q headers: %w", name, err)
		}
	}
	b.messages[goName] = m
	return m, nil
}

// headerDecl records the header names of a message's headers schema.
func (b *builder) headerDecl(s *spec.Schema, msgName string) error {
	s, _, err := b.doc.ResolveSchema(s)
	if err != nil {
		return err
	}
	if schemaType(s) != "object" {
		return fmt.Errorf("headers must be an object schema")
	}
	h := &headerDecl{Message: msgName}
	for _, key := range sortedKeys(s.Properties) {
		ps, _, err := b.doc.ResolveSchema(s.Properties[key])
		if err != nil {
			return err
		}
		h.Fields = append(h.Fields, headerField{Name: msgName + "Header" + exportName(key), Key: key, Doc: schemaDoc(ps)})
	}
	b.headers = append(b.headers, h)
	return nil
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
	for tok := range strings.SplitSeq(c.Address, ".") {
		if tok == "" {
			return nil, fmt.Errorf("channel %q: empty token in subject %q", key, c.Address)
		}
		sm := paramRe.FindAllStringSubmatchIndex(tok, -1)
		if len(sm) == 0 {
			if strings.ContainsAny(tok, "*> \t") {
				return nil, fmt.Errorf("channel %q: wildcards in addresses are not supported, use parameters", key)
			}
			continue
		}
		if len(sm) > 1 || sm[0][0] != 0 || sm[0][1] != len(tok) {
			return nil, fmt.Errorf("channel %q: parameter must span a whole subject token, got %q", key, tok)
		}
		pkey := tok[sm[0][2]:sm[0][3]]
		p := param{Arg: argName(pkey), Key: pkey}
		if raw, ok := ch.Parameters[pkey]; ok {
			def, _, err := b.doc.ResolveParameter(raw)
			if err != nil {
				return nil, fmt.Errorf("channel %q parameter %q: %w", key, pkey, err)
			}
			p.Doc, p.Enum = def.Description, def.Enum
		}
		c.Params = append(c.Params, p)
	}
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
