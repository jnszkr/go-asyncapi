package codegen

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/jnszkr/go-asyncapi/broker/schemavalidate"
	"github.com/jnszkr/go-asyncapi/internal/spec"
	"gopkg.in/yaml.v3"
)

// model is the template input: the AsyncAPI document reduced to models,
// channel definitions and the thin generated types holding the operations.
type model struct {
	Package  string
	Title    string
	Version  string
	Imports  []string
	Types    []*typeDecl
	Headers  []*headerDecl
	Channels []*channel
	Ops      []*operation
	APIs     []*apiType
	Schemas  []*message // messages whose payload schema is declared
}

// apiType is a generated type holding the operations of one Part.
type apiType struct {
	Name string
	Role string // completes "<Name> <Role> <title>" in the type's doc comment
	// Transport is the part of the broker package's Broker the type needs,
	// which broker.For<Transport> accepts.
	Transport string
	Ops       []*operation
}

var apiTypes = []struct {
	part      Part
	name      string
	role      string
	transport string
}{
	{PartProducer, "Producer", "publishes the messages of", "Publisher"},
	{PartConsumer, "Consumer", "subscribes to the messages of", "Subscriber"},
	{PartClient, "Client", "sends the requests of", "Requester"},
	{PartServer, "Server", "answers the requests of", "PubSub"},
}

type channel struct {
	Name    string // Go identifier prefix, e.g. "UserSignup"
	Key     string // key under #/channels
	Doc     string
	Address string
	Payload string // Go type of the messages on this channel
	Params  []param
	Message *message // message whose schema the channel carries, if any
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

// HasDefaults reports whether any parameter has a default value.
func (c *channel) HasDefaults() bool {
	return slices.ContainsFunc(c.Params, func(p param) bool { return p.Default != "" })
}

type param struct {
	Arg     string // Go parameter name
	Key     string // name in the address
	Doc     string
	Enum    []string
	Default string
}

type message struct {
	Name        string
	Type        string // Go payload type
	ContentType string
	Schema      string // JSON Schema of the payload, if it can be validated
}

// SchemaConst is the name of the constant holding the schema.
func (m *message) SchemaConst() string { return "schema" + m.Name }

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

func (o *operation) IsSend() bool { return o.Action == spec.ActionSend }

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

// Part returns the part the operation is generated in.
func (o *operation) Part() Part {
	switch {
	case o.IsSend() && o.Reply == nil:
		return PartProducer
	case o.IsSend():
		return PartClient
	case o.Reply == nil:
		return PartConsumer
	default:
		return PartServer
	}
}

// selectParts reduces the model to the given parts; none means all of them.
func (m *model) selectParts(parts []Part) error {
	if len(parts) == 0 {
		parts = allParts
	}
	var ops []*operation
	used := map[*channel]bool{}
	for _, o := range m.Ops {
		if slices.Contains(parts, o.Part()) {
			ops = append(ops, o)
			used[o.Channel] = true
		}
	}
	m.Ops = ops
	m.Channels = slices.DeleteFunc(m.Channels, func(c *channel) bool { return !used[c] })
	m.declareSchemas()

	for _, t := range apiTypes {
		api := &apiType{Name: t.name, Role: t.role, Transport: t.transport}
		for _, o := range m.Ops {
			if o.Part() == t.part {
				api.Ops = append(api.Ops, o)
			}
		}
		if len(api.Ops) == 0 {
			continue
		}
		for _, d := range m.Types {
			if d.Name == t.name || d.Name == "New"+t.name {
				return fmt.Errorf("type %s generated from the document clashes with the generated %s", d.Name, t.name)
			}
		}
		m.APIs = append(m.APIs, api)
	}
	return nil
}

// declareSchemas lists the schemas the selected operations refer to.
func (m *model) declareSchemas() {
	seen := map[*message]bool{}
	add := func(msg *message) {
		if msg != nil && msg.Schema != "" && !seen[msg] {
			seen[msg] = true
			m.Schemas = append(m.Schemas, msg)
		}
	}
	for _, c := range m.Channels {
		add(c.Message)
	}
	for _, o := range m.Ops {
		add(o.Reply)
	}
	sort.Slice(m.Schemas, func(i, j int) bool { return m.Schemas[i].Name < m.Schemas[j].Name })
}

var paramRe = regexp.MustCompile(`\{([^{}]+)\}`)

type builder struct {
	doc      *spec.Document
	types    *typeSet
	channels map[string]*channel
	messages map[string]*message // by canonical ref
	msgNames map[string]bool
	headers  []*headerDecl
}

func buildModel(doc *spec.Document, pkg string, warn func(string)) (*model, error) {
	b := &builder{
		doc:      doc,
		types:    newTypeSet(doc, warn),
		channels: map[string]*channel{},
		messages: map[string]*message{},
		msgNames: map[string]bool{},
	}
	m := &model{Package: pkg, Title: doc.Info.Title, Version: doc.Info.Version}

	opKeys := sortedKeys(doc.Operations)
	opNames := map[string]string{}
	for _, key := range opKeys {
		op, err := b.operation(key, doc.Operations[key])
		if err != nil {
			return nil, fmt.Errorf("operation %q: %w", key, err)
		}
		if other, ok := opNames[op.Name]; ok {
			return nil, fmt.Errorf("operations %q and %q have the same Go name %s", other, key, op.Name)
		}
		opNames[op.Name] = key
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
	breakRecursion(b.types.decls)
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
	name := exportName(key)
	if name == "Client" {
		// The generated types embed *broker.Client, and a method cannot share
		// its name with a field.
		name = "ClientOp"
	}
	o := &operation{
		Name:    name,
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
		ch.Message = o.Message
	case o.Message.Type:
		// Messages sharing a type share a schema, unless one of them is not JSON.
		if ch.Message != nil && ch.Message.Schema != o.Message.Schema {
			b.types.warn("channel %q: its messages differ in being JSON, so payloads are not validated", ch.Key)
			ch.Message = nil
		}
	default:
		// TODO(phase 2): channels carrying several message types.
		return nil, fmt.Errorf("channel %q carries both %s and %s: one message type per channel is supported", ch.Key, ch.Payload, o.Message.Type)
	}
	if op.Reply != nil {
		reply, _, err := b.doc.ResolveReply(op.Reply)
		if err != nil {
			return nil, fmt.Errorf("reply: %w", err)
		}
		// Requests get their reply on the subject NATS provides with the
		// request, which leaves no address to honour.
		if reply.Address != nil {
			return nil, fmt.Errorf("reply.address is not supported: replies go to the NATS reply subject of the request, so remove it")
		}
		chRef := ""
		if reply.Channel != nil {
			chRef = reply.Channel.Ref
			replyCh, replyKey, err := b.doc.ChannelByRef(chRef)
			if err != nil {
				return nil, fmt.Errorf("reply: %w", err)
			}
			if replyCh.Address != nil {
				return nil, fmt.Errorf("reply channel %q has fixed address %q, which is not supported: replies go to the NATS reply subject of the request, so set address: null", replyKey, *replyCh.Address)
			}
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
	canonical, err := b.doc.CanonicalRef(ref)
	if err != nil {
		return nil, err
	}
	if m, ok := b.messages[canonical]; ok {
		return m, nil
	}
	msg, name, err := b.doc.MessageByRef(ref)
	if err != nil {
		return nil, err
	}
	if msg.Name != "" {
		name = msg.Name
	}
	goName := b.messageName(canonical, name)
	m := &message{Name: goName, ContentType: firstNonEmpty(msg.ContentType, b.doc.DefaultContentType, "application/json")}
	if !isJSON(m.ContentType) {
		b.types.warn("message %q: content type %q needs a matching codec: the default is JSON, set one with broker.WithCodec", name, m.ContentType)
	}
	if m.Type, err = b.types.namedType(msg.Payload, goName); err != nil {
		return nil, fmt.Errorf("message %q payload: %w", name, err)
	}
	if msg.Headers != nil {
		if err := b.headerDecl(msg.Headers, goName); err != nil {
			return nil, fmt.Errorf("message %q headers: %w", name, err)
		}
	}
	m.Schema = b.payloadSchema(msg, name, m.ContentType)
	b.messages[canonical] = m
	return m, nil
}

// payloadSchema returns the schema to validate a message's payload with. A
// schema that cannot be used only costs validation, so it is a warning:
// generated types do not depend on it.
func (b *builder) payloadSchema(msg *spec.Message, name, contentType string) string {
	if !isJSON(contentType) {
		return ""
	}
	schema, err := b.doc.PayloadSchema(msg)
	if err == nil && schema != "" {
		_, err = schemavalidate.New().Compile(schema)
	}
	if err != nil {
		b.types.warn("message %q: payload is not validated by broker.WithValidator: %v", name, err)
		return ""
	}
	return schema
}

// isJSON reports whether a message's content type is JSON.
func isJSON(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), "json")
}

// messageName returns a Go name no other message uses. Messages are named by
// their key or name, which only have to be unique within their channel or
// components section.
func (b *builder) messageName(canonical, name string) string {
	goName := exportName(name)
	if b.msgNames[goName] {
		if rest, ok := strings.CutPrefix(canonical, "#/channels/"); ok {
			channelKey, _, _ := strings.Cut(rest, "/")
			goName = exportName(channelKey) + goName
		} else {
			goName = exportName(spec.RefName(canonical))
		}
	}
	return uniqueIn(b.msgNames, goName)
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
	args := map[string]bool{}
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
		p := param{Arg: uniqueIn(args, argName(pkey)), Key: pkey}
		if raw, ok := ch.Parameters[pkey]; ok {
			def, _, err := b.doc.ResolveParameter(raw)
			if err != nil {
				return nil, fmt.Errorf("channel %q parameter %q: %w", key, pkey, err)
			}
			p.Doc, p.Enum, p.Default = def.Description, def.Enum, def.Default
			if def.Location != "" {
				b.types.warn("channel %q parameter %q: location is ignored, pass the value in the parameters", key, pkey)
			}
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
