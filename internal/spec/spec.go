// Package spec models the subset of the AsyncAPI 3.0 document that the
// generator understands, including the NATS bindings.
package spec

// Document is an AsyncAPI 3.0 document.
type Document struct {
	AsyncAPI           string                `yaml:"asyncapi"`
	ID                 string                `yaml:"id"`
	Info               Info                  `yaml:"info"`
	DefaultContentType string                `yaml:"defaultContentType"`
	Servers            map[string]*Server    `yaml:"servers"`
	Channels           map[string]*Channel   `yaml:"channels"`
	Operations         map[string]*Operation `yaml:"operations"`
	Components         Components            `yaml:"components"`

	// raw is the untyped document tree, used to resolve $refs.
	raw any
}

type Info struct {
	Title       string `yaml:"title"`
	Version     string `yaml:"version"`
	Description string `yaml:"description"`
}

type Server struct {
	Ref         string          `yaml:"$ref"`
	Host        string          `yaml:"host"`
	Protocol    string          `yaml:"protocol"`
	Pathname    string          `yaml:"pathname"`
	Description string          `yaml:"description"`
	Bindings    *ServerBindings `yaml:"bindings"`
}

type Channel struct {
	Ref         string                `yaml:"$ref"`
	Address     *string               `yaml:"address"`
	Title       string                `yaml:"title"`
	Summary     string                `yaml:"summary"`
	Description string                `yaml:"description"`
	Messages    map[string]*Message   `yaml:"messages"`
	Parameters  map[string]*Parameter `yaml:"parameters"`
	Bindings    *ChannelBindings      `yaml:"bindings"`
}

type Parameter struct {
	Ref         string   `yaml:"$ref"`
	Enum        []string `yaml:"enum"`
	Default     string   `yaml:"default"`
	Description string   `yaml:"description"`
	Examples    []string `yaml:"examples"`
	Location    string   `yaml:"location"`
}

type Action string

const (
	ActionSend    Action = "send"
	ActionReceive Action = "receive"
)

type Operation struct {
	Ref         string             `yaml:"$ref"`
	Action      Action             `yaml:"action"`
	Channel     *Reference         `yaml:"channel"`
	Title       string             `yaml:"title"`
	Summary     string             `yaml:"summary"`
	Description string             `yaml:"description"`
	Messages    []*Reference       `yaml:"messages"`
	Reply       *OperationReply    `yaml:"reply"`
	Bindings    *OperationBindings `yaml:"bindings"`
}

type OperationReply struct {
	Ref      string       `yaml:"$ref"`
	Channel  *Reference   `yaml:"channel"`
	Messages []*Reference `yaml:"messages"`
	Address  *ReplyAddr   `yaml:"address"`
}

type ReplyAddr struct {
	Ref         string `yaml:"$ref"`
	Location    string `yaml:"location"`
	Description string `yaml:"description"`
}

type Reference struct {
	Ref string `yaml:"$ref"`
}

type Message struct {
	Ref           string           `yaml:"$ref"`
	Name          string           `yaml:"name"`
	Title         string           `yaml:"title"`
	Summary       string           `yaml:"summary"`
	Description   string           `yaml:"description"`
	ContentType   string           `yaml:"contentType"`
	Headers       *Schema          `yaml:"headers"`
	Payload       *Schema          `yaml:"payload"`
	CorrelationID any              `yaml:"correlationId"`
	Bindings      *MessageBindings `yaml:"bindings"`
}

// Schema is a JSON Schema (draft-07 subset) as used by AsyncAPI.
type Schema struct {
	Ref                  string             `yaml:"$ref"`
	Type                 string             `yaml:"type"`
	Format               string             `yaml:"format"`
	Title                string             `yaml:"title"`
	Description          string             `yaml:"description"`
	Properties           map[string]*Schema `yaml:"properties"`
	Required             []string           `yaml:"required"`
	Items                *Schema            `yaml:"items"`
	Enum                 []any              `yaml:"enum"`
	AdditionalProperties any                `yaml:"additionalProperties"`
	OneOf                []*Schema          `yaml:"oneOf"`
	AnyOf                []*Schema          `yaml:"anyOf"`
	AllOf                []*Schema          `yaml:"allOf"`
}

type Components struct {
	Schemas        map[string]*Schema         `yaml:"schemas"`
	Servers        map[string]*Server         `yaml:"servers"`
	Channels       map[string]*Channel        `yaml:"channels"`
	Operations     map[string]*Operation      `yaml:"operations"`
	Messages       map[string]*Message        `yaml:"messages"`
	Parameters     map[string]*Parameter      `yaml:"parameters"`
	Replies        map[string]*OperationReply `yaml:"replies"`
	ReplyAddresses map[string]*ReplyAddr      `yaml:"replyAddresses"`
}

// Bindings. Only the NATS ones are modelled; other protocols are ignored.

type ServerBindings struct {
	NATS *NATSServerBinding `yaml:"nats"`
}

type ChannelBindings struct {
	NATS *NATSChannelBinding `yaml:"nats"`
}

type OperationBindings struct {
	NATS *NATSOperationBinding `yaml:"nats"`
}

type MessageBindings struct {
	NATS *NATSMessageBinding `yaml:"nats"`
}

// NATSServerBinding is reserved for future use by the binding spec.
type NATSServerBinding struct{}

// NATSChannelBinding is reserved for future use by the binding spec.
type NATSChannelBinding struct{}

// NATSMessageBinding is reserved for future use by the binding spec.
type NATSMessageBinding struct{}

// NATSOperationBinding is the NATS operation binding (v0.1.0).
type NATSOperationBinding struct {
	// Queue is the name of the queue group to subscribe with. Max 255 chars.
	Queue          string `yaml:"queue"`
	BindingVersion string `yaml:"bindingVersion"`
}
