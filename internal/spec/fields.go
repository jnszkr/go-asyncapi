package spec

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// shape lists the fields the specification defines for an object, so that a
// misspelled field is reported rather than silently ignored.
type shape struct {
	fields string
	// one are fields holding a single object of a known shape.
	one map[string]*shape
	// each are fields holding a map or list of objects of a known shape.
	each map[string]*shape
}

var (
	parameterShape    = &shape{fields: "enum default description examples location"}
	replyAddressShape = &shape{fields: "description location"}
	replyShape        = &shape{fields: "address channel messages", one: map[string]*shape{"address": replyAddressShape}}
	serverShape       = &shape{fields: "host protocol protocolVersion pathname description title summary variables security tags externalDocs bindings"}
	messageTraitShape = &shape{fields: "headers correlationId contentType name title summary description tags externalDocs bindings examples"}
	messageShape      = &shape{
		fields: messageTraitShape.fields + " payload traits",
		each:   map[string]*shape{"traits": messageTraitShape},
	}
	channelShape = &shape{
		fields: "address messages title summary description servers parameters tags externalDocs bindings",
		each:   map[string]*shape{"messages": messageShape, "parameters": parameterShape},
	}
	operationTraitShape = &shape{fields: "title summary description security tags externalDocs bindings"}
	operationShape      = &shape{
		fields: operationTraitShape.fields + " action channel traits messages reply",
		one:    map[string]*shape{"reply": replyShape},
		each:   map[string]*shape{"traits": operationTraitShape},
	}
	componentsShape = &shape{
		fields: "schemas servers channels operations messages securitySchemes serverVariables parameters correlationIds replies replyAddresses externalDocs tags operationTraits messageTraits serverBindings channelBindings operationBindings messageBindings",
		each: map[string]*shape{
			"servers":         serverShape,
			"channels":        channelShape,
			"operations":      operationShape,
			"messages":        messageShape,
			"parameters":      parameterShape,
			"replies":         replyShape,
			"replyAddresses":  replyAddressShape,
			"operationTraits": operationTraitShape,
			"messageTraits":   messageTraitShape,
		},
	}
	documentShape = &shape{
		fields: "asyncapi id info servers defaultContentType channels operations components",
		one: map[string]*shape{
			"info":       {fields: "title version description termsOfService contact license tags externalDocs"},
			"components": componentsShape,
		},
		each: map[string]*shape{"servers": serverShape, "channels": channelShape, "operations": operationShape},
	}
)

// checkFields reports fields of node that its shape does not define.
// Specification extensions (x-) are allowed, and a $ref stands for an object
// that is checked where it is defined.
func checkFields(node any, sh *shape, path string) []error {
	m, ok := node.(map[string]any)
	if !ok {
		return nil
	}
	if _, isRef := m["$ref"]; isRef {
		return nil
	}
	fields := strings.Fields(sh.fields)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var errs []error
	for _, k := range keys {
		if !slices.Contains(fields, k) && !strings.HasPrefix(k, "x-") {
			at := path
			if at == "" {
				at = "the document root"
			}
			errs = append(errs, fmt.Errorf("unknown field %q in %s", k, at))
			continue
		}
		childPath := path + "/" + k
		if child, ok := sh.one[k]; ok {
			errs = append(errs, checkFields(m[k], child, childPath)...)
		}
		if child, ok := sh.each[k]; ok {
			switch v := m[k].(type) {
			case map[string]any:
				names := make([]string, 0, len(v))
				for name := range v {
					names = append(names, name)
				}
				sort.Strings(names)
				for _, name := range names {
					errs = append(errs, checkFields(v[name], child, childPath+"/"+name)...)
				}
			case []any:
				for i, item := range v {
					errs = append(errs, checkFields(item, child, fmt.Sprintf("%s/%d", childPath, i))...)
				}
			}
		}
	}
	return errs
}
