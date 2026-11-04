package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// PayloadSchema returns the payload schema of a resolved message as a
// self-contained JSON Schema draft-07 document, or "" if it has no payload.
// It keeps the keywords the Schema type does not model, such as minimum or
// then. Every local $ref becomes a reference into the document's definitions,
// so recursive schemas stay finite. The output is deterministic.
func (d *Document) PayloadSchema(m *Message) (string, error) {
	payload := m.raw["payload"]
	if payload == nil {
		return "", nil
	}
	b := &bundler{doc: d, names: map[string]string{}, defs: map[string]any{}}
	root, err := b.schema(payload)
	if err != nil {
		return "", err
	}
	if obj, ok := root.(map[string]any); ok && len(b.defs) > 0 {
		obj["definitions"] = b.defs
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(root); err != nil {
		return "", err
	}
	return strings.TrimSpace(buf.String()), nil
}

// bundler copies schemas out of the document, collecting what they refer to.
type bundler struct {
	doc   *Document
	names map[string]string // $ref → key in defs
	defs  map[string]any
}

// droppedKeywords have no effect on validation. definitions only matters
// through refs, which are resolved against the document; $id would change what
// refs are relative to.
var droppedKeywords = map[string]bool{
	"$comment": true, "$id": true, "$schema": true, "default": true, "definitions": true,
	"deprecated": true, "description": true, "discriminator": true, "example": true,
	"examples": true, "externalDocs": true, "readOnly": true, "title": true, "writeOnly": true,
	"xml": true,
}

func (b *bundler) schema(node any) (any, error) {
	obj, ok := node.(map[string]any)
	if !ok {
		return node, nil // a boolean schema, or the names in dependencies
	}
	_, hasFormat := obj["schemaFormat"]
	if _, hasInner := obj["schema"]; hasFormat || hasInner {
		format, _ := obj["schemaFormat"].(string)
		if !supportedSchemaFormat(format) {
			return nil, fmt.Errorf("unsupported schemaFormat %q", format)
		}
		return b.schema(obj["schema"])
	}
	if ref, ok := obj["$ref"].(string); ok {
		name, err := b.define(ref)
		if err != nil {
			return nil, err
		}
		return map[string]any{"$ref": "#/definitions/" + name}, nil
	}
	out := make(map[string]any, len(obj))
	for _, k := range slices.Sorted(maps.Keys(obj)) {
		if droppedKeywords[k] || strings.HasPrefix(k, "x-") {
			continue
		}
		var err error
		if out[k], err = b.keyword(k, obj[k]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// keyword copies the value of a keyword. Only some keywords hold schemas: the
// contents of others, such as enum or const, merely can look like schemas.
func (b *bundler) keyword(k string, v any) (any, error) {
	switch k {
	case "additionalItems", "additionalProperties", "contains", "else", "if", "not", "propertyNames", "then":
		return b.schema(v)
	case "allOf", "anyOf", "items", "oneOf":
		return b.schemas(v)
	case "dependencies", "patternProperties", "properties":
		return b.schemaMap(v)
	}
	return v, nil
}

// schemas reads a list of schemas. A single schema is accepted too, since
// items can be either.
func (b *bundler) schemas(node any) (any, error) {
	list, ok := node.([]any)
	if !ok {
		return b.schema(node)
	}
	out := make([]any, len(list))
	for i, v := range list {
		var err error
		if out[i], err = b.schema(v); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (b *bundler) schemaMap(node any) (any, error) {
	obj, ok := node.(map[string]any)
	if !ok {
		return node, nil
	}
	out := make(map[string]any, len(obj))
	for _, k := range slices.Sorted(maps.Keys(obj)) {
		var err error
		if out[k], err = b.schema(obj[k]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

var definitionNameRe = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// define adds what ref points to to the definitions and returns its key.
func (b *bundler) define(ref string) (string, error) {
	if name, ok := b.names[ref]; ok {
		return name, nil
	}
	node, err := b.doc.lookup(ref)
	if err != nil {
		return "", err
	}
	base := RefName(ref)
	if !definitionNameRe.MatchString(base) {
		base = "schema"
	}
	name := base
	for i := 2; ; i++ {
		if _, taken := b.defs[name]; !taken {
			break
		}
		name = base + strconv.Itoa(i)
	}
	// Register before descending so recursive schemas terminate.
	b.names[ref] = name
	b.defs[name] = nil
	if b.defs[name], err = b.schema(node); err != nil {
		return "", err
	}
	return name, nil
}
