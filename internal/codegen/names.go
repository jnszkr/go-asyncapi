package codegen

import (
	"strings"
	"unicode"
)

// commonInitialisms are rendered in upper case, following Go naming conventions.
var commonInitialisms = map[string]bool{
	"ACL": true, "API": true, "ASCII": true, "CPU": true, "CSS": true, "DNS": true,
	"EOF": true, "GUID": true, "HTML": true, "HTTP": true, "HTTPS": true, "ID": true,
	"IP": true, "JSON": true, "LHS": true, "QPS": true, "RAM": true, "RHS": true,
	"RPC": true, "SLA": true, "SKU": true, "SMTP": true, "SQL": true, "SSH": true, "TCP": true,
	"TLS": true, "TTL": true, "UDP": true, "UI": true, "UID": true, "UUID": true,
	"URI": true, "URL": true, "UTF8": true, "VM": true, "XML": true, "XMPP": true,
}

// exportName converts an arbitrary identifier ("user.signed-up", "userId",
// "order_id") into an exported Go identifier ("UserSignedUp", "UserID", "OrderID").
func exportName(s string) string {
	var b strings.Builder
	for _, w := range splitWords(s) {
		if u := strings.ToUpper(w); commonInitialisms[u] {
			b.WriteString(u)
			continue
		}
		r := []rune(w)
		b.WriteRune(unicode.ToUpper(r[0]))
		b.WriteString(string(r[1:]))
	}
	name := b.String()
	if name == "" {
		return "X"
	}
	if unicode.IsDigit([]rune(name)[0]) {
		name = "X" + name
	}
	return name
}

// splitWords splits on non-alphanumerics and lower→upper case transitions,
// keeping runs of capitals ("HTTPServer" → "HTTP", "Server") together.
func splitWords(s string) []string {
	var words []string
	var cur []rune
	flush := func() {
		if len(cur) > 0 {
			words = append(words, string(cur))
			cur = cur[:0]
		}
	}
	rs := []rune(s)
	for i, r := range rs {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()
			continue
		}
		if len(cur) > 0 && unicode.IsUpper(r) {
			prev := cur[len(cur)-1]
			nextLower := i+1 < len(rs) && unicode.IsLower(rs[i+1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
				flush()
			}
		}
		cur = append(cur, r)
	}
	flush()
	return words
}
