package codegen

import (
	"slices"
	"testing"
)

func TestExportName(t *testing.T) {
	tests := map[string]string{
		"userId":              "UserID",
		"order_id":            "OrderID",
		"user.signed-up":      "UserSignedUp",
		"HTTPServer":          "HTTPServer",
		"publishOrderCreated": "PublishOrderCreated",
		"sku":                 "SKU",
		"2fa":                 "X2fa",
		"":                    "X",
		"api_url":             "APIURL",
	}
	for in, want := range tests {
		if got := exportName(in); got != want {
			t.Errorf("exportName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSplitWords(t *testing.T) {
	tests := map[string][]string{
		"HTTPServer":     {"HTTP", "Server"},
		"userID":         {"user", "ID"},
		"order_id":       {"order", "id"},
		"v2Beta":         {"v2", "Beta"},
		"user.signed-up": {"user", "signed", "up"},
		"ABC":            {"ABC"},
	}
	for in, want := range tests {
		if got := splitWords(in); !slices.Equal(got, want) {
			t.Errorf("splitWords(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestArgName(t *testing.T) {
	tests := map[string]string{
		"userId":   "userID",
		"id":       "id",
		"IDField":  "idField",
		"url_path": "urlPath",
		"type":     "type_",
		"ctx":      "ctx_",
		"Name":     "name",
		"":         "x",
	}
	for in, want := range tests {
		if got := argName(in); got != want {
			t.Errorf("argName(%q) = %q, want %q", in, got, want)
		}
	}
}
