package codegen

import "testing"

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
