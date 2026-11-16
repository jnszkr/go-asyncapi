package codegen

import (
	"strings"
	"testing"
)

func TestCheckUniqueNames(t *testing.T) {
	tests := []struct {
		name, src, dup string
	}{
		{"methods may share names", "package p\ntype A struct{}\ntype B struct{}\nfunc (A) Validate() {}\nfunc (B) Validate() {}\n", ""},
		{"duplicate functions", "package p\nfunc NewA() {}\nfunc NewA() {}\n", "NewA"},
		{"type and function", "package p\ntype A struct{}\nfunc A() {}\n", "A"},
		{"duplicate values", "package p\nconst X = 1\nvar X = 2\n", "X"},
		{"first duplicate wins", "package p\nconst X, Y = 1, 2\nvar Y, X = 1, 2\n", "Y"},
		{"distinct names", "package p\ntype A struct{}\nfunc NewA() {}\nconst B = 1\n", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkUniqueNames([]byte(tt.src))
			switch {
			case tt.dup == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			case tt.dup != "" && (err == nil || !strings.HasPrefix(err.Error(), tt.dup+" would be generated twice")):
				t.Errorf("err = %v, want duplicate %s", err, tt.dup)
			}
		})
	}
	if err := checkUniqueNames([]byte("not go")); err == nil {
		t.Error("expected a parse error")
	}
}
