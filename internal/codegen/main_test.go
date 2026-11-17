package codegen

import (
	"fmt"
	"os"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"testing"
	"time"

	"github.com/jnszkr/go-asyncapi/internal/spec"
)

// Limits for a test run. The tests stay far below them; a run that reaches
// one has a bug, typically a mutant from mutation testing that recurses or
// loops forever. Without them such a run keeps allocating until it is timed
// out, which takes minutes, and macOS does not kill a process that exhausts
// memory: it swaps until the whole machine stops.
const (
	maxStack = 64 << 20 // per goroutine; the default is 1 GB
	maxHeap  = 1 << 30
)

func TestMain(m *testing.M) {
	debug.SetMaxStack(maxStack)
	go limitHeap(maxHeap, 50*time.Millisecond)
	os.Exit(m.Run())
}

// limitHeap crashes the process once heap objects and goroutine stacks
// together exceed limit.
func limitHeap(limit uint64, every time.Duration) {
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/stacks:bytes"},
	}
	for range time.Tick(every) {
		metrics.Read(samples)
		if used := samples[0].Value.Uint64() + samples[1].Value.Uint64(); used > limit {
			panic(fmt.Sprintf("test used %d MB, over the %d MB limit: runaway recursion or loop?", used>>20, limit>>20))
		}
	}
}

// TestDeclareSelfReference checks the guard that turns a schema leading back
// to itself into an error. No valid document reaches it, so it is called
// directly.
func TestDeclareSelfReference(t *testing.T) {
	ts := newTypeSet(&spec.Document{}, func(string) {})
	s := &spec.Schema{Type: "string"}
	ts.declaring[s] = true
	err := ts.declare(s, "Name")
	if err == nil || !strings.Contains(err.Error(), "Name: schema refers to itself") {
		t.Fatalf("declare = %v, want a self-reference error", err)
	}
	// The guard is released afterwards: declaring the schema normally works.
	delete(ts.declaring, s)
	if err := ts.declare(s, "Other"); err != nil {
		t.Fatal(err)
	}
	if len(ts.declaring) != 0 {
		t.Errorf("declaring = %v after declare returned", ts.declaring)
	}
}
