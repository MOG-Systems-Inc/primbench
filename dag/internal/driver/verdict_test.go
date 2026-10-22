package driver

import (
	"strings"
	"testing"
	"time"

	"github.com/MOG-Systems-Inc/primbench/bench"
)

func TestVerdicts(t *testing.T) {
	m := &measurement{run: map[string]bool{"eval": true}, results: map[string][]bench.Result{"eval": {
		{Name: "local: one process", Concurrency: 1, P50: 6 * time.Microsecond},
		{Name: "local: one process", Concurrency: 8, P50: time.Second},
		{Name: "three services, in sequence", Concurrency: 1, P50: 150 * time.Microsecond},
	}}}
	v := m.verdicts(2)
	if len(v) != 1 || !v[0].pass || !strings.HasPrefix(v[0].text, "evaluation: local 6.0 µs (published 6.0)") || !strings.HasSuffix(v[0].text, "within 2x: PASS") {
		t.Fatalf("within tolerance: %+v", v)
	}
	m.results["eval"][0].P50 = 13 * time.Microsecond
	if v := m.verdicts(2); v[0].pass || !strings.HasSuffix(v[0].text, ": FAIL") {
		t.Fatalf("over 2x slower passed: %+v", v)
	}
	m.results["eval"][0].P50 = 2 * time.Microsecond
	if v := m.verdicts(2); v[0].pass {
		t.Fatalf("over 2x faster passed: %+v", v)
	}
	m.results["eval"][0].P50 = 6 * time.Microsecond
	m.results["eval"][2].Errors = 1
	if v := m.verdicts(2); v[0].pass {
		t.Fatalf("a wrong answer passed: %+v", v)
	}
}
