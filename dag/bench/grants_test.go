package bench

import (
	"strings"
	"testing"
)

// The grants keep the shape the write-up reports: four to nine publish
// patterns per identity, and the JetStream API subjects collapsed into one
// $JS.API.> pattern.
func TestGrantShape(t *testing.T) {
	for _, g := range DevGrants() {
		if n := len(g.Publish); n < 4 || n > 9 {
			t.Errorf("%s: %d publish patterns, want four to nine", g.Identity, n)
		}
		for _, p := range g.Publish {
			if strings.HasPrefix(p, "$JS.API.") && p != "$JS.API.>" {
				t.Errorf("%s: %s is a JetStream API subject not collapsed into $JS.API.>", g.Identity, p)
			}
		}
	}
}
