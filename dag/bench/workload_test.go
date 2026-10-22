package bench

import (
	"encoding/json"
	"testing"
)

// fields counts a record's fields: every top-level key but payload, and
// every key of the payload.
func fields(t *testing.T, raw []byte) int {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	n := 0
	for k, v := range m {
		if k == "payload" {
			n += len(v.(map[string]any))
			continue
		}
		n++
	}
	return n
}

// The three records keep the sizes the write-up reports, so the validation
// work behind its numbers is the same work.
func TestRecordSizes(t *testing.T) {
	for _, c := range []struct {
		shape, id    string
		bytes, count int
	}{
		{"tag", "nd_tag0000000001", 208, 8},
		{"edge", "ed_edge000000000001", 310, 10},
		{"asset", "nd_asset0000000001", 726, 21},
	} {
		raw := Record(c.shape, c.id)
		if len(raw) != c.bytes {
			t.Errorf("%s: %d bytes, want %d", c.shape, len(raw), c.bytes)
		}
		if n := fields(t, raw); n != c.count {
			t.Errorf("%s: %d fields, want %d", c.shape, n, c.count)
		}
	}
	mix := Mix()
	allowed := 0
	for _, a := range mix {
		if a.Want == Allowed {
			allowed++
		}
	}
	if len(mix) != 20 || allowed != 16 {
		t.Errorf("the workload has %d actions, %d allowed; want 20 and 16", len(mix), allowed)
	}
}
