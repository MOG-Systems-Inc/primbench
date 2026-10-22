package ledger

import (
	"testing"

	"github.com/MOG-Systems-Inc/primbench/internal/schema"
)

// TestEverySchemaNamesItsFamily holds the family map's seed to the registry:
// every embedded schema has a family, and the seed names no schema the
// registry does not embed.
func TestEverySchemaNamesItsFamily(t *testing.T) {
	v, err := schema.Load("")
	if err != nil {
		t.Fatal(err)
	}
	embedded := map[string]bool{}
	for _, id := range v.IDs() {
		embedded[id] = true
		if Kind(id) == KindUnknown {
			t.Errorf("%s is embedded and has no family", id)
		}
	}
	for id := range SeedFamilies {
		if !embedded[id] {
			t.Errorf("the seed names %s, which the registry does not embed", id)
		}
	}
}

func TestKind(t *testing.T) {
	for id, want := range map[string]string{
		"sch_Asset_ProRes_v2":    "asset",
		"sch_Edge_Derivation_v1": "edge",
		"sch_Tag_v1":             "tag",
		"sch_Asset_ProRes_v9":    KindUnknown, // a family is per identifier, not per prefix
		"sch_Widget_v1":          KindUnknown,
		"":                       KindUnknown,
	} {
		if got := Kind(id); got != want {
			t.Errorf("Kind(%q) = %q, want %q", id, got, want)
		}
	}
}

func TestRegisterFamily(t *testing.T) {
	if err := RegisterFamily("sch_Voucher_v1", "voucher"); err != nil {
		t.Fatal(err)
	}
	if got := Kind("sch_Voucher_v1"); got != "voucher" {
		t.Errorf("Kind after register = %q, want voucher", got)
	}
	if err := RegisterFamily("sch_Voucher_v1", "voucher"); err != nil {
		t.Errorf("the same family again: %v", err)
	}
	if err := RegisterFamily("sch_Tag_v1", "asset"); err == nil {
		t.Error("another family for a known identifier was accepted")
	}
	if err := RegisterFamily("sch_X_v1", KindUnknown); err == nil {
		t.Error("the unknown family was accepted")
	}
}

func BenchmarkKind(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = Kind("sch_Asset_ProRes_v2")
	}
}
