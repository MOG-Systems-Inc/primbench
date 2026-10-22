// Package ledger is the part of the write path the benchmark evaluates
// against: the family of a schema identifier (Kind), the subject a record is
// written to (SubjectFor) and the stream table that binds subjects to
// streams (Streams).
//
// A subject has five tokens: tenant.<tenant>.dag.<family>.<id>. The sample
// tenant is acme. The stream table holds one stream per record family, and a
// schema identifier whose family has no stream has no subject a stream
// binds, so the broker refuses the publish.
package ledger

import (
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FirstTenant is the tenant the benchmark writes and reads as.
const FirstTenant = "acme"

// SubjectRoot prefixes every subject a record of FirstTenant is written to.
const SubjectRoot = "tenant." + FirstTenant + ".dag"

// KindUnknown is the family of a schema identifier the family map lacks.
// No stream binds <root>.unknown.>.
const KindUnknown = "unknown"

// SeedFamilies is the family of each schema the registry embeds, keyed by
// its identifier: the compiled table the family map is seeded from. A test
// fails when an embedded schema has no family here, or this table names a
// schema the registry does not embed.
var SeedFamilies = map[string]string{
	"sch_Asset_ProRes_v2":    "asset",
	"sch_Edge_Derivation_v1": "edge",
	"sch_Tag_v1":             "tag",
}

// families is the map Kind reads. A lookup takes no lock; RegisterFamily
// copies the map and swaps it in whole.
var (
	families   atomic.Pointer[map[string]string]
	registerMu sync.Mutex
)

func init() {
	m := maps.Clone(SeedFamilies)
	families.Store(&m)
}

// Kind is the record family of a schema identifier: one lookup in the family
// map. An identifier the map lacks is KindUnknown.
func Kind(schemaID string) string {
	if f, ok := (*families.Load())[schemaID]; ok {
		return f
	}
	return KindUnknown
}

// RegisterFamily adds a schema identifier's family to the map, as a schema
// that arrives at run time does. The same family again changes nothing;
// another family for a known identifier is refused, since an identifier is
// immutable.
func RegisterFamily(schemaID, family string) error {
	if schemaID == "" || family == "" || family == KindUnknown {
		return fmt.Errorf("ledger: refusing family %q for %q", family, schemaID)
	}
	registerMu.Lock()
	defer registerMu.Unlock()
	cur := *families.Load()
	if f, ok := cur[schemaID]; ok {
		if f != family {
			return fmt.Errorf("ledger: %s is family %s, not %s", schemaID, f, family)
		}
		return nil
	}
	next := maps.Clone(cur)
	next[schemaID] = family
	families.Store(&next)
	return nil
}

// Record is the part of a record's envelope that names its subject.
type Record struct {
	NodeID, EdgeID, SchemaID string
}

// ID is the node id, or the edge id when the record is an edge.
func (r Record) ID() string {
	if r.NodeID != "" {
		return r.NodeID
	}
	return r.EdgeID
}

// SubjectFor is the subject a record is written to:
// <root>.<Kind(schema_id)>.<id>.
func SubjectFor(r Record) string {
	return SubjectRoot + "." + Kind(r.SchemaID) + "." + r.ID()
}

// Streams is the stream table: one stream per record family, each over
// <root>.<family>.>. Subjects do not overlap.
var Streams = []struct {
	Name, Subject     string
	MaxMsgsPerSubject int64         // 0: unlimited
	MaxAge            time.Duration // 0: never expires
}{
	{Name: "DAG_ASSET", Subject: SubjectRoot + ".asset.>"},
	{Name: "DAG_EDGE", Subject: SubjectRoot + ".edge.>"},
	{Name: "DAG_TAG", Subject: SubjectRoot + ".tag.>"},
}

// BenchScratchStream is the stream the network mode publishes to. Its
// subjects are a tree under the tenant's root that no record family uses,
// so a timed publish never lands in a record stream.
const BenchScratchStream = "BENCH_SCRATCH"

// BenchScratchSubjects is the scratch stream's subject filter: <root>.bench.>.
func BenchScratchSubjects() string { return SubjectRoot + ".bench.>" }

// BenchScratchSubject moves a record's subject under the scratch tree:
// <root>.<family>.<id> becomes <root>.bench.<family>.<id>.
func BenchScratchSubject(subject string) string {
	return SubjectRoot + ".bench." + strings.TrimPrefix(subject, SubjectRoot+".")
}
