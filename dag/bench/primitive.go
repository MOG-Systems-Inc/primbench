package bench

import (
	"errors"
	"fmt"

	"github.com/MOG-Systems-Inc/primbench/internal/ledger"
	"github.com/MOG-Systems-Inc/primbench/internal/schema"

	"github.com/nats-io/nats-server/v2/server/gsl"
)

// Verb is what an identity asks to do with a subject.
type Verb string

const (
	Publish   Verb = "publish"
	Subscribe Verb = "subscribe"
)

// Refusal names why an action is refused. Allowed is the empty string.
type Refusal string

const (
	Allowed        Refusal = ""
	BadRecord      Refusal = "bad_record"  // not JSON, or no schema_id or id
	NotGranted     Refusal = "not_granted" // no grant pattern of the identity matches the subject
	NoStream       Refusal = "no_stream"   // no stream binds the subject: the family is not registered
	SchemaRejected Refusal = "schema"      // the record does not conform to its schema
)

// Decision is the outcome of one evaluation.
type Decision struct {
	Subject string
	Stream  string
	Refusal Refusal
	Err     error
}

// Primitive is the evaluation function over the three structures the system
// keys by one name: the schema registry (Kind and the validator), the subject
// tree (the stream bindings) and the grants (one subject matcher per identity
// and verb). Everything it holds is built once and read only.
//
// The subject matcher is the nats-server's own trie (server/gsl), the same
// token-wise algorithm the broker runs for a permission check and for routing
// a subject to a stream (server/sublist.go).
type Primitive struct {
	v       *schema.Validator
	streams *gsl.GenericSublist[string]
	grants  map[Verb]map[string]*gsl.GenericSublist[string]
}

// New builds the evaluator from a compiled registry, the stream topology in
// ledger.Streams and a grant list.
func New(v *schema.Validator, grants []Grant) (*Primitive, error) {
	p := &Primitive{
		v:       v,
		streams: gsl.NewSublist[string](),
		grants:  map[Verb]map[string]*gsl.GenericSublist[string]{Publish: {}, Subscribe: {}},
	}
	for _, s := range ledger.Streams {
		if err := p.streams.Insert(s.Subject, s.Name); err != nil {
			return nil, fmt.Errorf("stream %s: %w", s.Name, err)
		}
	}
	for _, g := range grants {
		for verb, pats := range map[Verb][]string{Publish: g.Publish, Subscribe: g.Subscribe} {
			sl := gsl.NewSublist[string]()
			for _, pat := range pats {
				if err := sl.Insert(pat, pat); err != nil {
					return nil, fmt.Errorf("grant %s %s %q: %w", g.Identity, verb, pat, err)
				}
			}
			p.grants[verb][g.Identity] = sl
		}
	}
	return p, nil
}

// Granted reports whether identity's grant for verb matches subject. An
// identity with no grant is granted nothing.
func (p *Primitive) Granted(identity string, verb Verb, subject string) bool {
	sl := p.grants[verb][identity]
	return sl != nil && sl.NumInterest(subject) > 0
}

// StreamOf is the stream that binds subject, or "" when none does.
func (p *Primitive) StreamOf(subject string) string {
	var name string
	p.streams.Match(subject, func(s string) { name = s })
	return name
}

// SubjectOf is the subject a record is written to, and its family:
// <root>.<Kind(schema_id)>.<id>, exactly as ledger.SubjectFor builds it.
func SubjectOf(schemaID, nodeID, edgeID string) (subject, family string) {
	r := ledger.Record{SchemaID: schemaID, NodeID: nodeID, EdgeID: edgeID}
	return ledger.SubjectFor(r), ledger.Kind(schemaID)
}

// envelope is the part of a record the evaluation reads before validation.
type envelope struct {
	NodeID, EdgeID, SchemaID string
}

var errNoID = errors.New("record has no schema_id or id")

// decodeRecord decodes a record once: the envelope's three fields are read
// from the decoded value, and the validator walks the same value.
func decodeRecord(raw []byte) (any, envelope, error) {
	inst, err := schema.Decode(raw)
	if err != nil {
		return nil, envelope{}, err
	}
	obj, _ := inst.(map[string]any)
	var e envelope
	e.SchemaID, _ = obj["schema_id"].(string)
	e.NodeID, _ = obj["node_id"].(string)
	e.EdgeID, _ = obj["edge_id"].(string)
	if e.SchemaID == "" || (e.NodeID == "" && e.EdgeID == "") {
		return nil, envelope{}, errNoID
	}
	return inst, e, nil
}

// CheckPublish evaluates one write: identity publishes the record raw.
//
//	Kind(schema_id) -> subject -> grant matches -> stream binds -> schema validates
//
// The grant is checked before the stream because the broker does the same:
// it refuses a publish the identity is not granted before it routes the
// subject to a stream. The order changes which refusal is reported, never
// whether one is.
func (p *Primitive) CheckPublish(identity string, raw []byte) Decision {
	inst, e, err := decodeRecord(raw)
	if err != nil {
		return Decision{Refusal: BadRecord, Err: err}
	}
	subject, _ := SubjectOf(e.SchemaID, e.NodeID, e.EdgeID)
	d := Decision{Subject: subject}
	if !p.Granted(identity, Publish, subject) {
		d.Refusal = NotGranted
		return d
	}
	if d.Stream = p.StreamOf(subject); d.Stream == "" {
		d.Refusal = NoStream
		return d
	}
	if err := p.v.ValidateValue(e.SchemaID, inst); err != nil {
		d.Refusal, d.Err = SchemaRejected, err
	}
	return d
}

// CheckSubscribe evaluates a read of the log: identity subscribes to the
// literal subject. It is the grant alone. (A wildcard filter is granted by the
// broker when it is a subset of an allow pattern; that case is not modelled
// here, and nothing in the benchmark asks it.)
func (p *Primitive) CheckSubscribe(identity, subject string) Decision {
	d := Decision{Subject: subject, Stream: p.StreamOf(subject)}
	if !p.Granted(identity, Subscribe, subject) {
		d.Refusal = NotGranted
	}
	return d
}
