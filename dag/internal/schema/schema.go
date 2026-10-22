// Package schema is the schema registry the write path validates against:
// every *.schema.json under schemas/, compiled once at load (JSON Schema
// draft 2020-12), keyed by schema identifier.
package schema

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed schemas/*.schema.json
var embedded embed.FS

// Validator holds the compiled schemas. It is read only after Load and safe
// for concurrent use.
type Validator struct {
	byID map[string]*jsonschema.Schema
}

// Load compiles the embedded registry. The argument is kept for the call
// shape of the original harness and is ignored.
func Load(string) (*Validator, error) {
	fsys, err := fs.Sub(embedded, "schemas")
	if err != nil {
		return nil, err
	}
	files, err := fs.Glob(fsys, "*.schema.json")
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	targets := map[string]string{}
	for _, f := range files {
		b, err := fs.ReadFile(fsys, f)
		if err != nil {
			return nil, err
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		m, ok := doc.(map[string]any)
		if !ok {
			continue
		}
		id, _ := m["$id"].(string)
		if id == "" {
			continue
		}
		if err := c.AddResource(id, doc); err != nil {
			return nil, fmt.Errorf("add %s: %w", f, err)
		}
		if base := strings.TrimSuffix(path.Base(f), ".schema.json"); strings.HasPrefix(base, "sch_") {
			targets[base] = id
		}
	}
	v := &Validator{byID: make(map[string]*jsonschema.Schema, len(targets))}
	for schemaID, id := range targets {
		sch, err := c.Compile(id)
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", schemaID, err)
		}
		v.byID[schemaID] = sch
	}
	return v, nil
}

// IDs is every schema identifier the registry holds, sorted.
func (v *Validator) IDs() []string {
	ids := make([]string, 0, len(v.byID))
	for id := range v.byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Validate checks a raw record against its schema. An unknown identifier is
// an error.
func (v *Validator) Validate(schemaID string, record []byte) error {
	sch, ok := v.byID[schemaID]
	if !ok {
		return fmt.Errorf("unknown schema_id %q", schemaID)
	}
	inst, err := Decode(record)
	if err != nil {
		return err
	}
	return sch.Validate(inst)
}

// ValidateValue checks a record already decoded by Decode against its
// schema, so a caller that has decoded the record once does not decode it
// again.
func (v *Validator) ValidateValue(schemaID string, inst any) error {
	sch, ok := v.byID[schemaID]
	if !ok {
		return fmt.Errorf("unknown schema_id %q", schemaID)
	}
	return sch.Validate(inst)
}
