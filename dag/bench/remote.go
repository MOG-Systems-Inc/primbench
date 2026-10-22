package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/MOG-Systems-Inc/primbench/internal/ledger"
	"github.com/MOG-Systems-Inc/primbench/internal/schema"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The comparison path is the same three checks done the way a standard stack
// does them, each as a call to its own service:
//
//   - the hierarchy, in Postgres: which stream (topic) a tenant's record of a
//     given schema is stored on, one row per (tenant, schema_id);
//   - the grants, in Redis or Valkey: one set per identity and verb holding
//     the families it may write, materialised from the same patterns (in a
//     Postgres table where no Valkey is reachable, GrantStore);
//   - the schema registry, over HTTP: POST /validate/<schema_id> with the
//     record, answered 204, 422 or 404 by the same compiled registry.
//
// The registry stand-in validates with the same code as the local path, so
// the validation work is equal on both sides and the difference is the hops.

// RegistryHandler is the HTTP schema registry stand-in.
func RegistryHandler(v *schema.Validator) http.Handler {
	known := map[string]bool{}
	for _, id := range v.IDs() {
		known[id] = true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /validate/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !known[id] {
			http.Error(w, "unknown schema_id", http.StatusNotFound)
			return
		}
		if err := v.Validate(id, body); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// Remote is the comparison path's clients. PG and HTTP are safe for
// concurrent use; each worker passes its own RESP connection.
type Remote struct {
	PG          *pgxpool.Pool
	HTTP        *http.Client
	RegistryURL string
	Tenant      string
}

// NewHTTPClient is a keep-alive client sized for conc workers.
func NewHTTPClient(conc int) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = conc * 2
	t.MaxIdleConnsPerHost = conc * 2
	t.MaxConnsPerHost = 0
	return &http.Client{Transport: t, Timeout: 10 * time.Second}
}

const hierarchyDDL = `
create table if not exists subject_tree (
  tenant    text not null,
  schema_id text not null,
  family    text not null,
  stream    text not null,
  primary key (tenant, schema_id)
)`

// Seed writes the comparison path's two stores from the same sources the
// local path reads: every schema in the registry whose family has a stream
// becomes a hierarchy row, and every identity's publish grant becomes the set
// of families it may write.
func Seed(ctx context.Context, pg *pgxpool.Pool, store GrantStore, v *schema.Validator, p *Primitive, grants []Grant, tenant string) error {
	if _, err := pg.Exec(ctx, hierarchyDDL); err != nil {
		return err
	}
	if _, err := pg.Exec(ctx, `delete from subject_tree where tenant = $1`, tenant); err != nil {
		return err
	}
	families := map[string]bool{}
	for _, id := range v.IDs() {
		subject, family := SubjectOf(id, "nd_probe0", "")
		stream := p.StreamOf(subject)
		if stream == "" {
			continue
		}
		families[family] = true
		if _, err := pg.Exec(ctx, `insert into subject_tree (tenant, schema_id, family, stream) values ($1,$2,$3,$4)`,
			tenant, id, family, stream); err != nil {
			return err
		}
	}
	for _, g := range grants {
		var members []string
		for f := range families {
			if p.Granted(g.Identity, Publish, ledger.SubjectRoot+"."+f+".nd_probe0") {
				members = append(members, f)
			}
		}
		if err := store.Replace(ctx, "grant:"+g.Identity+":publish", members); err != nil {
			return err
		}
	}
	return nil
}

// CheckPublish is the comparison path's evaluation of one write: the
// envelope is decoded locally, then Postgres answers the stream, the grant
// store the grant and the registry the schema, one after the other (parallel: the
// registry call runs beside the other two). The grant set is keyed by family
// and the family comes from the hierarchy row, so Postgres is asked first and
// an unregistered kind is refused as no_stream even for an identity that
// holds no grant on it; the local path, like the broker, reports not_granted
// there. Every action in Mix reaches the same refusal on both paths.
func (r *Remote) CheckPublish(ctx context.Context, grants GrantStore, identity string, raw []byte, parallel bool) Decision {
	_, e, err := decodeRecord(raw)
	if err != nil {
		return Decision{Refusal: BadRecord, Err: err}
	}
	subject, family := SubjectOf(e.SchemaID, e.NodeID, e.EdgeID)
	d := Decision{Subject: subject}

	var schemaErr error
	var schemaDone chan struct{}
	if parallel {
		schemaDone = make(chan struct{})
		go func() { schemaErr = r.validate(ctx, e.SchemaID, raw); close(schemaDone) }()
	}

	var stream string
	err = r.PG.QueryRow(ctx, `select family, stream from subject_tree where tenant = $1 and schema_id = $2`,
		r.Tenant, e.SchemaID).Scan(&family, &stream)
	if errors.Is(err, pgx.ErrNoRows) {
		stream = ""
	} else if err != nil {
		d.Refusal, d.Err = BadRecord, err
		return wait(d, schemaDone)
	}
	if stream == "" {
		d.Refusal = NoStream
		return wait(d, schemaDone)
	}
	ok, err := grants.Has(ctx, "grant:"+identity+":publish", family)
	if err != nil {
		d.Refusal, d.Err = BadRecord, err
		return wait(d, schemaDone)
	}
	if !ok {
		d.Refusal = NotGranted
		return wait(d, schemaDone)
	}
	d.Stream = stream
	if parallel {
		<-schemaDone
	} else {
		schemaErr = r.validate(ctx, e.SchemaID, raw)
	}
	if schemaErr != nil {
		d.Refusal, d.Err = SchemaRejected, schemaErr
	}
	return d
}

func wait(d Decision, done chan struct{}) Decision {
	if done != nil {
		<-done
	}
	return d
}

func (r *Remote) validate(ctx context.Context, schemaID string, raw []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.RegistryURL+"/validate/"+schemaID, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusUnprocessableEntity, http.StatusNotFound:
		return fmt.Errorf("registry %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
	return fmt.Errorf("registry: unexpected %d", resp.StatusCode)
}
