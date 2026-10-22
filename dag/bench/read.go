package bench

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/MOG-Systems-Inc/primbench/internal/readmodel"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The read side. Every read-model row carries its tenant, and a forced row
// policy compares it with mog_tenant(), the tenant set in the transaction
// (internal/readmodel). A read binds its transaction to the reader role
// and to the credential's tenant, then reads. The comparison does what a
// standard stack does instead: ask Redis whether the identity may read the
// tenant, then read with the tenant as a filter in the query.

// ReadTenants are the tenants the read benchmark's rows belong to.
var ReadTenants = []string{"acme", "globex", "initech"}

const readNodesDDL = `
create table if not exists nodes (
  node_id    text primary key,
  schema_id  text not null,
  kind       text not null,
  label      text not null,
  tenant_id  text not null,
  payload    jsonb not null,
  created_at timestamptz,
  updated_at timestamptz not null default now()
)`

// SeedReadModel creates the projector's nodes table, installs the read
// model's tenancy on it with readmodel.EnsureTenancy (the column, the forced
// policy, mog_tenant() and the reader role), and writes rows tag nodes, split
// across ReadTenants eight to one to one. It returns the ids by tenant. The
// pool's role must bypass row security, as the projector's does.
func SeedReadModel(ctx context.Context, pool *pgxpool.Pool, rows int) (map[string][]string, error) {
	// The seed drops and recreates nodes, so it runs only in a benchmark's
	// own database: the embedded one (bench) or a scratch one (bench_<unix>),
	// never a read model's.
	var db string
	if err := pool.QueryRow(ctx, `select current_database()`).Scan(&db); err != nil {
		return nil, err
	}
	if db != "bench" && !strings.HasPrefix(db, "bench_") {
		return nil, fmt.Errorf("refusing to seed database %q: the read benchmark drops nodes, so it runs only in a database named bench or bench_<suffix>", db)
	}
	if _, err := pool.Exec(ctx, `drop table if exists nodes`); err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, readNodesDDL); err != nil {
		return nil, err
	}
	ids := map[string][]string{}
	batch := &pgx.Batch{}
	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}
		err := pool.SendBatch(ctx, batch).Close()
		batch = &pgx.Batch{}
		return err
	}
	for i := 0; i < rows; i++ {
		t := ReadTenants[0]
		switch i % 10 {
		case 8:
			t = ReadTenants[1]
		case 9:
			t = ReadTenants[2]
		}
		id := fmt.Sprintf("nd_read%010d", i)
		ids[t] = append(ids[t], id)
		batch.Queue(`insert into nodes (node_id, schema_id, kind, label, tenant_id, payload, created_at)
			values ($1, 'sch_Tag_v1', 'tag', 'exercise: hip thrust', $2, $3::jsonb, now())`,
			id, t, `{"namespace":"exercise","value":"weighted hip thrust","origin":"vlm","confidence":0.91}`)
		if batch.Len() == 1000 {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, `analyze nodes`); err != nil {
		return nil, err
	}
	if err := readmodel.EnsureTenancy(ctx, pool, ReadTenants[0]); err != nil {
		return nil, err
	}
	return ids, nil
}

// SeedReadGrants writes the comparison path's read grants: identity
// "reader-<tenant>" may read its own tenant.
func SeedReadGrants(ctx context.Context, store GrantStore) error {
	for _, t := range ReadTenants {
		if err := store.Replace(ctx, "grant:reader-"+t+":read", []string{t}); err != nil {
			return err
		}
	}
	return nil
}

const readSQL = `select payload::text from nodes where node_id = $1`

// PolicyRead is the bound read in three round trips: one opens a read-only
// transaction as the reader role with the tenant set (simple protocol, so
// BEGIN and the settings travel together), one reads, one commits. found
// reports whether the row came back. The services bound a read this way
// before they pipelined it; PipelinedPolicyRead is how they bind it now.
func PolicyRead(ctx context.Context, pool *pgxpool.Pool, tenant, id string) (found bool, err error) {
	c, err := pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer c.Release()
	if !readmodel.IsTenant(tenant) {
		return false, fmt.Errorf("not a tenant %q", tenant)
	}
	if _, err := c.Exec(ctx, `begin read only; select set_config('role', '`+readmodel.ReaderRole+`', true), set_config('mog.tenant', '`+tenant+`', true)`); err != nil {
		return false, err
	}
	var payload string
	err = c.QueryRow(ctx, readSQL, id).Scan(&payload)
	found = err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	if _, cerr := c.Exec(ctx, `commit`); cerr != nil && err == nil {
		err = cerr
	}
	return found, err
}

// PipelinedPolicyRead is the same bound read in one round trip, as the
// services bind it: readmodel.Bind sends the reader role, the tenant as a
// bound parameter and the transaction's read-only flag, then the read, as
// one pipeline.
func PipelinedPolicyRead(ctx context.Context, pool *pgxpool.Pool, tenant, id string) (found bool, err error) {
	bound, err := readmodel.Bind(pool, tenant)
	if err != nil {
		return false, err
	}
	var payload string
	err = bound.QueryRow(ctx, readSQL, id).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// AppFilterRead is the comparison: the grant store answers whether identity
// may read tenant, then Postgres reads with the tenant in the query, as a
// role that the row policies do not bind.
func AppFilterRead(ctx context.Context, pool *pgxpool.Pool, grants GrantStore, identity, tenant, id string) (found bool, err error) {
	ok, err := grants.Has(ctx, "grant:"+identity+":read", tenant)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	var payload string
	err = pool.QueryRow(ctx, `select payload::text from nodes where node_id = $1 and tenant_id = $2`, id, tenant).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// PlainRead is the floor: the same read with no tenant and no policy, as a
// bypassing role.
func PlainRead(ctx context.Context, pool *pgxpool.Pool, id string) (found bool, err error) {
	var payload string
	err = pool.QueryRow(ctx, readSQL, id).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
