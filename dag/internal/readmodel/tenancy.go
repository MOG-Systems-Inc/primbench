// Package readmodel is the read model's tenancy as the projector installs
// it: the tenant column, the forced row policy, mog_tenant() and the reader
// role. A tenant-bound read sets the role and the tenant in its transaction;
// the policy then returns only that tenant's rows.
package readmodel

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ReaderRole is the role every tenant-bound read runs as: NOBYPASSRLS and
// SELECT on the read-model tables, nothing else.
const ReaderRole = "readmodel_reader"

// Tables is every read-model table the benchmark creates. Each carries
// tenant_id and the tenant policy.
var Tables = []string{"nodes"}

var slugRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// IsTenant reports whether s is a tenant slug as the subject root carries it
// (tenant.<slug>.dag).
func IsTenant(s string) bool { return slugRE.MatchString(s) }

// backfill sets the tenant on rows written before the table carried one.
var backfill = map[string]string{
	"nodes": `update nodes set tenant_id = $1 where tenant_id is null or tenant_id = ''`,
}

// mog_tenant() refuses an unbound read. A plain current_setting() would not
// do: once a session has set a custom setting it reads back as ” after the
// transaction ends, not as an error.
const mogTenantFn = `
create or replace function mog_tenant() returns text
language plpgsql
stable
as $$
declare
  t text := current_setting('mog.tenant', true);
begin
  if t is null or t = '' then
    raise exception 'tenant_unbound'
      using errcode = '42501',
            detail = 'mog.tenant is not set in this transaction; a read-model read must set it from the credential';
  end if;
  return t;
end
$$;
grant execute on function mog_tenant() to public;
`

const readerRole = `
do $$
begin
  create role ` + ReaderRole + ` nologin nosuperuser nobypassrls noinherit;
exception when duplicate_object then
  null;
end
$$;
`

// EnsureTenancy installs the tenancy on every table of Tables that exists, in
// one transaction: the column, the backfill, NOT NULL, mog_tenant(), the
// reader role and its grants, and the forced policy. Idempotent.
//
// The connecting role writes every tenant's rows, so it must bypass the
// policies: a superuser, or a role with BYPASSRLS. FORCE binds a plain owner
// too, so EnsureTenancy refuses any other role rather than install policies
// that would refuse its writes.
func EnsureTenancy(ctx context.Context, pool *pgxpool.Pool, rootTenant string) error {
	if !IsTenant(rootTenant) {
		return fmt.Errorf("tenancy: root tenant %q is not a tenant slug", rootTenant)
	}
	var bypass bool
	if err := pool.QueryRow(ctx,
		`select rolsuper or rolbypassrls from pg_roles where rolname = current_user`).Scan(&bypass); err != nil {
		return fmt.Errorf("tenancy: read the connecting role: %w", err)
	}
	if !bypass {
		return fmt.Errorf("tenancy: the connecting Postgres role neither is a superuser nor holds BYPASSRLS; " +
			"the read-model tables force row-level security, so its writes would be refused")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var present []string
	for _, t := range Tables {
		var ok bool
		if err := tx.QueryRow(ctx, `select to_regclass($1) is not null`, "public."+t).Scan(&ok); err != nil {
			return err
		}
		if ok {
			present = append(present, t)
		}
	}
	for _, t := range present {
		for _, q := range []struct {
			sql  string
			args []any
		}{
			{`alter table ` + t + ` add column if not exists tenant_id text`, nil},
			{backfill[t], []any{rootTenant}},
			{`alter table ` + t + ` alter column tenant_id set not null`, nil},
		} {
			if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
				return fmt.Errorf("tenancy: %s: %w", t, err)
			}
		}
	}
	if _, err := tx.Exec(ctx, mogTenantFn); err != nil {
		return fmt.Errorf("tenancy: mog_tenant(): %w", err)
	}
	if _, err := tx.Exec(ctx, readerRole); err != nil {
		return fmt.Errorf("tenancy: role %s: %w", ReaderRole, err)
	}
	for _, t := range present {
		for _, q := range []string{
			`grant select on ` + t + ` to ` + ReaderRole,
			`alter table ` + t + ` enable row level security`,
			`alter table ` + t + ` force row level security`,
			`drop policy if exists tenant_isolation on ` + t,
			// (select mog_tenant()) is an init plan: evaluated once per
			// query, not once per row.
			`create policy tenant_isolation on ` + t + `
			   using (tenant_id = (select mog_tenant()))
			   with check (tenant_id = (select mog_tenant()))`,
		} {
			if _, err := tx.Exec(ctx, q); err != nil {
				return fmt.Errorf("tenancy: %s: %w", t, err)
			}
		}
	}
	return tx.Commit(ctx)
}
