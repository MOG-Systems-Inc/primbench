package readmodel

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BindSQL binds the implicit transaction a read runs in: the reader role,
// the tenant as a bound parameter and the transaction's read-only flag.
// Raising transaction_read_only is allowed at any point in a transaction;
// only lowering it is not.
const BindSQL = `select set_config('role', $1, true), set_config('mog.tenant', $2, true), ` +
	`set_config('transaction_read_only', 'on', true)`

// Pool is the read model bound to one tenant, as the services bind it: every
// read goes out as one pipeline, the binding then the read, which Postgres
// runs as one implicit transaction, so the transaction-local settings revert
// at its end and the connection returns to the pool unbound.
type Pool struct {
	raw    *pgxpool.Pool
	tenant string
}

// Bind returns the read model bound to tenant, a slug.
func Bind(raw *pgxpool.Pool, tenant string) (*Pool, error) {
	if raw == nil {
		return nil, errors.New("readmodel: no pool")
	}
	if !IsTenant(tenant) {
		return nil, fmt.Errorf("readmodel: refusing to bind tenant %q", tenant)
	}
	return &Pool{raw: raw, tenant: tenant}, nil
}

// QueryRow runs sql bound to the tenant in one round trip.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	b := &pgx.Batch{}
	b.Queue(BindSQL, ReaderRole, p.tenant)
	b.Queue(sql, args...)
	br := p.raw.SendBatch(ctx, b)
	if _, err := br.Exec(); err != nil {
		_ = br.Close()
		return errRow{err}
	}
	return batchRow{br}
}

type batchRow struct{ br pgx.BatchResults }

func (r batchRow) Scan(dest ...any) error {
	err := r.br.QueryRow().Scan(dest...)
	if cerr := r.br.Close(); err == nil {
		err = cerr
	}
	return err
}

type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }
