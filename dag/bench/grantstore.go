package bench

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GrantStore is the comparison path's grant service: one set per identity and
// verb, holding the families (or tenants) the identity may act on. A standard
// stack keeps it in Redis or Valkey (RESP); where none is reachable, the same
// sets live in a Postgres table (PGGrants), one more round trip to the
// database the hierarchy already lives in.
type GrantStore interface {
	// Has reports whether set holds member.
	Has(ctx context.Context, set, member string) (bool, error)
	// Replace makes set hold exactly members.
	Replace(ctx context.Context, set string, members []string) error
	// Describe names the store for the report's header.
	Describe() string
}

// Has is SISMEMBER.
func (c *RESP) Has(_ context.Context, set, member string) (bool, error) {
	n, _, err := c.Do("SISMEMBER", set, member)
	return n == 1, err
}

// Replace is DEL, then SADD of each member.
func (c *RESP) Replace(_ context.Context, set string, members []string) error {
	if _, _, err := c.Do("DEL", set); err != nil {
		return err
	}
	for _, m := range members {
		if _, _, err := c.Do("SADD", set, m); err != nil {
			return err
		}
	}
	return nil
}

// Describe is the server's name and version from INFO.
func (c *RESP) Describe() string {
	_, info, _ := c.Do("INFO", "server")
	server := "Redis protocol, version unknown"
	for _, l := range strings.Split(info, "\n") {
		l = strings.TrimSpace(l)
		if k, val, ok := strings.Cut(l, ":"); ok && (k == "valkey_version" || k == "redis_version") {
			server = k[:strings.Index(k, "_")] + " " + val
			if k == "valkey_version" {
				break
			}
		}
	}
	return server
}

// PGGrants keeps the grant sets in a Postgres table, grant_set.
type PGGrants struct {
	Pool *pgxpool.Pool
}

// NewPGGrants creates grant_set in pool's database when absent.
func NewPGGrants(ctx context.Context, pool *pgxpool.Pool) (*PGGrants, error) {
	_, err := pool.Exec(ctx, `create table if not exists grant_set (
  set_key text not null,
  member  text not null,
  primary key (set_key, member)
)`)
	return &PGGrants{Pool: pool}, err
}

// Has is one primary-key lookup.
func (g *PGGrants) Has(ctx context.Context, set, member string) (bool, error) {
	var ok bool
	err := g.Pool.QueryRow(ctx, `select exists (select 1 from grant_set where set_key = $1 and member = $2)`, set, member).Scan(&ok)
	return ok, err
}

// Replace deletes set's rows and inserts members.
func (g *PGGrants) Replace(ctx context.Context, set string, members []string) error {
	if _, err := g.Pool.Exec(ctx, `delete from grant_set where set_key = $1`, set); err != nil {
		return err
	}
	for _, m := range members {
		if _, err := g.Pool.Exec(ctx, `insert into grant_set (set_key, member) values ($1, $2)`, set, m); err != nil {
			return err
		}
	}
	return nil
}

// Describe names the table.
func (g *PGGrants) Describe() string {
	return "Postgres table grant_set, in the same database as the hierarchy"
}
