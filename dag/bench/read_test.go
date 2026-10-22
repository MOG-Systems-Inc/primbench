package bench

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MOG-Systems-Inc/primbench/internal/readmodel"
)

// The three bound reads return a tenant's own row and nothing of another
// tenant's, and a pooled connection carries neither the reader role nor the
// tenant into its next use.
func TestReads(t *testing.T) {
	url, stop, err := StartPostgres(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids, err := SeedReadModel(ctx, pool, 100)
	if err != nil {
		t.Fatal(err)
	}
	own, other := ids["acme"][0], ids["globex"][0]
	for name, read := range map[string]func(string, string) (bool, error){
		"policy":    func(tn, id string) (bool, error) { return PolicyRead(ctx, pool, tn, id) },
		"pipelined": func(tn, id string) (bool, error) { return PipelinedPolicyRead(ctx, pool, tn, id) },
	} {
		if ok, err := read("acme", own); err != nil || !ok {
			t.Errorf("%s: own row: %v %v", name, ok, err)
		}
		if ok, err := read("acme", other); err != nil || ok {
			t.Errorf("%s: another tenant's row came back: %v %v", name, ok, err)
		}
		// The connection is back in the pool unbound: an unbound read by the
		// bypassing role sees every row, and mog_tenant() refuses.
		if ok, err := PlainRead(ctx, pool, other); err != nil || !ok {
			t.Errorf("%s: after the bound read the pool is not the owner's: %v %v", name, ok, err)
		}
		var s string
		if err := pool.QueryRow(ctx, `select mog_tenant()`).Scan(&s); err == nil {
			t.Errorf("%s: mog_tenant() answered %q outside a bound read", name, s)
		}
	}
	// The pipelined binding is read-only, as the services' is.
	b := &pgx.Batch{}
	b.Queue(readmodel.BindSQL, readmodel.ReaderRole, "acme")
	b.Queue(`select current_setting('transaction_read_only')`)
	br := pool.SendBatch(ctx, b)
	if _, err := br.Exec(); err != nil {
		t.Fatal(err)
	}
	var ro string
	if err := br.QueryRow().Scan(&ro); err != nil || ro != "on" {
		t.Errorf("pipelined binding: transaction_read_only %q, %v; want on", ro, err)
	}
	br.Close()
	pg, err := NewPGGrants(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	stores := map[string]GrantStore{"postgres": pg}
	if addr := os.Getenv("BENCH_REDIS_ADDR"); addr != "" {
		r, err := DialRESP(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		stores["valkey"] = r
	}
	for name, store := range stores {
		if err := SeedReadGrants(ctx, store); err != nil {
			t.Fatal(err)
		}
		if ok, err := AppFilterRead(ctx, pool, store, "reader-acme", "acme", own); err != nil || !ok {
			t.Errorf("%s: app filter own: %v %v", name, ok, err)
		}
		if ok, err := AppFilterRead(ctx, pool, store, "reader-acme", "acme", other); err != nil || ok {
			t.Errorf("%s: app filter other: %v %v", name, ok, err)
		}
		if ok, _ := AppFilterRead(ctx, pool, store, "reader-acme", "globex", other); ok {
			t.Errorf("%s: app filter: a reader read a tenant it holds no grant on", name)
		}
	}

	// The seed drops nodes, so it refuses any database but a benchmark's.
	otherDB, err := pgxpool.New(ctx, strings.Replace(url, "/bench?", "/postgres?", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer otherDB.Close()
	if _, err := SeedReadModel(ctx, otherDB, 10); err == nil || !strings.Contains(err.Error(), "refusing to seed") {
		t.Errorf("the seed ran in database postgres: %v", err)
	}
}
