package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MOG-Systems-Inc/primbench/bench"
	"github.com/MOG-Systems-Inc/primbench/internal/ledger"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// build compiles primbench into the test's directory.
func build(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "primbench")
	out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// natsServer is a one-node JetStream server with no authentication.
func natsServer(t *testing.T) *server.Server {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server did not start")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return fmt.Sprint(l.Addr())
}

// The network mode against a broker and a Postgres it reaches by URL: it
// creates the scratch stream, publishes only under the scratch tree, creates
// no other stream, prints every section and leaves no database behind.
func TestNetworkMode(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs primbench against a broker and a Postgres")
	}
	bin := build(t)
	srv := natsServer(t)
	pgURL, stop, err := bench.StartPostgres(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin, "-network", "-grants", "postgres", "-scratch-replicas", "1", "-conc", "1,2",
		"-local-ops", "50", "-net-ops", "20", "-warmup", "5", "-rows", "2000", "-registry-addr", freeAddr(t), "-pg", pgURL)
	cmd.Env = append(os.Environ(), "NATS_URL="+srv.ClientURL(), "NATS_NKEY_SEED=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("primbench -network: %v\n%s", err, out)
	}
	for _, want := range []string{"## Evaluation of one incoming write", "## One write end to end", "## One read by id under the tenant policy",
		"Postgres grant, then filtered query", "a scratch database of its own", "stream BENCH_SCRATCH, 1 replicas"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	// Two worker counts: three eval paths, three write paths, four reads,
	// each a table row with zero wrong.
	if n := strings.Count(string(out), " | 0 |\n"); n != 6+6+8 {
		t.Errorf("%d rows with zero wrong, want 20:\n%s", n, out)
	}

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	var streams []string
	names := js.StreamNames(ctx)
	for n := range names.Name() {
		streams = append(streams, n)
	}
	if len(streams) != 1 || streams[0] != ledger.BenchScratchStream {
		t.Errorf("streams after the run: %v", streams)
	}
	scratch, err := js.Stream(ctx, ledger.BenchScratchStream)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := scratch.Info(ctx, jetstream.WithSubjectFilter(">"))
	// Three write paths, two worker counts (1 and 2 workers), 25 operations
	// each with the warm-up, and the probe.
	if want := uint64(3*(1+2)*(20+5) + 1); info.State.Msgs != want {
		t.Errorf("the stream holds %d messages, want %d", info.State.Msgs, want)
	}
	for subj := range info.State.Subjects {
		if !strings.HasPrefix(subj, ledger.SubjectRoot+".bench.") {
			t.Errorf("a publish outside the scratch tree: %s", subj)
		}
	}
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var dbs int
	_ = pool.QueryRow(ctx, `select count(*) from pg_database where datname like 'bench\_%'`).Scan(&dbs)
	if dbs != 0 {
		t.Error("the scratch database was left behind")
	}
}

// Without a database the network mode does not start.
func TestNetworkModeRefusesWithoutADatabase(t *testing.T) {
	if testing.Short() {
		t.Skip("builds primbench")
	}
	bin := build(t)
	cmd := exec.Command(bin, "-network")
	cmd.Env = append(os.Environ(), "DATABASE_URL=", "NATS_URL=nats://127.0.0.1:1")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "-network needs -pg or DATABASE_URL") {
		t.Fatalf("started without a database: %v\n%s", err, out)
	}
}
