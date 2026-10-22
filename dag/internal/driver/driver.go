// Package driver is primbench: it measures the evaluation of an incoming
// action two ways and prints the latency distributions as Markdown tables,
// then one verdict line per section against the published numbers.
//
// The local path is the system's: Kind, the subject, the grant match, the
// stream lookup and schema validation, all in the calling process. The
// comparison path does the same checks as calls to three services: Postgres
// for the hierarchy, a grant store for the grants (Redis or Valkey, or a
// Postgres table where no Valkey is reachable) and an HTTP schema registry. It
// also measures a write end to end (the checks, then a publish to a JetStream
// broker, waiting for the acknowledgement) and a read under the read model's
// row policies against a grant lookup plus a filtered query.
//
// It runs two ways.
//
// On one machine (the default), everything is local: Postgres is started
// from the embedded binaries unless -pg is given, Valkey or Redis must be
// listening at -redis, and the broker is the nats-server library in this
// process, enforcing the benchmark's grants.
//
//	valkey-server --port 16379 --save '' --appendonly no &
//	go run ./bench/cmd/primbench -redis 127.0.0.1:16379 > results.md
//
// With -network, the broker and Postgres are servers reached over the
// network:
//
//   - The broker is the cluster at NATS_URL, as the identity whose nkey seed
//     is in NATS_NKEY_SEED (none: an unauthenticated connection). Every
//     publish goes to a scratch subject under the tenant's root,
//     <root>.bench.<family>.<id>, captured by the stream BENCH_SCRATCH, which
//     nothing reads. When the stream is absent the run creates it with
//     -scratch-replicas replicas, a 15 minute max age and a 256 MiB cap, or
//     stops before timing anything when the identity may not.
//   - Postgres is the server at -pg or DATABASE_URL (a superuser). The run
//     creates its own database there, bench_<unix>, holds the hierarchy, the
//     read model's rows and, with -grants postgres, the grant sets in it, and
//     drops it at the end.
//   - The registry stand-in is this binary re-run as its own process, as on
//     one machine.
package driver

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/MOG-Systems-Inc/primbench/bench"
	"github.com/MOG-Systems-Inc/primbench/internal/ledger"
	"github.com/MOG-Systems-Inc/primbench/internal/schema"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// Main runs primbench with the process's flags.
func Main() {
	var (
		serveRegistry = flag.String("serve-registry", "", "serve the HTTP schema registry stand-in on this address and do nothing else")
		network       = flag.Bool("network", false, "measure against the broker at NATS_URL and the Postgres at -pg or DATABASE_URL")
		full          = flag.Bool("full", false, "the worker counts of the paper's run: -conc 1,8,18,64 (a -conc given beside it still wins)")
		tolerance     = flag.Float64("tolerance", 2, "a section passes when every one-worker p50 is within this factor of the published one, either way")
		concList      = flag.String("conc", "1,8,64", "worker counts")
		localOps      = flag.Int("local-ops", 20000, "timed operations per worker, local evaluation")
		netOps        = flag.Int("net-ops", 2000, "timed operations per worker, every path that crosses a socket")
		warmup        = flag.Int("warmup", 200, "untimed operations per worker before timing")
		pgURL         = flag.String("pg", "", "Postgres URL (a superuser); empty starts the embedded one, or with -network reads DATABASE_URL")
		pgPort        = flag.Int("pg-port", 15499, "port for the embedded Postgres; 0 takes a free one")
		grantStore    = flag.String("grants", "auto", "where the comparison path keeps its grants: valkey (at -redis), postgres, or auto (valkey when one listens at -redis, else postgres)")
		redisAddr     = flag.String("redis", "127.0.0.1:16379", "Redis or Valkey address")
		registryAddr  = flag.String("registry-addr", "127.0.0.1:18089", "address for the registry stand-in; empty takes a free loopback port")
		rows          = flag.Int("rows", 100000, "read-model rows for the read benchmark")
		sectionList   = flag.String("sections", "eval,write,read", "which sections to run")
		scratchRepl   = flag.Int("scratch-replicas", 3, "with -network: replicas of BENCH_SCRATCH when the run creates it")
	)
	flag.Parse()
	if *full {
		set := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { set[f.Name] = true })
		for name, val := range map[string]string{"conc": "1,8,18,64"} {
			if !set[name] {
				ck(flag.Set(name, val))
			}
		}
	}
	v, err := schema.Load("")
	ck(err)
	if *serveRegistry != "" {
		log.Fatal(http.ListenAndServe(*serveRegistry, bench.RegistryHandler(v)))
	}
	var concs []int
	maxConc := 0
	for _, s := range strings.Split(*concList, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		ck(err)
		concs = append(concs, n)
		maxConc = max(maxConc, n)
	}
	run := map[string]bool{}
	for _, s := range strings.Split(*sectionList, ",") {
		run[strings.TrimSpace(s)] = true
	}
	if *grantStore != "valkey" && *grantStore != "postgres" && *grantStore != "auto" {
		log.Fatalf("-grants %q: want valkey, postgres or auto", *grantStore)
	}
	if *registryAddr == "" {
		*registryAddr = freeLoopback()
	}
	ctx := context.Background()
	started := time.Now()

	if *network {
		if *pgURL == "" {
			*pgURL = strings.TrimSpace(os.Getenv("DATABASE_URL"))
		}
		if *pgURL == "" {
			log.Fatal("-network needs -pg or DATABASE_URL: the comparison path's database")
		}
		if strings.TrimSpace(os.Getenv("NATS_URL")) == "" && run["write"] {
			log.Fatal("-network needs NATS_URL for the write section")
		}
	}

	m := &measurement{run: run, results: map[string][]bench.Result{}}
	runErr := m.measure(ctx, v, options{
		network: *network, concs: concs, maxConc: maxConc, localOps: *localOps, netOps: *netOps, warmup: *warmup,
		pgURL: *pgURL, pgPort: *pgPort, grants: *grantStore, redisAddr: *redisAddr, registryAddr: *registryAddr,
		rows: *rows, started: started, scratchReplicas: *scratchRepl,
	})
	if runErr != nil {
		log.Printf("primbench: %v", runErr)
	}
	pass := true
	if runErr == nil {
		fmt.Fprint(os.Stdout, "## Verdict\n\n")
		if *network {
			fmt.Fprintln(os.Stdout, "Not judged: the published numbers are loopback numbers, and this run crossed a network.")
		} else {
			fmt.Fprintf(os.Stdout, "Each section's one-worker p50s against the published ones (Apple M5 Max, loopback, grants in Valkey), within %gx either way, with no wrong answer.", *tolerance)
			if m.grants == "postgres" {
				fmt.Fprint(os.Stdout, " No Valkey was listening, so this run's comparison path kept its grants in a Postgres table.")
			}
			fmt.Fprint(os.Stdout, "\n\n```\n")
			for _, line := range m.verdicts(*tolerance) {
				fmt.Fprintln(os.Stdout, line.text)
				pass = pass && line.pass
			}
			fmt.Fprintln(os.Stdout, "```")
		}
		fmt.Fprintf(os.Stdout, "\nWall time: %.1f s\n", time.Since(started).Seconds())
	}
	if runErr != nil || m.wrong() > 0 || !pass {
		os.Exit(1)
	}
}

type options struct {
	network                  bool
	concs                    []int
	maxConc                  int
	localOps, netOps, warmup int
	pgURL                    string
	pgPort                   int
	grants, redisAddr        string
	registryAddr             string
	rows                     int
	started                  time.Time
	scratchReplicas          int
}

// measurement is what one run measured, by section.
type measurement struct {
	run     map[string]bool
	results map[string][]bench.Result
	header  map[string]string
	grants  string // the grant store the run used: valkey or postgres
}

func (m *measurement) row(out io.Writer, section string, r bench.Result) {
	m.results[section] = append(m.results[section], r)
	fmt.Fprintln(out, r.Row())
}

func (m *measurement) measure(ctx context.Context, v *schema.Validator, o options) error {
	out := os.Stdout
	p, err := bench.New(v, bench.DevGrants())
	if err != nil {
		return err
	}
	mix := bench.Mix()

	// Postgres: the embedded one, the one given, or with -network a scratch
	// database of its own on the one given.
	tmp, err := os.MkdirTemp("", "primbench-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	pgURL := o.pgURL
	pgWhere := "given by -pg"
	switch {
	case o.network:
		scratchURL, drop, err := scratchDatabase(ctx, o.pgURL, fmt.Sprintf("bench_%d", o.started.Unix()))
		if err != nil {
			return err
		}
		defer drop()
		pgURL, pgWhere = scratchURL, "the server at -pg, reached over the network; a scratch database of its own, dropped at the end"
	case pgURL == "":
		var stop func() error
		pgURL, stop, err = bench.StartPostgres(tmp, uint32(o.pgPort))
		if err != nil {
			return err
		}
		defer stop()
		pgWhere = "embedded binaries, native process, loopback TCP"
	}
	cfg, err := pgxpool.ParseConfig(pgURL)
	if err != nil {
		return err
	}
	cfg.MaxConns = int32(o.maxConc + 8)
	cfg.MinConns = int32(o.maxConc)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	// The grant store: one Valkey connection per worker, or the Postgres
	// table, shared. auto takes Valkey when one answers at -redis.
	if o.grants == "auto" {
		o.grants = "postgres"
		if r, err := bench.DialRESP(o.redisAddr); err == nil {
			if _, _, err := r.Do("PING"); err == nil {
				o.grants = "valkey"
			}
			r.Close()
		}
	}
	m.grants = o.grants
	grants := make([]bench.GrantStore, o.maxConc)
	if o.grants == "postgres" {
		pg, err := bench.NewPGGrants(ctx, pool)
		if err != nil {
			return err
		}
		for i := range grants {
			grants[i] = pg
		}
	} else {
		for i := range grants {
			r, err := bench.DialRESP(o.redisAddr)
			if err != nil {
				return fmt.Errorf("valkey at %s: %w", o.redisAddr, err)
			}
			defer r.Close()
			grants[i] = r
		}
	}

	// The registry stand-in, as its own process.
	self, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(self, "-serve-registry", o.registryAddr)
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		return err
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	if err := waitListening(o.registryAddr); err != nil {
		return err
	}
	remote := &bench.Remote{PG: pool, HTTP: bench.NewHTTPClient(o.maxConc), RegistryURL: "http://" + o.registryAddr, Tenant: ledger.FirstTenant}
	if err := bench.Seed(ctx, pool, grants[0], v, p, bench.DevGrants(), ledger.FirstTenant); err != nil {
		return err
	}

	// The broker. On one machine it is started in this process when the
	// write section runs; with -network it is the cluster, one connection per
	// worker as the task's identity.
	var brokerLine string
	var publisher func(w int, identity string) jetstream.JetStream
	subject := func(s string) string { return s }
	if m.run["write"] {
		if o.network {
			conns := make([]jetstream.JetStream, o.maxConc)
			var first *nats.Conn
			for w := range conns {
				nc, err := natsConnect(fmt.Sprintf("primbench-%d", w))
				if err != nil {
					return fmt.Errorf("nats: %w", err)
				}
				defer nc.Close()
				if first == nil {
					first = nc
				}
				if conns[w], err = jetstream.New(nc); err != nil {
					return err
				}
			}
			// One publish before timing anything: the stream must be there.
			if err := ensureScratch(ctx, conns[0], o.scratchReplicas); err != nil {
				return err
			}
			publisher = func(w int, _ string) jetstream.JetStream { return conns[w] }
			subject = ledger.BenchScratchSubject
			brokerLine = fmt.Sprintf("the cluster at NATS_URL, nats-server %s, %d servers known to the client; stream %s, %s, file storage",
				first.ConnectedServerVersion(), len(first.Servers()), ledger.BenchScratchStream, scratchReplicas(ctx, conns[0]))
		} else {
			brokerDir, err := os.MkdirTemp(tmp, "nats-")
			if err != nil {
				return err
			}
			br, err := bench.StartBroker(bench.DevGrants(), brokerDir)
			if err != nil {
				return err
			}
			defer br.Close()
			js := make([]map[string]jetstream.JetStream, o.maxConc)
			for w := range js {
				js[w] = map[string]jetstream.JetStream{}
				for _, a := range mix {
					if a.Want != bench.Allowed || js[w][a.Identity] != nil {
						continue
					}
					nc, err := br.Connect(a.Identity)
					if err != nil {
						return err
					}
					defer nc.Close()
					if js[w][a.Identity], err = jetstream.New(nc); err != nil {
						return err
					}
				}
			}
			publisher = func(w int, identity string) jetstream.JetStream { return js[w][identity] }
			brokerLine = "nats-server library in this process, JetStream, one node, file store"
		}
	}

	m.header = header(ctx, pool, grants[0], pgWhere, brokerLine, o)
	printHeader(out, m.header, o.network)

	// 1. The evaluation alone.
	if m.run["eval"] {
		fmt.Fprint(out, "## Evaluation of one incoming write\n\n")
		fmt.Fprintf(out, "The workload cycles through %d actions: %d allowed, and one each refused as not granted (twice), no stream and schema. A wrong answer is an action whose refusal differs from the one it was written for.\n\n", len(mix), countAllowed(mix))
		fmt.Fprintln(out, bench.Header)
		for _, c := range o.concs {
			m.row(out, "eval", bench.Run("local: one process", c, o.localOps, o.warmup, func(w, i int) error {
				a := mix[(w+i)%len(mix)]
				return expect(a, p.CheckPublish(a.Identity, a.Raw))
			}))
		}
		for _, parallel := range []bool{false, true} {
			name := "three services, in sequence"
			if parallel {
				name = "three services, registry in parallel"
			}
			for _, c := range o.concs {
				m.row(out, "eval", bench.Run(name, c, o.netOps, o.warmup, func(w, i int) error {
					a := mix[(w+i)%len(mix)]
					return expect(a, remote.CheckPublish(ctx, grants[w], a.Identity, a.Raw, parallel))
				}))
			}
		}
		fmt.Fprintln(out)
	}

	// 2. A write end to end: the checks, then the publish and its ack.
	if m.run["write"] {
		var allowed []bench.Action
		for _, a := range mix {
			if a.Want == bench.Allowed {
				allowed = append(allowed, a)
			}
		}
		publish := func(w int, a bench.Action, d bench.Decision) error {
			if err := expect(a, d); err != nil {
				return err
			}
			_, err := publisher(w, a.Identity).Publish(ctx, subject(d.Subject), a.Raw)
			return err
		}
		fmt.Fprint(out, "## One write end to end\n\n")
		if o.network {
			fmt.Fprintf(out, "The %d allowed actions of the workload: the checks, then a publish to the cluster at NATS_URL, to the action's subject moved under %s, waiting for the stream's acknowledgement. With more than one replica, an acknowledgement waits for a majority of them.\n\n", len(allowed), ledger.BenchScratchSubjects())
		} else {
			fmt.Fprintf(out, "The %d allowed actions of the workload: the checks, then a publish to a one-node JetStream broker in this process that enforces the same grants and holds every stream, waiting for the stream's acknowledgement. The broker writes to a file store on this machine's disk and does not wait for an fsync.\n\n", len(allowed))
		}
		fmt.Fprintln(out, bench.Header)
		for _, c := range o.concs {
			m.row(out, "write", bench.Run("publish only, no checks", c, o.netOps, o.warmup, func(w, i int) error {
				a := allowed[(w+i)%len(allowed)]
				d := p.CheckPublish(a.Identity, a.Raw) // the subject; not timed apart
				_, err := publisher(w, a.Identity).Publish(ctx, subject(d.Subject), a.Raw)
				return err
			}))
		}
		for _, c := range o.concs {
			m.row(out, "write", bench.Run("local checks, then publish", c, o.netOps, o.warmup, func(w, i int) error {
				a := allowed[(w+i)%len(allowed)]
				return publish(w, a, p.CheckPublish(a.Identity, a.Raw))
			}))
		}
		for _, c := range o.concs {
			m.row(out, "write", bench.Run("three services, then publish", c, o.netOps, o.warmup, func(w, i int) error {
				a := allowed[(w+i)%len(allowed)]
				return publish(w, a, remote.CheckPublish(ctx, grants[w], a.Identity, a.Raw, false))
			}))
		}
		fmt.Fprintln(out)
	}

	// 3. A read under the tenant policy.
	if m.run["read"] {
		ids, err := bench.SeedReadModel(ctx, pool, o.rows)
		if err != nil {
			return err
		}
		if err := bench.SeedReadGrants(ctx, grants[0]); err != nil {
			return err
		}
		own, other := ids[bench.ReadTenants[0]], ids[bench.ReadTenants[1]]
		pick := func(w, i int) (id string, want bool) {
			if (w+i)%10 == 9 {
				return other[(w*7919+i)%len(other)], false
			}
			return own[(w*7919+i)%len(own)], true
		}
		check := func(found, want bool, err error) error {
			if err != nil {
				return err
			}
			if found != want {
				return fmt.Errorf("found %v, want %v", found, want)
			}
			return nil
		}
		grantName := "Valkey grant"
		if o.grants == "postgres" {
			grantName = "Postgres grant"
		}
		fmt.Fprint(out, "## One read by id under the tenant policy\n\n")
		fmt.Fprintf(out, "%d tag rows across three tenants, eight to one to one. The reader is the first tenant's. One read in ten asks for another tenant's row and must come back empty; a wrong answer is a read that did not.\n\n", o.rows)
		fmt.Fprintln(out, bench.Header)
		for _, c := range o.concs {
			m.row(out, "read", bench.Run("plain read, no policy (the floor)", c, o.netOps, o.warmup, func(w, i int) error {
				id, want := pick(w, i)
				found, err := bench.PlainRead(ctx, pool, id)
				if !want {
					want = true // no policy: the other tenant's row comes back
				}
				return check(found, want, err)
			}))
		}
		for _, c := range o.concs {
			m.row(out, "read", bench.Run("row policy, three round trips", c, o.netOps, o.warmup, func(w, i int) error {
				id, want := pick(w, i)
				found, err := bench.PolicyRead(ctx, pool, ledger.FirstTenant, id)
				return check(found, want, err)
			}))
		}
		for _, c := range o.concs {
			m.row(out, "read", bench.Run("row policy, pipelined (1 round trip)", c, o.netOps, o.warmup, func(w, i int) error {
				id, want := pick(w, i)
				found, err := bench.PipelinedPolicyRead(ctx, pool, ledger.FirstTenant, id)
				return check(found, want, err)
			}))
		}
		for _, c := range o.concs {
			m.row(out, "read", bench.Run(grantName+", then filtered query", c, o.netOps, o.warmup, func(w, i int) error {
				id, want := pick(w, i)
				found, err := bench.AppFilterRead(ctx, pool, grants[w], "reader-"+ledger.FirstTenant, ledger.FirstTenant, id)
				return check(found, want, err)
			}))
		}
		fmt.Fprintln(out)
	}
	return nil
}

// scratchDatabase creates db on the server at admin and answers its URL and
// the function that drops it.
func scratchDatabase(ctx context.Context, admin, db string) (string, func(), error) {
	u, err := url.Parse(admin)
	if err != nil {
		return "", nil, fmt.Errorf("database URL: %w", err)
	}
	pool, err := pgxpool.New(ctx, admin)
	if err != nil {
		return "", nil, err
	}
	if _, err := pool.Exec(ctx, "create database "+db); err != nil {
		pool.Close()
		return "", nil, fmt.Errorf("create database %s: %w", db, err)
	}
	drop := func() {
		if _, err := pool.Exec(context.Background(), "drop database if exists "+db+" with (force)"); err != nil {
			log.Printf("WARN: drop database %s: %v", db, err)
		}
		pool.Close()
	}
	u.Path = "/" + db
	return u.String(), drop, nil
}

func (m *measurement) wrong() int {
	n := 0
	for _, rs := range m.results {
		for _, r := range rs {
			n += r.Errors
		}
	}
	return n
}

func countAllowed(mix []bench.Action) int {
	n := 0
	for _, a := range mix {
		if a.Want == bench.Allowed {
			n++
		}
	}
	return n
}

func expect(a bench.Action, d bench.Decision) error {
	if d.Refusal != a.Want {
		return fmt.Errorf("%s: %q (%v), want %q", a.Name, d.Refusal, d.Err, a.Want)
	}
	return nil
}

// freeLoopback is a loopback address the kernel just handed out and released.
func freeLoopback() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	ck(err)
	defer l.Close()
	return l.Addr().String()
}

func waitListening(addr string) error {
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("registry stand-in did not listen on %s", addr)
}

// natsConnect opens a connection to NATS_URL, as the identity whose nkey
// seed is in NATS_NKEY_SEED when it is set.
func natsConnect(name string) (*nats.Conn, error) {
	opts := []nats.Option{nats.Name(name)}
	if seed := strings.TrimSpace(os.Getenv("NATS_NKEY_SEED")); seed != "" {
		kp, err := nkeys.FromSeed([]byte(seed))
		if err != nil {
			return nil, fmt.Errorf("NATS_NKEY_SEED: %w", err)
		}
		pub, err := kp.PublicKey()
		if err != nil {
			return nil, err
		}
		opts = append(opts, nats.Nkey(pub, kp.Sign))
	}
	return nats.Connect(os.Getenv("NATS_URL"), opts...)
}

// scratchConfig is BENCH_SCRATCH as the run creates it.
func scratchConfig(replicas int) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        ledger.BenchScratchStream,
		Description: "primbench -network: timed publishes only; nothing reads them",
		Subjects:    []string{ledger.BenchScratchSubjects()},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		Replicas:    replicas,
		MaxAge:      15 * time.Minute,
		MaxBytes:    256 << 20,
		Discard:     jetstream.DiscardOld,
	}
}

// ensureScratch publishes one probe to the scratch tree. When no stream
// answers, it creates BENCH_SCRATCH and probes again; when the identity may
// not create it, the run stops here.
func ensureScratch(ctx context.Context, js jetstream.JetStream, replicas int) error {
	probe := ledger.BenchScratchSubject(ledger.SubjectRoot + ".probe.primbench")
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := js.Publish(c, probe, []byte(`{}`), jetstream.WithRetryAttempts(0))
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrNoStreamResponse) {
		return fmt.Errorf("publish to %s: %w", probe, err)
	}
	if _, err := js.CreateStream(c, scratchConfig(replicas)); err != nil {
		return fmt.Errorf("no stream binds %s, and creating %s failed: %w; create it, or run as an identity that may",
			ledger.BenchScratchSubjects(), ledger.BenchScratchStream, err)
	}
	if _, err := js.Publish(c, probe, []byte(`{}`)); err != nil {
		return fmt.Errorf("publish to %s after creating %s: %w", probe, ledger.BenchScratchStream, err)
	}
	return nil
}

// scratchReplicas is the stream's replica count, read from the stream when
// the identity may read its info.
func scratchReplicas(ctx context.Context, js jetstream.JetStream) string {
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if st, err := js.Stream(c, ledger.BenchScratchStream); err == nil {
		if info, err := st.Info(c); err == nil {
			return fmt.Sprintf("%d replicas", info.Config.Replicas)
		}
	}
	return "replicas unknown (its info is not granted to this identity)"
}

// header is the run's setting, without any address.
func header(ctx context.Context, pool *pgxpool.Pool, grants bench.GrantStore, pgWhere, broker string, o options) map[string]string {
	var pgv string
	_ = pool.QueryRow(ctx, `show server_version`).Scan(&pgv)
	grantWhere := "native process, loopback TCP"
	if _, ok := grants.(*bench.PGGrants); ok {
		grantWhere = "no Valkey is reachable here, so the grant sets are a Postgres table"
	} else if o.network {
		grantWhere = "reached over the network"
	}
	registry := "this binary, own process, HTTP/1.1 keep-alive over loopback"
	h := map[string]string{
		"cpu":         cpuName(),
		"os":          runtime.GOOS + "/" + runtime.GOARCH,
		"go":          runtime.Version(),
		"postgres":    pgv + " (" + pgWhere + ")",
		"grant_store": grants.Describe() + " (" + grantWhere + ")",
		"registry":    registry,
		"gogc":        envOr("GOGC", "100 (default)"),
		"run":         o.started.UTC().Format(time.RFC3339),
	}
	if broker != "" {
		h["broker"] = broker
	}
	return h
}

func printHeader(out io.Writer, h map[string]string, network bool) {
	fmt.Fprintf(out, "# Primitive benchmark\n\n")
	fmt.Fprintf(out, "| Machine | Value |\n| :-- | :-- |\n")
	for _, k := range []string{"cpu", "os", "go", "postgres", "grant_store", "registry", "broker", "gogc", "run"} {
		if v, ok := h[k]; ok {
			fmt.Fprintf(out, "| %s | %s |\n", strings.ReplaceAll(k, "_", " "), v)
		}
	}
	fmt.Fprintln(out)
	if network {
		fmt.Fprint(out, "Measured over the network: the broker and Postgres are servers reached over it. ")
	}
	fmt.Fprint(out, "Latencies are wall time per operation, one clock pair per call, warm-up excluded. Workers run concurrently; ops/s is all workers together.\n\n")
}

// cpuName is the processor's name: sysctl on macOS, /proc/cpuinfo on Linux.
func cpuName() string {
	cores := fmt.Sprintf(", %d logical cores", runtime.NumCPU())
	if b, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
		return strings.TrimSpace(string(b)) + cores
	}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if k, v, ok := strings.Cut(l, ":"); ok && strings.TrimSpace(k) == "model name" {
				return strings.TrimSpace(v) + cores
			}
		}
	}
	return runtime.GOARCH + cores
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func ck(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
