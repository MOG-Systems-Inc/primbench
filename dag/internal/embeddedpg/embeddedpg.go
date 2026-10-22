// Package embeddedpg starts the native Postgres the tests and the benchmark
// run, from the embedded-postgres library, so that any number of them can
// start at once on one machine.
//
// The library shares two things between processes unless told otherwise,
// and both broke parallel runs:
//
//   - Its runtime directory, where it unpacks the binaries. Unset, it is
//     ~/.embedded-postgres-go/extracted, and every Start deletes it and
//     unpacks again: a second process starting while the first runs initdb
//     removes the first one's libraries (dyld: Library not loaded:
//     libicudata). Here every instance's runtime, binaries and data live
//     under its own directory. Only the download cache stays shared, and the
//     library writes it with a temporary file and a rename.
//   - The port. A port picked before the start can be taken by another
//     process before Postgres binds it ("process already listening on
//     port"). Here the port comes from the kernel, and a start that finds it
//     taken is tried again on a new one.
package embeddedpg

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

// Options names the database, its owner and any server parameters. Port,
// when set, is the one port to use, with no retry; zero takes one from the
// kernel and tries again on a taken one.
type Options struct {
	User, Password, Database string
	Params                   map[string]string
	Port                     uint32
}

// Instance is one running Postgres.
type Instance struct {
	Port uint32
	URL  string
	pg   *embeddedpostgres.EmbeddedPostgres
}

// Stop stops it.
func (i *Instance) Stop() error { return i.pg.Stop() }

// attempts is how many ports a start tries before it gives up.
const attempts = 8

// pickPort is a port the kernel just handed out and released.
var pickPort = func() (uint32, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return uint32(l.Addr().(*net.TCPAddr).Port), nil
}

// Start runs one Postgres with its runtime, binaries and data under dir,
// which must be the caller's own (a test's temporary directory). The first
// start in a directory unpacks the binaries there.
func Start(dir string, o Options) (*Instance, error) {
	if o.User == "" {
		o.User, o.Password, o.Database = "bench", "bench", "bench"
	}
	var last error
	tries := attempts
	if o.Port != 0 {
		tries = 1
	}
	for i := 0; i < tries; i++ {
		port, err := pickPort()
		if o.Port != 0 {
			port, err = o.Port, nil
		}
		if err != nil {
			return nil, err
		}
		cfg := embeddedpostgres.DefaultConfig().
			Port(port).
			RuntimePath(filepath.Join(dir, "runtime")).
			BinariesPath(filepath.Join(dir, "bin")).
			DataPath(filepath.Join(dir, "data")).
			Logger(io.Discard).
			Username(o.User).Password(o.Password).Database(o.Database)
		if len(o.Params) > 0 {
			cfg = cfg.StartParameters(o.Params)
		}
		pg := embeddedpostgres.NewDatabase(cfg)
		err = pg.Start()
		if err == nil {
			return &Instance{Port: port, pg: pg,
				URL: fmt.Sprintf("postgres://%s:%s@localhost:%d/%s?sslmode=disable", o.User, o.Password, port, o.Database)}, nil
		}
		last = err
		if o.Port != 0 || (!portTaken(err) && portFree(port)) {
			return nil, fmt.Errorf("embedded postgres: %w", err)
		}
	}
	return nil, fmt.Errorf("embedded postgres: no free port in %d tries: %w", attempts, last)
}

// portFree reports whether nothing listens on port now: a start that failed
// on a port another process holds is tried again even when the library's
// error does not say why.
func portFree(port uint32) bool {
	l, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return false
	}
	_ = l.Close()
	return true
}

// portTaken reports whether a start failed because another process holds
// its port.
func portTaken(err error) bool {
	s := err.Error()
	return strings.Contains(s, "already listening on port") ||
		strings.Contains(s, "Address already in use") ||
		strings.Contains(s, "could not bind")
}
