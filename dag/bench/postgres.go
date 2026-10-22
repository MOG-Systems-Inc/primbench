package bench

import (
	"fmt"

	"github.com/MOG-Systems-Inc/primbench/internal/embeddedpg"
)

// StartPostgres runs a native Postgres from the embedded-postgres library
// (the binaries the repo's tests already use), with its runtime, binaries and
// data under dir, on port (zero: one the kernel hands out, embeddedpg), as
// superuser bench. It returns the URL and the stop function.
func StartPostgres(dir string, port uint32) (url string, stop func() error, err error) {
	pg, err := embeddedpg.Start(dir, embeddedpg.Options{User: "bench", Password: "bench", Database: "bench", Port: port,
		Params: map[string]string{"max_connections": "300", "shared_buffers": "256MB"}})
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("postgres://bench:bench@127.0.0.1:%d/bench?sslmode=disable", pg.Port), pg.Stop, nil
}
