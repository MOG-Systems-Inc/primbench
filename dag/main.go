// Command primbench measures the one evaluation of an incoming action
// against the same checks made by three services, for a write and a read,
// and judges the run against the published numbers.
//
//	go run .
//
// needs nothing but Go: Postgres, the JetStream broker and the schema
// registry stand-in are started by the run itself. See README.md.
package main

import "github.com/MOG-Systems-Inc/primbench/internal/driver"

func main() { driver.Main() }
