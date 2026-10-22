// Package bench measures the one evaluation of an incoming action against
// the same checks done by three services.
//
// The evaluation is the one the write path makes. A record's schema_id gives
// its family (ledger.Kind); the family and the id give its subject
// (ledger.SubjectFor); the identity's grant must match the subject; a stream
// must bind the subject (ledger.Streams); the record must pass its schema
// (internal/schema). The first three are lookups keyed by one name, done with
// the nats-server's own subject trie, so their cost depends on the number of
// tokens in a subject and the size of a grant list, never on how many records
// exist. TestEvaluationMatchesBroker holds the evaluation to what a broker
// enforcing the same grants and streams does.
//
// The comparison path (remote.go) asks Postgres for the hierarchy, a grant
// store for the grant (Redis or Valkey, or a Postgres table where none is
// reachable: grantstore.go) and an HTTP registry for the schema. The read
// comparison (read.go) runs the read model's forced row policies against a
// grant lookup plus a filtered query.
//
// To run the measurement on one machine (Valkey or Redis must be listening;
// Postgres is started from the embedded binaries the tests use):
//
//	valkey-server --port 16379 --bind 127.0.0.1 --save '' --appendonly no --daemonize yes
//	go run ./bench/cmd/primbench > results.md
//	go test ./bench -run '^$' -bench . -benchmem
//
// BENCH_REDIS_ADDR=127.0.0.1:16379 adds the Redis read path to go test.
//
// primbench -network runs the same sections against a NATS cluster and a
// Postgres server reached over the network. See cmd/primbench.
package bench
