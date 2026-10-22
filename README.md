# primbench

From the `dag/` directory:

```
cd dag
go run .
```

That is the whole validation: about 33 seconds on my laptop, after the first run's download of the modules and the Postgres binaries. It needs nothing installed but Go 1.25 or later. The run starts its own Postgres from the embedded-postgres binaries, its own JetStream broker from the nats-server library, and its own schema registry stand-in, all on loopback: Postgres on port 15499 and the registry stand-in on 127.0.0.1:18089. `make validate`, from the repository's root, runs the same command.

**What it prints.** The machine it ran on, then three Markdown tables: the evaluation of one incoming write, one write end to end, and one read by id under the tenant policy. It ends with one verdict line per section, comparing this run's one-worker p50s with the ones I published, and the wall time. On my Mac:

```
evaluation: local 6.0 µs (published 6.0), three services 102.1 µs (published 144.9), registry in parallel 63.8 µs (published 101.2); 0 wrong; within 2x: PASS
write: publish only 47.0 µs (published 47.0), local checks then publish 49.3 µs (published 45.7), three services then publish 147.5 µs (published 188.5); 0 wrong; within 2x: PASS
read: plain 26.5 µs (published 26.0), policy pipelined 31.6 µs (published 30.6), policy bound 72.9 µs (published 71.6), Postgres grant then query 50.9 µs (no published number: no Valkey was listening); 0 wrong; within 2x: PASS
```

A section passes when every one-worker p50 is within 2x of the published one, either way, and no operation came out wrong. The command exits 0 when every section passes.

**How long it takes.** Measured on an Apple M5 Max:

| Run | Wall time |
| :-- | --: |
| `go run .`, after the first | 32.4 s |

The default run is the published one at 1, 8 and 64 workers: 20,000 timed operations per worker for the local evaluation and 2,000 for every path that crosses a socket, and all 100,000 rows in the read section. `go run . -full` adds the published run's 18 workers, `make full` the same.

**One difference from the published run.** I published with Valkey holding the comparison path's grants. With nothing installed, no Valkey is listening, so the grants go in a Postgres table instead. That makes the three-service rows faster than published, and the read row "Postgres grant, then filtered query" has no published number to judge. To run the published setting, start Valkey first and run the same command:

```
valkey-server --port 16379 --bind 127.0.0.1 --save '' --appendonly no --daemonize yes
cd dag
go run .
```

---

## The paper and its numbers

This repository also holds the paper the bench was written for, and what stands behind it.

- [`SPEC.md`](./SPEC.md) is the paper, *One Name for Schema, Hierarchy and Access Control on a Replicated Log*.
- [`BENCH.md`](./BENCH.md) is the benchmark report the paper cites as reference [5]: the method, the workload and the published tables.
- [`SPEC.pdf`](./SPEC.pdf) is the typeset paper.

The verdict lines of `go run .` compare this run's one-worker p50s with the medians published in `BENCH.md`.

## What it measures, in one paragraph

I wrote this benchmark to measure one claim. In the system I build, the schema registry, the subject tree and the identity grants are keyed by one name, so checking an incoming action is a few lookups and a schema validation in the writer's own process. A standard stack makes the same three checks as calls to three services. primbench measures both, for a write and for a read, and checks every answer. It is the harness behind the benchmark section of my paper on the primitive, published by MOG Systems, Inc.

## Run it every other way

From the `dag/` directory, as the write-up runs it:

```
go run ./bench/cmd/primbench > results.md
GOGC=off go run ./bench/cmd/primbench -sections eval >> results.md
go test ./bench -run '^$' -bench BenchmarkEvaluation -benchmem -count 5
```

and the rest:

```
go run . -full > results.md
GOGC=off go run . -full -sections eval >> results.md
go test ./bench -run '^$' -bench BenchmarkEvaluation -benchmem -count 5
go test ./...
```

`go run ./bench/cmd/primbench` is the same program as `go run .`, at the path the paper's write-up names. The first Postgres start downloads its binaries.

Flags:

| Flag | Default | Meaning |
| :-- | :-- | :-- |
| `-full` | off | the published run's worker counts, `-conc 1,8,18,64`; a `-conc` given beside it wins |
| `-tolerance` | 2 | the factor, either way, within which a one-worker p50 passes |
| `-conc` | `1,8,64` | worker counts |
| `-local-ops` | 20000 | timed operations per worker, local evaluation |
| `-net-ops` | 2000 | timed operations per worker, every path that crosses a socket |
| `-warmup` | 200 | untimed operations per worker before timing |
| `-rows` | 100000 | read-model rows for the read section |
| `-pg` | embedded | an existing Postgres superuser URL instead of the embedded one |
| `-pg-port` | 15499 | port for the embedded Postgres; 0 takes a free one |
| `-redis` | `127.0.0.1:16379` | Valkey or Redis address |
| `-grants` | `auto` | where the comparison path keeps its grants: `valkey`, `postgres`, or `auto` (Valkey when one answers at `-redis`) |
| `-registry-addr` | `127.0.0.1:18089` | address for the registry stand-in; empty takes a free loopback port |
| `-sections` | `eval,write,read` | which sections to run |
| `-network` | off | run against a NATS cluster at `NATS_URL` and the Postgres at `-pg` or `DATABASE_URL` |

From `dag/`, `go test ./...` runs the correctness tests: every action's verdict, the verdict against a broker enforcing the same grants, the registry stand-in, the bound reads, the record sizes, the verdict lines, and the network mode end to end. `BENCH_REDIS_ADDR=127.0.0.1:16379` adds the Valkey read path to the tests.

**Over the network.** With `-network`, the broker is the cluster at `NATS_URL`, as the identity whose nkey seed is in `NATS_NKEY_SEED` (unset: no authentication). Every timed publish goes to `tenant.acme.dag.bench.<family>.<id>`, captured by the stream `BENCH_SCRATCH`, which nothing reads. When that stream is absent the run creates it with `-scratch-replicas` replicas, or stops before timing anything if the identity may not. Postgres is the server at `-pg` or `DATABASE_URL`. The run creates its own database there, `bench_<unix>`, and drops it at the end. The published numbers are loopback numbers, so a network run prints no verdict.

## What is measured

**The evaluation.** The local path is the evaluation function of the paper. It decodes the envelope, takes `Kind(schema_id)` and the subject, matches the identity's publish grant, finds the stream that binds the subject, and validates the record. Grant and stream matching use the nats-server's own subject trie. A subject has five tokens: `tenant.<tenant>.dag.<family>.<id>`. The sample tenant is `acme`, and the families are `asset`, `edge` and `tag`, one stream each.

The comparison path makes the same three checks as three calls:

1. Postgres: `select family, stream from subject_tree where tenant = $1 and schema_id = $2`, one row per registered schema whose family has a stream.
2. Valkey: `SISMEMBER grant:<identity>:publish <family>`, one set per identity holding the families its patterns match.
3. The registry: `POST /validate/<schema_id>` with the record. It answers 204, 422 or 404, using the same compiled registry as the local path.

Both paths validate with the same code, so the difference between them is the hops. In the parallel variant, the registry call runs beside the other two.

**The grants.** Eight identities: an authority that writes every family, two GPU workers, two browser workers and three editing workstations, four to nine publish patterns each, with the JetStream API subjects collapsed into one `$JS.API.>` pattern. The set is a transcription of my production cluster's record-family patterns. Only the names are generic. The broker creates its streams as an identity of its own, which holds every subject and is not one of the grants.

**The workload.** Three record shapes, each named by size: the small node, a tag (208 bytes, 8 fields); the edge (310 bytes, 10 fields); and the large node, a video clip's asset record (726 bytes, 21 fields). A test holds them to those sizes. Twenty actions, cycled by every worker:

| Count | Action | Expected |
| --: | :-- | :-- |
| 6 | The authority writes the small node | allowed |
| 5 | The authority writes the edge | allowed |
| 3 | The GPU worker writes the large node | allowed |
| 1 | An editing workstation writes the large node | allowed |
| 1 | The authority writes the large node | allowed |
| 1 | The GPU worker writes the small node, a family it is not granted | not granted |
| 1 | A browser worker writes the large node, a family it is not granted | not granted |
| 1 | The authority writes a kind no family knows | no stream |
| 1 | The authority writes the small node with a required field missing | schema |

Every operation's outcome is checked against the expected one. The "wrong" column counts mismatches.

**A write end to end.** The sixteen allowed actions only. Each worker holds one connection per identity to the in-process broker, which enforces the same grants and holds every stream. An operation is the checks, then `Publish`, then waiting for the stream's acknowledgement.

**A read.** A `nodes` table with the read model's tenancy installed: the tenant column, the forced row policy, `mog_tenant()` and the reader role. It holds 100,000 rows of the small node kind across three tenants, eight to one to one. The reader is the first tenant's. One read in ten asks for another tenant's row and must come back empty.

- *Plain read.* `select ... where node_id = $1` as the owning role, which the policy does not bind. This is the floor.
- *Row policy, three round trips.* One round trip opens a read-only transaction as the reader role with the tenant set, one reads, and one commits.
- *Row policy, pipelined.* As the services bind a read: the reader role, the tenant and the read-only flag, and the read, go as one pipeline. Postgres runs them as one implicit transaction, and the settings revert at its end. A test checks that the connection returns to the pool unbound.
- *Valkey grant, then a filtered query.* `SISMEMBER grant:<identity>:read <tenant>`, then `select ... where node_id = $1 and tenant_id = $2` as the owning role.

**Timing.** One `time.Now` pair around each call, with 200 untimed warm-up calls per worker. Percentiles are nearest-rank over every timed call. Workers run at once, and ops/s counts all workers together. When there are more workers than cores, a latency includes time spent waiting to run.

## The published results

I measured these on 19 October 2026 on one machine, with `-conc 1,8,18,64` and the default operation counts.

| | |
| :-- | :-- |
| Computer | Apple M5 Max, 18 cores, 48 GB, macOS 26.5.1 |
| Go | 1.27.1 |
| Postgres | 18.3 from the embedded-postgres binaries, as a native process, over loopback TCP; `max_connections` 300, `shared_buffers` 256 MB |
| Grant store | Valkey 9.1.2 from Homebrew, as a native process, over loopback TCP, persistence off |
| Schema registry stand-in | the benchmark binary re-run as its own process, HTTP/1.1 with keep-alive over loopback |
| Broker | the nats-server v2.14.0 library inside the benchmark process: JetStream, one node, file store in a temporary directory, no wait for disk sync |

Nothing ran in containers. Native processes on loopback are the faster setting for the comparison path.

### The steps, one at a time

`go test ./bench -run '^$' -bench BenchmarkEvaluation -benchmem -count 5`, median of five:

| Step | ns/op | B/op | allocs/op |
| :-- | --: | --: | --: |
| `Kind` and the subject | 71.7 | 112 | 4 |
| Grant match (the GPU worker, a subject of the large node's family) | 66.0 | 0 | 0 |
| Stream lookup | 86.8 | 0 | 0 |
| Schema validation, the large node | 15,171 | 18,673 | 375 |
| The whole evaluation, the large node | 17,467 | 18,893 | 380 |
| The whole evaluation, the small node | 5,504 | 6,569 | 116 |
| The whole evaluation, cycling the workload | 8,712 | 9,168 | 170 |

### The evaluation of one write

| Path | Workers | Ops | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | ops/s | wrong |
| :-- | --: | --: | --: | --: | --: | --: | --: | --: | --: | --: |
| local: one process | 1 | 20000 | 5.96 | 15.04 | 23.62 | 121.50 | 247.25 | 7.87 | 126516 | 0 |
| local: one process | 8 | 160000 | 8.12 | 25.00 | 172.25 | 325.67 | 719.46 | 15.12 | 517989 | 0 |
| local: one process | 18 | 360000 | 10.96 | 39.50 | 529.46 | 1312.12 | 4643.58 | 31.47 | 558484 | 0 |
| local: one process | 64 | 1280000 | 11.25 | 38.42 | 659.75 | 26149.92 | 112533.42 | 96.61 | 612567 | 0 |
| three services, in sequence | 1 | 2000 | 144.88 | 160.83 | 184.96 | 320.92 | 417.67 | 138.73 | 7206 | 0 |
| three services, in sequence | 8 | 16000 | 265.00 | 357.88 | 518.38 | 708.29 | 931.04 | 266.74 | 29876 | 0 |
| three services, in sequence | 18 | 36000 | 469.88 | 655.21 | 892.50 | 1073.42 | 1405.38 | 473.30 | 37888 | 0 |
| three services, in sequence | 64 | 128000 | 1296.88 | 1713.25 | 2252.58 | 2953.88 | 4658.67 | 1282.17 | 49754 | 0 |
| three services, registry in parallel | 1 | 2000 | 101.21 | 110.67 | 179.04 | 333.75 | 440.42 | 101.40 | 9856 | 0 |
| three services, registry in parallel | 8 | 16000 | 237.25 | 321.33 | 436.42 | 589.58 | 820.88 | 241.97 | 32927 | 0 |
| three services, registry in parallel | 18 | 36000 | 448.12 | 601.08 | 779.00 | 1016.79 | 1368.29 | 454.09 | 39490 | 0 |
| three services, registry in parallel | 64 | 128000 | 1328.38 | 1753.42 | 2325.79 | 2883.62 | 7138.08 | 1332.43 | 47849 | 0 |

The local path's tail grows from eight workers because the collector reclaims the validator's allocations, 375 for the large node. With the collector off (`GOGC=off`), the same run gives:

| Path | Workers | Ops | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | ops/s | wrong |
| :-- | --: | --: | --: | --: | --: | --: | --: | --: | --: | --: |
| local: one process | 1 | 20000 | 5.83 | 15.62 | 20.88 | 35.50 | 48.96 | 7.61 | 130958 | 0 |
| local: one process | 8 | 160000 | 8.42 | 23.29 | 37.04 | 65.17 | 1044.25 | 11.18 | 702708 | 0 |
| local: one process | 18 | 360000 | 11.62 | 33.38 | 77.67 | 279.54 | 1626.21 | 16.51 | 1077940 | 0 |
| local: one process | 64 | 1280000 | 11.08 | 32.38 | 155.38 | 853.33 | 142328.75 | 56.90 | 947068 | 0 |

### One write end to end

| Path | Workers | Ops | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | ops/s | wrong |
| :-- | --: | --: | --: | --: | --: | --: | --: | --: | --: | --: |
| publish only, no checks | 1 | 2000 | 46.96 | 60.08 | 77.88 | 209.75 | 364.04 | 49.16 | 20328 | 0 |
| publish only, no checks | 8 | 16000 | 119.21 | 171.54 | 299.38 | 2769.33 | 3129.92 | 130.31 | 61120 | 0 |
| publish only, no checks | 18 | 36000 | 178.33 | 276.71 | 467.21 | 858.92 | 1151.88 | 192.90 | 92646 | 0 |
| publish only, no checks | 64 | 128000 | 446.88 | 707.96 | 1411.92 | 2010.58 | 6220.83 | 484.38 | 131150 | 0 |
| local checks, then publish | 1 | 2000 | 45.67 | 58.38 | 67.38 | 155.33 | 168.46 | 47.55 | 21009 | 0 |
| local checks, then publish | 8 | 16000 | 115.42 | 163.83 | 226.62 | 458.75 | 835.88 | 120.32 | 65958 | 0 |
| local checks, then publish | 18 | 36000 | 177.58 | 270.58 | 438.17 | 851.62 | 1145.62 | 190.28 | 93853 | 0 |
| local checks, then publish | 64 | 128000 | 454.29 | 732.17 | 1461.33 | 2124.21 | 3735.92 | 492.33 | 129124 | 0 |
| three services, then publish | 1 | 2000 | 188.54 | 205.50 | 251.17 | 429.92 | 467.17 | 191.38 | 5224 | 0 |
| three services, then publish | 8 | 16000 | 357.58 | 450.46 | 615.38 | 785.88 | 916.71 | 368.28 | 21629 | 0 |
| three services, then publish | 18 | 36000 | 661.29 | 842.96 | 1096.00 | 1409.50 | 5008.04 | 680.01 | 26385 | 0 |
| three services, then publish | 64 | 128000 | 1791.17 | 2258.25 | 2866.83 | 4193.92 | 9328.29 | 1828.17 | 34871 | 0 |

### One read by id under the tenant policy

| Path | Workers | Ops | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | ops/s | wrong |
| :-- | --: | --: | --: | --: | --: | --: | --: | --: | --: | --: |
| plain read, no policy (the floor) | 1 | 2000 | 26.04 | 31.08 | 36.83 | 42.04 | 47.50 | 26.64 | 37498 | 0 |
| plain read, no policy (the floor) | 8 | 16000 | 60.29 | 85.79 | 110.96 | 134.71 | 197.42 | 62.74 | 126723 | 0 |
| plain read, no policy (the floor) | 18 | 36000 | 136.29 | 191.46 | 254.04 | 354.50 | 456.33 | 141.12 | 127042 | 0 |
| plain read, no policy (the floor) | 64 | 128000 | 441.33 | 554.46 | 690.46 | 800.04 | 1096.29 | 446.47 | 142936 | 0 |
| row policy, three round trips | 1 | 2000 | 71.58 | 81.88 | 91.00 | 104.58 | 172.88 | 72.51 | 13784 | 0 |
| row policy, three round trips | 8 | 16000 | 179.00 | 219.58 | 259.79 | 293.38 | 317.88 | 180.71 | 44150 | 0 |
| row policy, three round trips | 18 | 36000 | 407.17 | 499.08 | 592.96 | 677.25 | 903.12 | 414.13 | 43392 | 0 |
| row policy, three round trips | 64 | 128000 | 1296.88 | 1521.58 | 1753.29 | 1940.88 | 2262.75 | 1309.41 | 48783 | 0 |
| row policy, pipelined (1 round trip) | 1 | 2000 | 30.62 | 36.17 | 42.96 | 57.67 | 72.54 | 31.06 | 32163 | 0 |
| row policy, pipelined (1 round trip) | 8 | 16000 | 66.67 | 94.08 | 123.12 | 154.33 | 183.92 | 69.49 | 114666 | 0 |
| row policy, pipelined (1 round trip) | 18 | 36000 | 139.17 | 190.17 | 244.21 | 312.08 | 450.12 | 143.49 | 124988 | 0 |
| row policy, pipelined (1 round trip) | 64 | 128000 | 436.42 | 568.79 | 733.33 | 883.38 | 1553.54 | 443.28 | 143903 | 0 |
| Valkey grant, then filtered query | 1 | 2000 | 94.83 | 103.50 | 113.58 | 138.29 | 195.54 | 95.63 | 10453 | 0 |
| Valkey grant, then filtered query | 8 | 16000 | 150.83 | 189.08 | 232.54 | 280.38 | 358.04 | 153.18 | 52094 | 0 |
| Valkey grant, then filtered query | 18 | 36000 | 273.58 | 350.50 | 427.67 | 479.42 | 577.88 | 277.36 | 64650 | 0 |
| Valkey grant, then filtered query | 64 | 128000 | 828.25 | 1076.46 | 1302.04 | 1462.17 | 2069.67 | 839.21 | 76004 | 0 |

## Reading the results

- **The lookups on the name are free next to everything else.** Together they take about 225 ns. Schema validation is about 88 percent of the large node's evaluation. A faster evaluation needs a faster validator, not a different key.
- **Three calls cost about 140 µs at one worker on loopback.** The local evaluation takes about 6 µs. At 64 workers the three calls cost about 1.3 ms, and throughput is about 50,000 a second against about 610,000 a second.
- **For a write, the publish dominates.** The local checks before a publish are lost in its noise. The three calls add about 140 µs at one worker and 1.3 ms at 64.
- **The read policy costs nothing measurable when its binding is pipelined.** Sent as one pipeline, the bound read costs 30.6 µs at p50 against 26.0 µs for a plain read. Bound in three round trips, as the services bind it, it costs 71.6 µs. At 64 workers that is slower than the Valkey path, because Postgres is the bottleneck and the binding triples the trips to it.
- **Caching.** The comparison path makes every call on every operation. A production stack caches schemas and grants in the writer. That closes the latency gap and opens a staleness window instead.

## Not measured

> [measurement: the same harness against a three-node cluster from a machine in the same region: publish p50 and p99 at 1, 8 and 64 workers, with each stream at three replicas]

> [measurement: the comparison path with its three services in Docker containers, as first briefed: not run, because Docker is not installed on the benchmark machine]

> [measurement: the comparison path across a real network, the three services on other hosts in one availability zone]

## The one-command run, in full

The output of `go run .` from `dag/` on my Mac, with nothing but Go installed, the run whose verdict lines open this page. Its own wall time line counts the run, not the download and compile before it.

| Machine | Value |
| :-- | :-- |
| cpu | Apple M5 Max, 18 logical cores |
| os | darwin/arm64 |
| go | go1.27.1 |
| postgres | 18.3 (embedded binaries, native process, loopback TCP) |
| grant store | Postgres table grant_set, in the same database as the hierarchy (no Valkey is reachable here, so the grant sets are a Postgres table) |
| registry | this binary, own process, HTTP/1.1 keep-alive over loopback |
| broker | nats-server library in this process, JetStream, one node, file store |
| gogc | 100 (default) |
| run | 2026-10-20T23:48:48Z |

Latencies are wall time per operation, one clock pair per call, warm-up excluded. Workers run concurrently; ops/s is all workers together.

#### Evaluation of one incoming write

The workload cycles through 20 actions: 16 allowed, and one each refused as not granted (twice), no stream and schema. A wrong answer is an action whose refusal differs from the one it was written for.

| Path | Workers | Ops | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | ops/s | wrong |
| :-- | --: | --: | --: | --: | --: | --: | --: | --: | --: | --: |
| local: one process | 1 | 20000 | 6.00 | 15.46 | 23.00 | 79.79 | 240.38 | 7.83 | 127127 | 0 |
| local: one process | 8 | 160000 | 8.08 | 22.33 | 125.29 | 362.25 | 1064.25 | 13.77 | 569345 | 0 |
| local: one process | 64 | 1280000 | 11.04 | 37.08 | 633.42 | 25605.71 | 144083.67 | 95.02 | 629536 | 0 |
| three services, in sequence | 1 | 2000 | 102.12 | 118.33 | 195.75 | 311.12 | 363.42 | 98.21 | 10178 | 0 |
| three services, in sequence | 8 | 16000 | 240.92 | 379.62 | 537.62 | 672.33 | 985.50 | 250.34 | 31806 | 0 |
| three services, in sequence | 64 | 128000 | 1329.46 | 1908.67 | 2939.08 | 4230.33 | 11194.17 | 1345.10 | 47334 | 0 |
| three services, registry in parallel | 1 | 2000 | 63.75 | 78.17 | 160.33 | 270.92 | 312.96 | 66.91 | 14936 | 0 |
| three services, registry in parallel | 8 | 16000 | 220.21 | 314.46 | 462.29 | 646.00 | 1147.50 | 232.11 | 34300 | 0 |
| three services, registry in parallel | 64 | 128000 | 1318.25 | 1819.62 | 2889.08 | 4186.71 | 7464.83 | 1361.91 | 46747 | 0 |

#### One write end to end

The 16 allowed actions of the workload: the checks, then a publish to a one-node JetStream broker in this process that enforces the same grants and holds every stream, waiting for the stream's acknowledgement. The broker writes to a file store on this machine's disk and does not wait for an fsync.

| Path | Workers | Ops | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | ops/s | wrong |
| :-- | --: | --: | --: | --: | --: | --: | --: | --: | --: | --: |
| publish only, no checks | 1 | 2000 | 47.04 | 77.54 | 158.83 | 289.96 | 925.08 | 54.35 | 18384 | 0 |
| publish only, no checks | 8 | 16000 | 116.00 | 163.04 | 261.42 | 675.83 | 916.88 | 121.79 | 65113 | 0 |
| publish only, no checks | 64 | 128000 | 443.08 | 730.92 | 1431.62 | 2110.62 | 3321.75 | 483.47 | 131353 | 0 |
| local checks, then publish | 1 | 2000 | 49.33 | 62.92 | 79.46 | 102.21 | 156.92 | 51.51 | 19396 | 0 |
| local checks, then publish | 8 | 16000 | 114.96 | 159.88 | 221.08 | 466.46 | 714.54 | 118.81 | 66974 | 0 |
| local checks, then publish | 64 | 128000 | 451.12 | 698.50 | 1310.79 | 2179.58 | 3497.33 | 481.67 | 132071 | 0 |
| three services, then publish | 1 | 2000 | 147.54 | 168.58 | 273.83 | 451.25 | 475.17 | 151.75 | 6587 | 0 |
| three services, then publish | 8 | 16000 | 338.62 | 428.83 | 594.12 | 743.25 | 927.17 | 349.61 | 22837 | 0 |
| three services, then publish | 64 | 128000 | 1852.62 | 2335.92 | 3138.42 | 4585.42 | 7951.71 | 1891.77 | 33717 | 0 |

#### One read by id under the tenant policy

100000 tag rows across three tenants, eight to one to one. The reader is the first tenant's. One read in ten asks for another tenant's row and must come back empty; a wrong answer is a read that did not.

| Path | Workers | Ops | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | mean µs | ops/s | wrong |
| :-- | --: | --: | --: | --: | --: | --: | --: | --: | --: | --: |
| plain read, no policy (the floor) | 1 | 2000 | 26.50 | 31.92 | 41.33 | 58.33 | 78.96 | 27.33 | 36545 | 0 |
| plain read, no policy (the floor) | 8 | 16000 | 62.83 | 88.21 | 114.33 | 135.29 | 165.29 | 64.88 | 122731 | 0 |
| plain read, no policy (the floor) | 64 | 128000 | 433.96 | 570.08 | 730.58 | 880.21 | 1526.62 | 441.89 | 144386 | 0 |
| row policy, three round trips | 1 | 2000 | 72.92 | 82.83 | 95.88 | 128.29 | 162.71 | 73.94 | 13519 | 0 |
| row policy, three round trips | 8 | 16000 | 181.08 | 226.62 | 271.33 | 304.50 | 357.38 | 183.76 | 43440 | 0 |
| row policy, three round trips | 64 | 128000 | 1310.04 | 1542.38 | 1784.00 | 2051.46 | 3552.96 | 1323.54 | 48269 | 0 |
| row policy, pipelined (1 round trip) | 1 | 2000 | 31.62 | 37.00 | 42.83 | 49.50 | 134.21 | 32.01 | 31211 | 0 |
| row policy, pipelined (1 round trip) | 8 | 16000 | 66.71 | 95.12 | 124.67 | 163.58 | 189.88 | 69.75 | 114070 | 0 |
| row policy, pipelined (1 round trip) | 64 | 128000 | 433.04 | 578.96 | 821.54 | 1591.92 | 8282.46 | 439.25 | 144955 | 0 |
| Postgres grant, then filtered query | 1 | 2000 | 50.88 | 58.88 | 66.67 | 91.50 | 159.00 | 51.80 | 19289 | 0 |
| Postgres grant, then filtered query | 8 | 16000 | 122.42 | 158.58 | 195.88 | 228.46 | 290.08 | 124.72 | 63910 | 0 |
| Postgres grant, then filtered query | 64 | 128000 | 872.12 | 1068.12 | 1273.21 | 1452.12 | 1843.58 | 882.94 | 72334 | 0 |

#### The verdict

Each section's one-worker p50s against the published ones (Apple M5 Max, loopback, grants in Valkey), within 2x either way, with no wrong answer. No Valkey was listening, so this run's comparison path kept its grants in a Postgres table.

```
evaluation: local 6.0 µs (published 6.0), three services 102.1 µs (published 144.9), registry in parallel 63.8 µs (published 101.2); 0 wrong; within 2x: PASS
write: publish only 47.0 µs (published 47.0), local checks then publish 49.3 µs (published 45.7), three services then publish 147.5 µs (published 188.5); 0 wrong; within 2x: PASS
read: plain 26.5 µs (published 26.0), policy pipelined 31.6 µs (published 30.6), policy bound 72.9 µs (published 71.6), Postgres grant then query 50.9 µs (no published number: no Valkey was listening); 0 wrong; within 2x: PASS
```

Wall time: 32.4 s

#### The steps

From `go test ./bench -run '^$' -bench BenchmarkEvaluation -benchmem -count 1`:

```
BenchmarkEvaluation/kind_and_subject-18         	38031800	        28.34 ns/op	      48 B/op	       1 allocs/op
BenchmarkEvaluation/grant_match-18              	18121903	        64.94 ns/op	       0 B/op	       0 allocs/op
BenchmarkEvaluation/stream_lookup-18            	14914644	        81.55 ns/op	       0 B/op	       0 allocs/op
BenchmarkEvaluation/schema_validate_asset-18    	   96145	     12864 ns/op	   16288 B/op	     361 allocs/op
BenchmarkEvaluation/whole_asset-18              	   80803	     13322 ns/op	   16469 B/op	     362 allocs/op
BenchmarkEvaluation/whole_tag-18                	  337116	      3596 ns/op	    5463 B/op	     105 allocs/op
BenchmarkEvaluation/whole_mix-18                	  194474	      6192 ns/op	    8009 B/op	     163 allocs/op
```

The validation is the same validator on the same schemas; the decode is not (below), so this run allocates 361 times to validate the large node where the published run did 375.

## How this repository differs from the build I measured

- **Names.** The tenant, the identities and the subjects are generic. The grant set keeps the production shape, pattern for pattern.
- **The stream table** holds the three families the workload writes. A stream lookup walks the subject's tokens, so its cost does not depend on how many streams the table holds.
- **`Kind`** is one lookup in a family map keyed by schema identifier, seeded from a table for the three embedded schemas, as in production, where the map holds every schema identifier. `go test ./internal/ledger -bench Kind` measures it alone: 3.7 ns, no allocation.
- **The schemas** keep every validation keyword of the originals: types, patterns, enums, `if` and `else`, `additionalProperties`. Descriptions and examples are removed.
- **The decode.** This harness decodes a record once, with its own decoder over the bytes (`dag/internal/schema/decode.go`, held by a test to what `jsonschema.UnmarshalJSON` returns), reads the envelope from the decoded value and validates that value. The build I measured decoded each record twice, through `encoding/json`. So the large node here takes 362 allocations and about 13 µs where the published run took 380 and 17.5 µs; the comparison path takes the same decode, so the difference between the paths is unchanged. The 361 allocations left are the validator's.
- **The network mode** reports to standard output only.
- **The default run** keeps its grants in Postgres when no Valkey is listening. `-full` with Valkey running is the published setting.

## Where it is checked

Paths are relative to the repository's root.

| Part | Where |
| :-- | :-- |
| The local path | `dag/bench/primitive.go` (`CheckPublish`) |
| The comparison path | `dag/bench/remote.go` |
| The grants | `dag/bench/grants.go` (`DevGrants`) |
| The stream table, `Kind` and the subject | `dag/internal/ledger/streams.go` (`Streams`, `Kind`, `SubjectFor`) |
| The schema registry | `dag/internal/schema/`, schemas in `dag/internal/schema/schemas/` |
| The tenancy install and the bound read | `dag/internal/readmodel/tenancy.go` (`EnsureTenancy`), `dag/internal/readmodel/bound.go` (`Bind`) |
| The bound reads | `dag/bench/read.go` |
| The three records | `dag/bench/workload.go`: the small node is `sch_Tag_v1`, the edge is `sch_Edge_Derivation_v1`, the large node is `sch_Asset_ProRes_v2` |
| The driver and the verdict | `dag/main.go`, `dag/bench/cmd/primbench`, `dag/internal/driver/driver.go`, `dag/internal/driver/verdict.go` |
| The correctness tests | `dag/main_test.go`, `dag/bench/primitive_test.go`, `dag/bench/grants_test.go`, `dag/bench/read_test.go`, `dag/bench/workload_test.go`, `dag/internal/ledger/streams_test.go`, `dag/internal/driver/verdict_test.go` |
