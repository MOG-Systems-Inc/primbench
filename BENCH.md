# The benchmark

**What this is.** The method and the full measured results behind section 8 of [`SPEC.md`](./SPEC.md). I measured every number here on one machine on 19 October 2026. None is projected or estimated. The harness and the files it is checked against are listed at the end.

## The machine

| | |
| :-- | :-- |
| Computer | Apple M5 Max, 18 cores, 48 GB, macOS 26.5.1 |
| Go | 1.27.1 |
| Postgres | 18.3 from the embedded-postgres binaries the repo's tests use, as a native process, over loopback TCP; `max_connections` 300, `shared_buffers` 256 MB |
| Grant store | Valkey 9.1.2 from Homebrew, as a native process, over loopback TCP, persistence off |
| Schema registry stand-in | the benchmark binary re-run as its own process, HTTP/1.1 with keep-alive over loopback |
| Broker | the nats-server v2.14.0 library inside the benchmark process: JetStream, one node, file store in a temporary directory, no wait for disk sync |

Docker is not installed on this machine, so nothing ran in containers. Native processes on loopback are the faster setting for the comparison path. Docker Desktop on macOS routes container traffic through a virtual machine, which adds to every round trip.

Valkey's own tool, run single-client on the same machine, gives about 70 µs a round trip:

```
valkey-benchmark -p 16379 -c 1 -n 20000 -t ping_mbulk,sadd
  avg 0.068 ms   p50 0.071 ms   p99 0.087 to 0.103 ms
```

The harness's own Valkey client measures the same, so the comparison path's cost is the server's round trip and not the client.

## What is measured

**The evaluation.** The local path is the evaluation function of the paper's section 3. It decodes the envelope, takes `Kind(schema_id)` and the subject, matches the identity's publish grant, finds the stream that binds the subject, and validates the record. Grant and stream matching use the nats-server's subject trie. The grants are a transcription of the production cluster's record-family patterns: four to nine publish patterns per identity, with the JetStream API subjects collapsed into one `$JS.API.>` pattern. The cluster's rendered grants are larger (the authority's has 923 patterns, almost all JetStream API subjects); the trie's cost is a hash lookup per token level and does not grow with the pattern count, and the larger grants are not separately measured here.

The comparison path makes the same three checks as three calls:

1. Postgres: `select family, stream from subject_tree where tenant = $1 and schema_id = $2`, one row per registered schema whose family has a stream.
2. Valkey: `SISMEMBER grant:<identity>:publish <family>`, one set per identity holding the families its patterns match.
3. The registry: `POST /validate/<schema_id>` with the record. It answers 204, 422 or 404, using the same compiled registry as the local path.

Both paths validate with the same code. The difference between them is the hops. In the parallel variant, the registry call runs beside the other two.

**The workload.** Three record shapes from the registry, each named here by size: the small node (208 bytes, 8 fields), the edge (310 bytes, 10 fields) and the large node (726 bytes, 21 fields). The schema identifiers are in the table at the end. Twenty actions, cycled by every worker:

| Count | Action | Expected |
| --: | :-- | :-- |
| 6 | The authority writes the small node: 208 bytes, 8 fields | allowed |
| 5 | The authority writes the edge: 310 bytes, 10 fields | allowed |
| 3 | The GPU worker writes the large node: 726 bytes, 21 fields | allowed |
| 1 | An editing workstation writes the large node | allowed |
| 1 | The authority writes the large node | allowed |
| 1 | The GPU worker writes the small node, a family it is not granted | not granted |
| 1 | A browser worker writes the large node, a family it is not granted | not granted |
| 1 | The authority writes a kind no family knows | no stream |
| 1 | The authority writes the small node with a required field missing | schema |

Every operation's outcome is checked against the expected one. The "wrong" column counts mismatches, and it is zero everywhere.

**A write end to end.** The sixteen allowed actions only. Each worker holds one connection per identity to the in-process broker, which enforces the same grants and holds every stream in the stream table. An operation is the checks, then `Publish`, then waiting for the stream's acknowledgement.

**A read.** The projector's `nodes` table, with the read model's tenancy installed as the projector installs it: the tenant column, the forced policy, `mog_tenant()` and the reader role. It holds 100,000 rows of the small node kind across three tenants, eight to one to one. The reader is the first tenant's. One read in ten asks for another tenant's row and must come back empty.

- *Plain read.* `select … where node_id = $1` as the owning role, which the policy does not bind. This is the floor.
- *Row policy, three round trips.* One round trip opens a read-only transaction as the reader role with the tenant set, one reads, and one commits. The services bind with the pipelined form below, which [`SPEC.md`](./SPEC.md) section 8 measures with the services' own code.
- *Row policy, pipelined.* The settings and the read go as one pipeline, as the services bind every read. Postgres runs them as one implicit transaction, and the settings revert at its end. A test checks that the connection returns to the pool unbound.
- *Valkey grant, then a filtered query.* `SISMEMBER grant:<identity>:read <tenant>`, then `select … where node_id = $1 and tenant_id = $2` as the owning role.

**Timing.** One `time.Now` pair around each call, with 200 untimed warm-up calls per worker. Percentiles are nearest-rank over every timed call. Workers run at once, and ops/s counts all workers together. At 64 workers there are more workers than cores, so a latency includes time spent waiting to run.

## Results

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

Repeated on 20 October 2026 on the same machine, five more runs each, with a shorter tenant token, so the records are 716, 198 and 304 bytes: `Kind` and the subject 76 to 77 ns, grant match 60 to 63 ns, stream lookup 81 to 82 ns, validation of the large node 14.8 to 15.3 µs, the whole large node 16.5 to 17.4 µs, the whole small node 5.1 to 5.4 µs, the workload's mix 8.2 to 8.4 µs. The allocation counts matched exactly.

Every table below is a single run of the driver. Its percentiles are over every timed call of that run, and no confidence interval is given; the spread above is for the step benchmarks only. The two 64-worker rows that read 1,296.88 µs at p50 are different workloads that happened to land on the same value of the machine's 41.67 ns clock; the run's raw output was not kept, so the coincidence is recorded, not rechecked.

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
| the checks, then publish (harness "publish only" row) | 1 | 2000 | 46.96 | 60.08 | 77.88 | 209.75 | 364.04 | 49.16 | 20328 | 0 |
| the checks, then publish (harness "publish only" row) | 8 | 16000 | 119.21 | 171.54 | 299.38 | 2769.33 | 3129.92 | 130.31 | 61120 | 0 |
| the checks, then publish (harness "publish only" row) | 18 | 36000 | 178.33 | 276.71 | 467.21 | 858.92 | 1151.88 | 192.90 | 92646 | 0 |
| the checks, then publish (harness "publish only" row) | 64 | 128000 | 446.88 | 707.96 | 1411.92 | 2010.58 | 6220.83 | 484.38 | 131150 | 0 |
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

- **The lookups on the name are free next to everything else.** Together they take about 225 ns. Schema validation is about 87 percent of the large node's evaluation. A faster evaluation needs a faster validator, not a different key.
- **Three calls cost about 140 µs at one worker on loopback.** The local evaluation takes about 6 µs. At 64 workers the three calls cost about 1.3 ms, and throughput is about 50,000 a second against about 610,000 a second.
- **For a write, the publish dominates.** The local checks are about 6 to 9 µs from the evaluation table, against a loopback publish of about 47 µs, so they are a small part of a single-node publish. The "publish only, no checks" row runs those same checks, so it is not a no-checks baseline; read the checks' cost from the evaluation table, not from the gap between those two rows. The three service calls add about 140 µs at one worker and 1.3 ms at 64.
- **The read policy costs nothing measurable when its binding is pipelined.** Sent as one pipeline, the bound read costs 30.6 µs at p50 against 26.0 µs for a plain read. Bound in three round trips, it costs 71.6 µs. At 64 workers that is slower than the Valkey path, because Postgres is the bottleneck and the binding triples the trips to it.
- **Caching.** The comparison path makes every call on every operation. A production stack caches schemas and grants in the writer. That closes the latency gap and opens a staleness window instead.

## Measured in the deployment

The same harness was run inside the deployment on 19 October 2026, against the three-node cluster with each stream at three replicas, from a task in the region that holds the cluster. Those results are reported in section 8 of [`SPEC.md`](./SPEC.md) under "The same harness inside the deployment": the evaluation, the write end to end, and the read under the policy, each at 1 and 64 workers. They are not repeated here.

The harness has no path that publishes without the checks, so the cost of the checks is read from the evaluation table above, about 6 to 9 microseconds for this workload's mix, against the publish latency.

## How to run it

From the implementation's `dag/` directory:

```
valkey-server --port 16379 --bind 127.0.0.1 --save '' --appendonly no --daemonize yes
go run ./bench/cmd/primbench > results.md
GOGC=off go run ./bench/cmd/primbench -sections eval >> results.md
go test ./bench -run '^$' -bench BenchmarkEvaluation -benchmem -count 5
```

Flags: `-conc` (worker counts, default `1,8,64`; this run used `1,8,18,64`), `-local-ops` (20,000 per worker), `-net-ops` (2,000 per worker), `-rows` (100,000), `-pg` (an existing Postgres superuser URL instead of the embedded one), `-redis` (address), `-sections` (`eval,write,read`). Postgres starts from the embedded binaries on port 15499. The registry stand-in listens on 127.0.0.1:18089.

`go test ./bench` runs the correctness tests: every action's verdict, the verdict against a broker enforcing the same grants, the registry stand-in, and the bound reads. `BENCH_REDIS_ADDR=127.0.0.1:16379` adds the Valkey read path.

## Where it is checked

Paths are relative to the implementation's repository root, which carries the `dag/bench` harness, and to the infrastructure definitions' root, from which the harness's grants are transcribed. The services bind a read in one round trip; the tables keep both the three-round-trip and the pipelined one-round-trip measurements.

| Part | Where it is checked |
| :-- | :-- |
| The local path | `dag/bench/primitive.go` (`CheckPublish`) |
| The comparison path | `dag/bench/remote.go` |
| The grants | the infrastructure definitions that render the broker configuration (private) |
| The stream table | `dag/internal/ledger/streams.go` (`Streams`) |
| The tenancy install and the bound read | `dag/internal/readmodel/tenancy.go` (`EnsureTenancy`), `dag/internal/readmodel/bound.go` (`Bind`) |
| The three records | `dag/bench/workload.go`: the small node is `sch_Tag_v1`, the edge is `sch_Edge_Derivation_v1`, the large node is `sch_Asset_ProRes_v2` |
| The driver and the correctness tests | `dag/bench/cmd/primbench`, `dag/bench/primitive_test.go` |
