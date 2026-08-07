# Txn-file Integration Test Design

## 1. Summary

Add `integration_tests_in_docker`, a standalone Docker-based Go module that exercises client-go's txn-file path against real NextGen PD, three real TiKV stores, a real `tikv-worker`, and MinIO-backed DFS. The test process runs inside the Compose network; TiDB, mocks, traffic proxies, and host port mappings are not part of this suite.

The initial suite has two end-to-end cases:

1. an optimistic transaction is serialized into multiple txn chunks, spans at least three Regions, commits, and is read back through client-go; and
2. a later-Region write conflict causes a determinate txn-file failure after the lower Region has prewritten, with no committed partial values and no leaked locks on the prewritten keys.

This suite complements the request-level mock coverage in `integration_tests/txn_file_test.go`. It proves compatibility across the client serialization, worker HTTP contract, DFS storage, TiKV txn-file execution, PD keyspaces, Region routing, and cleanup behavior that mocks cannot prove.

## 2. Current Behavior Being Tested

### 2.1 Txn-file selection

`transaction.KVTxn.Commit` first honors the transaction-level `KVTxn.DisableTxnFile()` flag. When it has not been set, `twoPhaseCommitter.useTxnFile` selects txn-file only if all of the following hold:

- `txn.vars.DisableTxnFile` is false;
- the transaction is optimistic; pessimistic transactions are rejected;
- `shouldWriteBinlog()` is false;
- `config.TiKVClient.TxnChunkWriterAddr` is non-empty;
- `txn.GetMemBuffer().Size()` in bytes is at least the selected minimum; and
- the request source is eligible.

The minimum is `txn.vars.TxnFileMinMutationSize` when that per-transaction override is non-zero; otherwise it is `config.TiKVClient.TxnFileMinMutationSize`. For an internal request, client-go halves the selected minimum using integer division before comparing it. An external request is allowed regardless of `TxnFileRequestSourceWhitelist`; an internal request must have its request-source type in that allowlist. These rules come from `txnkv/transaction/txn.go`, `txnkv/transaction/txn_file.go::useTxnFile`, `kv/variables.go`, and `config/client.go`.

The suite sets the global minimum to `1` only to make its small fixtures eligible. Setup, seeding, competing writes, and cleanup explicitly call `DisableTxnFile()` so those transactions cannot affect the target txn-file metric or behavior. The target transactions remain ordinary optimistic transactions and use the default external request source.

### 2.2 Actual execution and failure semantics

`twoPhaseCommitter.initKeysAndMutations` iterates the MemBuffer in key order. The first eligible mutation becomes the primary (the first mutation that is not a check-not-exists or shared-lock primary candidate). `executeTxnFile` then performs this order:

1. serialize the already sorted mutations and upload their chunks;
2. optionally pre-split Region boundaries derived from chunk ranges;
3. synchronously prewrite the primary batch first;
4. finish all secondary prewrites, with bounded concurrency and retry/regrouping as needed;
5. obtain a commit timestamp;
6. synchronously commit the primary batch; then
7. schedule secondary commits asynchronously.

`txnChunkSlice.groupToBatches` orders batches by their smallest chunk key so the primary batch is first. The primary prewrite starts the TTL manager; secondary prewrites are complete before the commit timestamp is requested. These details matter to the conflict test: its ordered `a`, `b`, `c` mutations and Region layout make the `a`/`b` primary batch prewrite before the `c` batch discovers the conflict.

For a determinate error before a successful primary commit, the deferred path runs txn-file rollback: the primary rollback is synchronous and secondary rollbacks are asynchronous. If the primary commit has an RPC error or reports an undetermined result, client-go marks the outcome undetermined and deliberately skips rollback. This initial suite creates no undetermined outcome and must not claim that rollback handles one.

Rollback concerns transactional locks and values, not DFS garbage collection. A successful POST creates a chunk object, but client-go attaches chunk IDs only after a fully successful build has collected and sorted successful results. If collection returns early on an error, already accepted chunks are not attached or tracked, and the forked backoffer cancels in-flight requests best-effort. POST retries are non-idempotent. A partially failed build can therefore leave accepted, untracked or orphaned chunks. Neither test asserts DFS file deletion, and orphan reclamation, crash recovery, and recovery of an undetermined commit remain outside the initial scope in section 3.2.

### 2.3 Chunk protocol and allocation behavior

`txnkv/transaction/txn_file.go::newChunkWriterClient` posts to:

```text
POST /txn_chunk?keyspace_id=<keyspace-id>
```

It does not send `api_version`. The body is zero or more entries followed by a little-endian IEEE CRC32:

```text
u16 key length | key | u8 op | u32 value length | value | u32 CRC32
```

`cloud-storage-engine/components/cloud_worker/src/txn_chunk.rs::handle_txn_chunk` defaults a missing `api_version` to v1 and parses that exact format. API v2 adds a one-byte `pessimistic_action` after `op`; it must not be selected by this suite because client-go does not emit that byte. `server.rs::start` routes both `/txn_chunk` and the valid `/healthz` endpoint.

Each chunk is built in one serialization buffer. The key and value bytes are copied into that buffer once, then the CRC is appended. Chunk ranges retain references to mutation keys and mutation slices retain references to their backing data; they do not clone keys or values for accounting. HTTP retries create a new `bytes.Reader` over the same completed `[]byte`. Do not add production hot-path counters, tracing, defensive copies, or packet capture to observe this test. The tests infer the intended number of chunks from exact serialized sizes and use the existing txn-file counters only to prove path selection.

`TxnChunkMaxSize` is a target for packing, not a hard upper bound on a single oversized entry. Client-go flushes a non-empty payload before an entry only when appending that entry and the four-byte CRC would exceed the target. It then appends the entry even if that single entry plus CRC exceeds the target. The fixtures instead require every entry plus CRC to fit the 256-byte target so their chunk calculation is exact and unambiguous.

## 3. Goals and Non-goals

### 3.1 Goals

- Exercise the public client-go transaction API against real PD, TiKV, and `tikv-worker` binaries.
- Use the same PD and TiKV/worker images as cloud-storage-engine's Docker Compose environment.
- Create and use an explicit API-v2 keyspace without starting TiDB.
- Exercise multiple txn chunks and multiple Regions.
- Verify both success and determinate-failure rollback semantics.
- Make local and CI execution deterministic, one-command, and diagnosable.
- Keep Docker assets reusable by later Docker-based integration suites.

### 3.2 Non-goals

- Replacing existing unit or mock integration tests.
- Testing TiDB SQL behavior, schema files, coprocessor workers, resource control, TLS, backup, restore, CDC, or remote compaction.
- Testing pessimistic txn-file transactions. Client-go currently rejects pessimistic transactions in `useTxnFile`.
- Testing crash recovery, network fault injection, undetermined commit outcomes, Region movement, or worker high availability in the initial suite.
- Publishing or rebuilding PD, TiKV, or `tikv-worker` images.

## 4. Repository Layout and Module Boundary

```text
integration_tests_in_docker/
|-- AGENTS.md
|-- README.md
|-- go.mod
|-- go.sum
|-- docker-compose/
|   |-- Dockerfile.test
|   |-- Dockerfile.test.dockerignore
|   |-- docker-compose.yml
|   |-- run.sh
|   |-- configs/
|   |   |-- pd.toml
|   |   |-- tikv-1.toml
|   |   |-- tikv-2.toml
|   |   |-- tikv-3.toml
|   |   `-- tikv-worker.toml
|   `-- bootstrap/
|       |-- create-keyspace.sh
|       `-- init-minio.sh
`-- txn_file/
    |-- helpers_test.go
    |-- main_test.go
    `-- txn_file_test.go
```

This remains a separate module, like `integration_tests`, so its dependencies and Docker workflow do not affect the main module's test graph:

```go
module integration_tests_in_docker

go 1.25.10

replace github.com/tikv/client-go/v2 => ../
```

Its direct dependencies are only:

- `github.com/tikv/client-go/v2` (replaced by `../`);
- `github.com/pingcap/kvproto` for API-version and direct RPC protobuf types;
- `github.com/google/uuid` for isolated prefixes;
- `github.com/prometheus/client_golang` for counter reads;
- `github.com/stretchr/testify`; and
- `go.uber.org/goleak`.

`go mod tidy` determines indirect dependencies. The module must not add TiDB, PD client, failpoint, mock, or proxy dependencies. Tests use `util/codec.EncodeBytes`, not any TiDB package.

`Dockerfile.test` builds from the client-go repository root, because the replacement points one level above the module. Its Compose build has a root context and a Dockerfile path under `integration_tests_in_docker/docker-compose`. It copies the root and integration-module `go.mod`/`go.sum` files before `go mod download`, then copies source. `Dockerfile.test.dockerignore` is the Dockerfile-specific ignore file for that root context; it excludes repository metadata, generated output, unrelated local trees, and other cache-heavy artifacts while retaining the root Go source, root module files, and this module. This keeps cache invalidation useful without changing the module boundary.

## 5. Environment Architecture

```text
                         Compose network
 +----------------+      +--------+      +-------------------+
 | Go test runner |----->| PD     |----->| TiKV 1, 2, 3      |
 +----------------+      +--------+      +-------------------+
         |                                           |
         | POST /txn_chunk                           | DFS txn chunks
         v                                           v
 +----------------+      +--------+      +-------------------+
 | tikv-worker    |----->| MinIO  |<-----| TiKV DFS client   |
 +----------------+      +--------+      +-------------------+
```

All services, including the one-shot Go runner, are on the same private Compose network. PD advertises TiKV service addresses such as `tikv-1:20160`, so a host-side runner would need address rewriting and would no longer exercise the actual topology. No service publishes a host port. This prevents collisions, keeps the runner able to resolve advertised addresses, and makes the Docker network part of the test rather than an accidental test harness detail.

### 5.1 Exact source topology and images

Base the fixture on `cloud-storage-engine/scripts/docker-compose/docker-compose.yml` and its configs. At design time its exact image defaults are:

| Component | Image |
|---|---|
| PD and keyspace bootstrap | `hub-zot.pingcap.net/mirrors/tidbx/tikv/pd/image:release-nextgen-202603` |
| TiKV and `tikv-worker` | `${TIKV_COMPOSE_IMAGE:-hub-zot.pingcap.net/mirrors/tidbx/tikv/tikv/image:release-nextgen-202603}` |
| MinIO and bucket bootstrap | `hub.pingcap.net/test-infra/minio:latest` |
| Runner | `golang:1.25.10` |

`TIKV_COMPOSE_IMAGE` remains an explicit override for a compatible locally built CSE TiKV image. Updating these versions is a deliberate compatibility change: update them from the CSE source Compose file, not independently.

The fixture contains exactly MinIO, `minio-init`, PD, `create-keyspace`, `tikv-1`, `tikv-2`, `tikv-3`, `tikv-worker`, and the one-shot `test` service. It excludes TiDB and `copr-worker`.

Copy the CSE PD/TiKV/worker settings and remove only settings clearly limited to TiDB, coprocessor workers, backup, CDC, or unrelated workload tuning. In particular, preserve:

- PD client and peer listen/advertise addresses, its data path, and `/var/log/pd/pd.log`;
- TiKV `server.advertise-addr` values `tikv-{1,2,3}:20160`, API v2 storage, `pd.load-all-keyspaces-on-start = true`, each store's own data and raft paths, and `/var/log/tikv/tikv.log`;
- CSE DFS configuration in TiKV and worker: S3 backend, `cse` prefix, bucket `tidbcloud-local-dfs`, MinIO endpoint, credentials, region, and `DFS_ALLOW_FALLBACK_LOCAL=false`;
- the TiKV remote-compactor address expected by the CSE DFS configuration;
- `tikv-worker` address `0.0.0.0:19000`, PD endpoint `http://pd:2379`, `register = false`, DFS configuration, data path, and `/var/log/tikv-worker/tikv-worker.log`; and
- cleared `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` variants and `NO_PROXY`/`no_proxy` entries for every retained service name.

The runner receives `PD_ADDR=pd:2379`, `KEYSPACE_NAME=local_normal`, and `TXN_CHUNK_WRITER_ADDR=tikv-worker:19000`; missing or empty values fail setup immediately. The worker health check is `GET /healthz`; TiKV health is its local `/status`; MinIO health is `/minio/health/live`; and PD health is `pd-ctl member`.

### 5.2 MinIO and keyspace bootstrap

`init-minio.sh` waits for MinIO, locates `mc` or `mcli`, configures an alias using the source Compose endpoint and credentials, creates **only** `tidbcloud-local-dfs` with `mb --ignore-existing`, and verifies it with `mc stat`/`mcli stat`. The absence of `mc`/`mcli`, a failed create, or a failed verification is fatal. It must not fall back to `mkdir` on the MinIO data directory; that is not a verified S3 operation.

`create-keyspace.sh` is idempotent:

1. wait, with a bounded retry, for `pd-ctl -u http://pd:2379 member` and for `SYSTEM` to be visible;
2. create `local_normal` only if it is absent:

   ```text
   /pd-ctl -u http://pd:2379 keyspace create local_normal \
     --config gc_management_type=keyspace_level
   ```

3. query each keyspace as one object with the locally verified PD CLI form:

   ```text
   /pd-ctl -u http://pd:2379 keyspace show name local_normal
   /pd-ctl -u http://pd:2379 keyspace show name SYSTEM
   ```

4. parse or tightly validate the `local_normal` object itself for exact `name = local_normal`, `state = ENABLED`, and `config.gc_management_type = keyspace_level`; and
5. independently verify the `SYSTEM` object exists with exact name and `ENABLED` state.

The object-level checks avoid incorrectly accepting another keyspace's state or GC configuration. They are required because `NewCodecPDClientWithKeyspace` calls `GetKeyspaceMeta` and `apicodec.NewCodecV2`, but does not reject disabled metadata itself; `GetKeyspaceID` has a separate enabled-state check that this constructor does not use. The public client is created with `txnkv.NewClient`, `WithAPIVersion(kvrpcpb.APIVersion_V2)`, and `WithKeyspace("local_normal")` after bootstrap completes.

## 6. Test Process Design

### 6.1 Global configuration, clients, and goroutines

Every test setup calls `config.UpdateGlobal`, immediately registers its returned restore closure with `t.Cleanup`, and only then creates a client. Register the client close cleanup afterwards so Go's LIFO cleanup order closes the client before restoring global configuration. The configured values are exact:

```text
TiKVClient.TxnChunkWriterAddr        = "tikv-worker:19000"
TiKVClient.TxnFileMinMutationSize    = 1
TiKVClient.TxnChunkMaxSize           = 256
TiKVClient.TxnChunkWriterConcurrency = 2
```

Call `config.GetGlobalConfig().TiKVClient.Valid()` before creating the client; the fixture must reject zero chunk size or writer concurrency instead of silently constructing an invalid test. Do not mutate the configuration after client construction. Tests do not use `t.Parallel` because the global config and global Prometheus counters are shared.

`helpers_test.go` owns environment parsing, client creation, Region lookup, split/invalidate/wait helpers, direct `CmdGet`, exact counter reads, and txn-file-disabled setup/cleanup helpers. Each test creates and closes its own `txnkv.Client`; cleanup checks the close error. Any read transaction is rolled back after its value assertions. A transaction that was committed is already closed; callers do not roll it back again.

`txn_file/main_test.go` is deliberately small. It runs `goleak.VerifyTestMain` after per-test cleanup has closed all clients. Its option list contains exactly the existing repository ignore `goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start")`; do not add broad, mock-store, TTL-manager, or retry ignores. Do not enable or use failpoints in this module.

### 6.2 Key and Region helpers

All data keys, split keys, and Region-probe keys use one helper:

```go
func encodeKey(prefix, suffix string) []byte {
	return codec.EncodeBytes(nil, []byte(prefix+"_"+suffix))
}
```

This is the repository's TiDB-like mem-comparable representation from `integration_tests/util_test.go`, implemented by `util/codec.EncodeBytes`; it does not import TiDB. Each test prefix combines the test name with a UUID. The suffixes are fixed-width and ordered so the ordering used by the chunk planner, split points, and assertions is deterministic.

`KVStore.SplitRegions` does not invalidate an old cached Region after a successful split. The split helper therefore:

1. locates the pre-split Region containing the first split key;
2. calls `SplitRegions(ctx, splitKeys, false, nil)`;
3. explicitly calls `GetRegionCache().InvalidateCachedRegion(old.Region)`; and
4. uses bounded `require.Eventually` calls to `LocateKey` until the expected keys resolve to distinct Region IDs.

It never asks PD to scatter and never waits for scatter. Tests compare both Region IDs and ordering boundaries: keys expected in a lower Region must be in the same Region, and the later key must be in a different higher Region.

### 6.3 Metric discipline and cleanup order

Use `prometheus/testutil.ToFloat64` on `metrics.TxnFileRequestsOk` and `metrics.TxnFileRequestsError`. Snapshot the relevant global counter immediately before the target `Commit`, then assert an exact `+1` immediately after it returns. No setup, seed, competitor, read, probe, or cleanup transaction may intervene between the snapshot and assertion.

Run all value, Region, metric, and direct-lock assertions before test-specific cleanup. Cleanup then uses a new transaction with `DisableTxnFile()`, deletes only the test's known keys, commits, and closes the client. A cleanup failure is reported but does not replace the target assertion's failure. The Compose project is still destroyed with volumes, so cleanup is for test hygiene and diagnostics rather than a prerequisite for isolation.

## 7. End-to-End Test Cases

### 7.1 Successful multi-chunk, multi-Region commit

`TestTxnFileCommitAcrossChunksAndRegions` performs the following:

1. Generate unique, ordered mem-comparable data keys and two ordered split points between them. Locate the initial Region, split with `scatter=false`, invalidate that cached Region, and eventually prove that the data keys resolve to at least three distinct Region IDs.
2. Select values so every serialized put entry fits the fixture target:

   ```text
   entry = 2 + len(key) + 1 + 4 + len(value)
   entry + 4 <= 256
   ```

   Compute chunk count before beginning the target transaction using the production packing rule. For each ordered entry, flush the current payload before that entry only when:

   ```text
   payload is non-empty && len(payload) + entry + 4 > 256
   ```

   Append the entry, and count the final non-empty payload. Require the calculated count to be greater than one. This calculation intentionally treats `TxnChunkMaxSize` as the target described in section 2.3.
3. Begin an optimistic transaction, set all selected key/value pairs, snapshot `TxnFileRequestsOk` immediately before `Commit`, commit, and require that counter to be exactly the prior value plus one.
4. Begin a fresh read transaction, require every value to match exactly, then roll the read transaction back.
5. Only after those assertions, remove the test's values using a txn-file-disabled cleanup transaction.

The exact chunk calculation proves the fixture must produce multiple chunk payloads without production instrumentation. The counter proves client-go chose txn-file rather than normal 2PC. The real stack then validates worker v1 parsing and CRC checking, DFS persistence, API-v2 keyspace routing, txn-file prewrite/commit, and read visibility.

### 7.2 Determinate conflict after primary prewrite

`TestTxnFileWriteConflictRollsBack` verifies rollback of actual prewritten state rather than relying on normal reads that could resolve locks:

1. Generate ordered keys `a`, `b`, and `c` under one unique prefix. Seed their baseline values in a transaction that calls `DisableTxnFile()`.
2. Locate the Region containing the pre-split range, split immediately before `c` with `scatter=false`, explicitly invalidate the old Region, and wait until direct `LocateKey` calls prove `a` and `b` share a lower Region while `c` is in a distinct higher Region.
3. Begin optimistic transaction T1 and stage new values for `a`, `b`, and `c`. T1's start timestamp is now fixed.
4. Begin a separate T2, call `DisableTxnFile()`, change only `c`, and commit T2 after T1 started.
5. Immediately before T1's `Commit`, snapshot `metrics.TxnFileRequestsError`. Require `tikverr.IsErrWriteConflict(err)` and immediately require the error counter to have increased by exactly one.

Because mutation order is `a`, `b`, `c`, the eligible primary is in the lower Region. Actual txn-file execution prewrites that primary batch synchronously, then completes secondary prewrites; the later `c` batch encounters T2's newer write and returns the determinate write conflict. T1 cannot obtain a commit timestamp or commit a partial value, and client-go begins rollback with the primary synchronously and remaining Region batches asynchronously.

6. After failure, obtain a fresh read timestamp with `KVStore.CurrentTimestamp`. In a bounded `require.Eventually` loop, send a direct `tikvrpc.CmdGet` with that timestamp through `KVStore.SendReq` to the Region returned by `LocateKey` for each key. A response with a Region error is retried by re-locating; any non-lock key error fails the probe. The successful condition is:

   - `a` and `b` return their baseline values and `GetResponse.Error.Locked` is nil; and
   - `c` returns T2's value and `GetResponse.Error.Locked` is nil.

   Normal transactional reads are not an acceptable substitute because their lock resolver can resolve a leaked lock and hide the defect.
7. After the direct probes and all assertions, delete the three test keys through a txn-file-disabled cleanup transaction.

This test proves no committed partial values and no leaked locks for the keys that may have been prewritten. It does not inspect or assert removal of DFS chunk files.

## 8. Docker Lifecycle and Diagnostics

`integration_tests_in_docker/docker-compose/run.sh` is the only supported automated entry point. It uses `set -eu`, chooses a unique project name such as `client-go-txn-file-${USER}-${timestamp}-${random}` unless `COMPOSE_PROJECT_NAME` is explicitly supplied, and wraps every command as `docker compose --project-name "$project" --file "$compose_file"`.

The script first runs `compose config --quiet`. It installs its exit trap before starting any service. The trap always runs `compose down -v --remove-orphans`; it preserves the original test or startup status even if diagnostics or teardown fail.

Startup is dependency- and time-bounded, not sleep-based:

1. start PD and MinIO, then wait for their health checks;
2. run and require successful completion of `minio-init` and `create-keyspace`;
3. start the three TiKVs and `tikv-worker`, with Compose dependencies also requiring the successful bootstrap jobs;
4. wait with a bounded loop for each TiKV health check and `tikv-worker /healthz`; and
5. build and run the one-shot in-network test service, preserving its exit status exactly.

The script must not use whole-stack `up --abort-on-container-exit`: successful bootstrap jobs are expected to exit before the test runner. It can run the test service directly after readiness instead.

If startup or the test fails, diagnostics run before teardown and are best-effort only:

1. `compose ps`;
2. `compose logs` for all retained services, preserving container stdout/stderr; and
3. `compose exec -T` reads of the actual file logs: `/var/log/pd/pd.log`, `/var/log/tikv/tikv.log` from every TiKV, and `/var/log/tikv-worker/tikv-worker.log`.

Each diagnostic command is guarded so a missing container or log cannot overwrite the original status. On success, the trap still tears down volumes and orphans. `README.md` may document manual inspection, but automatic runs stay clean and leave no host ports.

## 9. Documentation and CI

`README.md` documents the topology, Docker/Compose and private-registry prerequisites, the one-command invocation, `TIKV_COMPOSE_IMAGE`, expected resource use, root Docker context/cache behavior, in-network address resolution, manual diagnostic commands, and common failures for image pulls, PD/keyspace bootstrap, MinIO/DFS, TiKV health, worker health, and advertised addresses.

`AGENTS.md` records the durable constraints: preserve CSE source compatibility, use API v2 and `local_normal`, keep TiDB and coprocessor workers out, use `codec.EncodeBytes` for data and split/probe keys, keep worker API v1 until client serialization changes, do not replace services with mocks, use unique prefixes and bounded waits, and collect file logs before teardown.

Add one CI job only on a private runner with Docker capacity and access to both CSE registry locations. The job runs:

```text
./integration_tests_in_docker/docker-compose/run.sh
```

Do not schedule it on a public runner until registry access is guaranteed. This initial real-stack job does not add race mode; existing client-go race jobs continue to cover in-process races.

## 10. Validation and Acceptance Criteria

Implementation is accepted only after all of the following are true:

- `docker compose -f integration_tests_in_docker/docker-compose/docker-compose.yml config --quiet` succeeds.
- From `integration_tests_in_docker`, the targeted command succeeds:

  ```text
  go test -v -count=1 -timeout=10m ./txn_file
  ```

- Changed Go code is formatted and statically checked with the module's formatting check, `go vet ./txn_file`, and applicable repository lint configuration. The initial design itself adds no runtime production instrumentation or code changes.
- `run.sh` starts real NextGen PD, three TiKVs, `tikv-worker`, and MinIO without TiDB or host port mappings, and the runner uses Compose-advertised addresses.
- Bootstrap creates and verifies only the required S3 bucket, and separately verifies exact enabled `SYSTEM` and `local_normal` keyspace objects, including `local_normal`'s GC configuration.
- The success test proves at least three Regions, computes more than one valid chunk using the exact 256-byte rule, commits, observes exactly one successful txn-file metric increment, and reads exact values in a rolled-back fresh transaction.
- The conflict test prewrites the lower primary Region before its higher-Region conflict, returns `IsErrWriteConflict`, observes exactly one txn-file error metric increment, preserves the baseline values of `a` and `b`, preserves T2's `c`, and reaches lock-free direct `CmdGet` results by its bounded deadline.
- Setup and cleanup transactions disable txn-file; target metric snapshots occur immediately before target commit and are checked immediately after it; no test uses `t.Parallel`, failpoints, mocks, or normal reads as lock-leak evidence.
- The required `goleak.VerifyTestMain` check passes with only `go.opencensus.io/stats/view.(*worker).start` ignored.
- A failing run prints Compose status, Compose stdout/stderr, and PD/TiKV/worker file logs before teardown without changing its failing status.
- Two consecutive clean `run.sh` executions pass. This demonstrates clean-volume teardown and idempotent bootstrap.
- The CI job is restricted to an eligible private runner with registry access.

## 11. Source References

- `txnkv/transaction/txn.go::KVTxn.Commit`, `KVTxn.DisableTxnFile`, and `KVTxn.Rollback` — commit dispatch and transaction-level disable behavior.
- `txnkv/transaction/2pc.go::twoPhaseCommitter.initKeysAndMutations` and `primary` — sorted mutation and primary selection behavior.
- `txnkv/transaction/txn_file.go::{useTxnFile, buildTxnFiles, executeTxnFile, executeTxnFileAction, newChunkWriterClient, chunkWriterClient.request}` — selection, packing, upload, primary-first execution, retries, metrics, and rollback/undetermined semantics.
- `kv/variables.go` and `config/client.go` — per-transaction and global txn-file controls plus configuration validation.
- `txnkv/client.go::NewClient`, `internal/locate/pd_codec.go::NewCodecPDClientWithKeyspace`, and `internal/apicodec/codec_v2.go::NewCodecV2` — public API-v2 client and keyspace codec construction.
- `tikv/split_region.go::KVStore.SplitRegions` and `internal/locate/region_cache.go::InvalidateCachedRegion` — split and explicit cache-invalidation requirements.
- `integration_tests/{txn_file_test.go,2pc_test.go,split_test.go,main_test.go,util_test.go}` — existing request-level, direct-lock-probe, split, TestMain, and mem-comparable-key patterns.
- `util/codec/bytes.go::EncodeBytes` — local mem-comparable key encoding.
- `cloud-storage-engine/scripts/docker-compose/{docker-compose.yml,configs/*.toml,bootstrap/create-keyspace.sh,bootstrap/init-minio.sh}` — exact image defaults, retained service configuration, DFS topology, and bootstrap starting point.
- `cloud-storage-engine/components/cloud_worker/src/{server.rs,txn_chunk.rs}` — worker health route, txn-chunk route, API-v1 default, CRC validation, v2 pessimistic-action byte, and DFS chunk creation.
- `pd/tools/pd-ctl/pdctl/command/keyspace_command.go` and `pd/tools/pd-ctl/tests/keyspace/keyspace_test.go` — verified `keyspace show name` command form and object-level metadata checks.
