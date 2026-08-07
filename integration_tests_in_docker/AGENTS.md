# Txn-file Docker integration constraints

- Keep this fixture source-compatible with cloud-storage-engine's Compose
  fixture. Update PD/TiKV/worker image tags and CSE settings together from the
  CSE source; do not independently retag one service. Preserve verified
  tag-and-digest image references; update MinIO's pinned digest deliberately
  during a reviewed fixture update. Keep the source-compatible MinIO server
  image separate from the digest-pinned `quay.io/minio/mc` bootstrap client:
  the server image lacks `mc`/`mcli`. Preserve the CSE `x-service-limits` on
  PD, all TiKVs, and `tikv-worker` unless the source fixture changes.
- Use storage API v2 and the `local_normal` keyspace. Retain exactly the real
  MinIO, PD, three-TiKV, `tikv-worker`, bootstrap, and in-network test
  topology—no TiDB, `copr-worker`, mocks, proxies, or host-port mappings.
- Preserve the retained CSE TiKV compatibility settings in each store's
  `log`, `storage`, `kvengine`, `raftstore`, `readpool`, and `rfengine`
  sections. Continue excluding `[backup]`, `[cdc]`, `[coprocessor]`, and
  remote coprocessor addresses.
- Preserve the required NextGen lifecycle: PD+MinIO -> bucket -> TiKV
  cluster/`SYSTEM` -> `local_normal` -> `tikv-worker` -> tests. Never move
  keyspace bootstrap before the TiKV cluster bootstrap or worker startup
  before `local_normal`; the worker's initial keyspace/encryption metadata
  path must serve `/txn_chunk` for that keyspace.
- Encode every data, split, and direct-probe key with `codec.EncodeBytes`.
  Maintain ordered, UUID-qualified prefixes so test data is isolated.
- Keep the worker txn-chunk body API-v1 compatible until client-go's
  serialization changes: client-go omits `api_version`, and must not emit the
  API-v2 `pessimistic_action` byte prematurely.
- Use bounded readiness/Region/lock waits. After `SplitRegions`, explicitly
  invalidate the previously located Region-cache entry before polling for the
  new layout.
- Every non-target write transaction—including seed/setup, competing writes,
  and cleanup—must call `DisableTxnFile()`. Fresh success-path read
  transactions are rolled back, and conflict lock evidence uses direct
  `tikvrpc.CmdGet` rather than a transaction. Snapshot the relevant txn-file
  metric immediately before the target commit and assert its exact delta
  immediately afterwards; no unrelated transaction may intervene.
- Prove lock cleanup with direct `tikvrpc.CmdGet` evidence, not a normal
  transaction read whose lock resolver could mask a leaked lock.
- Preserve the unique Compose project and clean-volume teardown behavior.
  On failure, collect Compose status/stdout/stderr and PD, every TiKV, and
  worker file logs before teardown; diagnostics must not replace the original
  status.
- Keep the self-hosted Docker CI job limited to trusted `push` events. Never
  run it for `pull_request` or `pull_request_target` events.
- Do not add failpoints, `t.Parallel`, mocks, broad `goleak` ignores, or
  production instrumentation for this suite. Keep only the existing narrow
  OpenCensus goleak ignore.
