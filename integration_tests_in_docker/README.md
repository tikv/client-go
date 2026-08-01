# Docker txn-file integration tests

This standalone Go module exercises client-go's txn-file path against real
NextGen PD, three TiKV stores, `tikv-worker`, and MinIO-backed DFS. SQL
behavior is out of scope.

## Topology and scope

The retained Compose services are exactly:

- `minio` and the one-shot `minio-init` bucket bootstrap;
- `pd` and the one-shot `create-keyspace` bootstrap;
- `tikv-1`, `tikv-2`, and `tikv-3`;
- `tikv-worker`; and
- the one-shot in-network `test` service.

TiKV runs with storage API v2 and the bootstrap creates the enabled
`local_normal` keyspace with `gc_management_type=keyspace_level`. The test
client uses that API-v2 keyspace. `tikv-worker` receives client-go's
`/txn_chunk` body without an `api_version`; that is the worker's API-v1 body
format. Do not select the API-v2 body format until client-go serialization is
changed accordingly.

The required NextGen lifecycle is PD+MinIO -> bucket -> TiKV cluster/`SYSTEM`
-> `local_normal` -> `tikv-worker` -> tests. `SYSTEM` becomes available only
after the TiKV cluster bootstraps. The worker starts after `local_normal` so
its initial keyspace/encryption metadata path can serve `/txn_chunk` for that
keyspace.

There is intentionally no TiDB, `copr-worker`, mock, proxy, or host-port
mapping. Every service and the Go runner share the private Compose network.
PD advertises `tikv-1:20160`, `tikv-2:20160`, and `tikv-3:20160`; these names
resolve from the `test` container, not from the host. A host-side `go test` is
therefore unsupported: it cannot faithfully use the Compose-advertised TiKV
addresses without rewriting the topology.

## Prerequisites

- Docker Engine, with permission to use its daemon, and Docker Compose **v2 or
  newer** (the modern `docker compose` plugin, not the legacy
  `docker-compose` executable).
- Image-pull access to both private registries:
  `hub-zot.pingcap.net/mirrors/tidbx/tikv` (PD and TiKV/worker) and
  `hub.pingcap.net/test-infra` (the MinIO server), plus pull access to
  `quay.io/minio` (the MinIO client). Authenticate with your normal registry
  workflow before running the suite.
- Practical capacity for PD, three TiKVs, a worker, MinIO, image builds, and
  their volumes. The CSE-derived per-service upper limits on PD, each TiKV,
  and `tikv-worker` (4 CPUs and 8 GB) are required for CSE worker-count
  adjustment compatibility on high-core hosts. They cap individual services;
  they do not reserve that maximum for every service. Plan for at least 4
  CPUs, 12 GiB RAM, and 30 GiB free Docker disk; more capacity reduces startup
  and build timeouts.

The Compose file contains the fixed local-only MinIO credentials
`minioadmin`/`minioadmin`; do not substitute or publish real credentials in
this fixture.

The verified source MinIO server image lacks `mc`/`mcli`. The strict S3 bucket
bootstrap therefore deliberately uses the digest-pinned `quay.io/minio/mc`
client image. This is the feasibility correction to the original CSE fallback,
not a topology change.

The named `minio-data` volume uses a 1 GiB tmpfs backing. The integration data
is small and intentionally ephemeral, and the bounded filesystem prevents
MinIO's percentage-based free-space guard from rejecting writes merely because
the host Docker filesystem is large and nearly full. The space is an upper
bound, not a reservation, and `down -v` still removes the named volume.

## Run

From the repository root, run the only supported automated entry point:

```sh
./integration_tests_in_docker/docker-compose/run.sh
```

The script verifies the rendered Compose configuration, starts dependencies in
readiness order, waits for the TiKV cluster before creating the keyspace, then
starts the worker before running the in-network test container and preserving
its exit status. It needs no host Go installation because the test image runs:

```text
go test -v -count=1 -timeout=10m ./txn_file
```

Use a compatible locally built CSE TiKV image, when needed, with:

```sh
TIKV_COMPOSE_IMAGE=your-compatible-tikv-image \
  ./integration_tests_in_docker/docker-compose/run.sh
```

`TIKV_COMPOSE_IMAGE` overrides TiKV and `tikv-worker` only; it must remain
compatible with the cloud-storage-engine fixture. Default image tags are
digest-pinned. Update PD, TiKV, and worker tag-and-digest pairs together from
the CSE source Compose configuration rather than changing one component
independently; retain MinIO's source tag and update its pinned digest
deliberately as part of a reviewed fixture update.

Unless supplied, `COMPOSE_PROJECT_NAME` is a unique name containing the user,
timestamp, and random suffix. Set a unique explicit name only when needed for
manual correlation:

```sh
COMPOSE_PROJECT_NAME=<unique-project> \
  ./integration_tests_in_docker/docker-compose/run.sh
```

The exit trap always runs `docker compose ... down -v --remove-orphans`, on
success and failure. This clean-volume teardown permits repeated runs; do not
expect retained containers or data volumes after the script exits.

### Build context and cache behavior

The test image has the repository root as its Docker build context, even
though its Dockerfile is under `integration_tests_in_docker/docker-compose`.
The module's `replace github.com/tikv/client-go/v2 => ../` requires the root
source tree. `Dockerfile.test` copies the root and nested-module
`go.mod`/`go.sum` files immediately before its single nested-module `go mod
download`, then copies source, so that layer stays cached until dependency
manifests change. The Dockerfile-specific ignore file keeps unrelated local,
generated, and common secret artifacts out of that root context.

## Diagnostics

On a startup or test failure, `run.sh` collects these diagnostics **before**
tearing volumes down, without replacing the original failure status:

1. `compose ps`;
2. `compose logs --no-color` for all retained services; and
3. the file logs `/var/log/pd/pd.log`, `/var/log/tikv/tikv.log` from every
   TiKV, and `/var/log/tikv-worker/tikv-worker.log`.

The script reads file logs through `compose exec` for active containers and
falls back to `docker cp` plus `tar` for stopped containers, so startup-crash
logs remain available before teardown.

For an active project, use the same project and Compose-file placeholders for
manual inspection:

```sh
PROJECT=<project-name>
COMPOSE_FILE=<path-to-repo>/integration_tests_in_docker/docker-compose/docker-compose.yml

docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE" ps
docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE" logs --no-color
docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE" exec -T pd sh -c 'cat /var/log/pd/pd.log'
docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE" exec -T tikv-1 sh -c 'cat /var/log/tikv/tikv.log'
docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE" exec -T tikv-2 sh -c 'cat /var/log/tikv/tikv.log'
docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE" exec -T tikv-3 sh -c 'cat /var/log/tikv/tikv.log'
docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE" exec -T tikv-worker sh -c 'cat /var/log/tikv-worker/tikv-worker.log'
```

The automatic cleanup normally makes these commands useful only while the
script is still running or when independently reproducing a failure with the
same project name.

## Common failures

| Symptom | Check / resolution |
| --- | --- |
| Image pull fails | Verify Docker Engine connectivity and authentication/authorization for both private registries and pull access to `quay.io/minio`, then pull the referenced images again. |
| `Docker Compose v2 or newer is required` | Install or select a modern `docker compose` plugin so `docker compose version --short` reports v2 or newer. |
| `create-keyspace` fails | This required NextGen lifecycle step runs after the TiKV cluster is healthy and before the worker starts, when `SYSTEM` is available. The image's `pd-ctl` may return zero for a missing keyspace, so bootstrap detects existence from the exact `name` field in its output. Inspect its Compose logs. Bootstrap strictly validates the individual `SYSTEM` and `local_normal` objects, including `ENABLED` state and `local_normal`'s `gc_management_type=keyspace_level`; a similarly named or unrelated keyspace is not accepted. |
| `minio-init` fails or DFS is unavailable | The MinIO server must become healthy, and the dedicated `quay.io/minio/mc` client must create and stat only `tidbcloud-local-dfs`. There is no directory-creation fallback. Check the bucket, endpoint, and logs; `DFS_ALLOW_FALLBACK_LOCAL=false` deliberately turns missing DFS into a failure. |
| A TiKV never becomes healthy | Inspect that store's `/status` health and `/var/log/tikv/tikv.log`; confirm all TiKV data/raft volumes have sufficient Docker disk and that bootstrap jobs completed. |
| `tikv-worker` never becomes healthy | Inspect `GET /healthz`, `/var/log/tikv-worker/tikv-worker.log`, worker DFS configuration, and its PD endpoint. |
| Client cannot reach a TiKV after connecting to PD | Run only through Compose. Verify the advertised service names and Compose DNS; do not run host-side tests or add host ports/address rewriting. |

The suite proves successful txn-file commits and determinate write-conflict
rollback behavior. It must not claim DFS orphan cleanup, recovery of an
undetermined commit, or deletion of partially accepted chunk objects: those
outcomes are outside this fixture's guarantees.

## CI

The integration workflow has one Docker job, `integration-txn-file-docker`.
It runs only for trusted `push` events—never `pull_request` code—when the
repository variable `TXN_FILE_DOCKER_RUNNER_LABEL` is non-empty and the
existing integration skip-label condition allows it. Set that variable to the
**extra self-hosted label** assigned to an eligible Linux Docker runner with
sufficient capacity and access to both private registries. The workflow
combines `self-hosted`, `linux`, and the variable value, so there is no
public-runner fallback or organization-specific label baked into the
repository. CI checks out the source and invokes only `run.sh`; Go setup is not
needed because the tests build inside Docker.
