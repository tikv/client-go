# Repository Guidelines

## Project Structure & Module Organization

This repository is the Go module `github.com/tikv/client-go/v2`, a Go client library for TiKV and CSE. Public packages live at the repository root, including `tikv`, `rawkv`, `txnkv`, `kv`, `tikvrpc`, `oracle`, `config`, `metrics`, `trace`, and `error`. Implementation-only packages live under `internal/`, such as `internal/client`, `internal/locate`, `internal/apicodec`, `internal/kvrpc`, `internal/mockstore`, and `internal/unionstore`.

Tests are colocated with packages in `*_test.go` files. Broader integration coverage is under `integration_tests/`, with a separate `go.mod`; raw KV integration tests live in `integration_tests/raw/`. Runnable examples are under `examples/`, and several example directories are independent Go modules.

## Build, Test, and Development Commands

- `go test ./...`: run the main module unit tests locally.
- `go test --tags=intest ./...`: run the full unit-test set used by CI.
- `go test -race --tags=intest ./...`: run CI-equivalent race tests.
- `go test ./internal/apicodec`: run targeted API codec tests; use this after changing API v2 request or response key handling.
- `go generate ./...`: regenerate generated files, then check `git diff` for unexpected output.
- `golangci-lint run`: run repository linting and formatting checks from `.golangci.yml`.
- `cd integration_tests && gotestsum --format short-verbose -- ./...`: run local integration tests from the integration module.
- `cd integration_tests && go test --with-tikv`: run integration tests against local `pd-server` and `tikv-server` binaries after starting them as described in `README.md`.

## Coding Style & Naming Conventions

Use Go 1.25.x and standard Go formatting. Run `gofmt`/`goimports` through `golangci-lint`, or format touched files before submitting. Keep package names short, lowercase, and consistent with existing directories. Exported identifiers must have clear Go doc comments when they are part of the public client API.

Prefer existing error, retry, backoff, logging, metrics, and failpoint helpers instead of introducing parallel mechanisms. Keep public API changes small and deliberate because this module is consumed by multiple TiDB ecosystem components, including TiDB, TiCDC, BR, go-ycsb, and other projects. Design and review for compatibility and stability across these consumers, not only for TiDB.

## API v2 Codec Guidelines

All API v2 commands that carry keys must have complete and consistent encode/decode handling in `internal/apicodec`. This includes keys, key ranges, lock information, region errors, and any request or response fields that carry encoded keys. When adding or modifying a key-bearing RPC, update both request and response paths, add table-driven coverage in `internal/apicodec`, and verify that encode/decode behavior is symmetric. Missing or one-sided handling can create key-format mismatches and correctness bugs, especially across TiKV and CSE paths. When adding or modifying a `tikvrpc.Cmd*` whose request has a context field, update `tikvrpc/gen.sh`, regenerate `tikvrpc/cmds_generated.go`, and add `AttachContext` coverage.

## Testing Guidelines

Use Go's `testing` package with `testify/require`, `testify/assert`, and `testify/suite`, matching nearby tests. Name tests `TestXxx` and keep suite types in the existing lowercase style, such as `testRawkvSuite` or `testSnapshotSuite`.

Add or update package-level tests for every behavior change. Use `integration_tests/` when behavior depends on TiKV, PD, transactions, lock resolution, failpoints, or API-version behavior that mocks cannot cover. For concurrency-sensitive changes, run the race command before opening a PR.

CSE is only used behind TiDB. Transactional integration tests that run against next-gen/CSE should generate keys through a TiDB-like mem-comparable helper such as `encodeKey(...)`, not raw ad-hoc keys like `[]byte("k")`. For split/range/order-sensitive tests, update data keys, split keys, range bounds, and expected keys together.

## Commit & Pull Request Guidelines

Recent commits use concise, imperative subjects scoped by package or area, for example `apicodec: cover remaining API v2 key-bearing commands` or `txnkv: enable TiKV-side async resolve region lock for read path`. Follow that pattern and reference issues or PRs when relevant. This repository has DCO checks enabled; create commits with `git commit -s` so the commit message includes the required `Signed-off-by` trailer.

Pull requests should include a short problem statement, a summary of behavior changes, and the exact tests run. Link related issues and note compatibility impact for TiDB, TiCDC, BR, CSE, or other downstream users.

## Security & Configuration Tips

Do not commit local TiKV, PD, MinIO, or credential configuration. Keep generated logs, local data directories, and temporary integration-test artifacts out of version control. When testing against TiDB, prefer a local `go mod edit -replace=github.com/tikv/client-go/v2=/path/to/client-go` and avoid committing temporary replace directives unless the PR intentionally changes dependency wiring.

## Agent-Specific Instructions

Before editing, check the working tree and avoid modifying unrelated untracked or user-edited files. Keep documentation updates factual and repository-specific. When touching generated code, run `go generate ./...` and include generated diffs only when they are expected.
