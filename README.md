# TiKV Go Client

TiKV Go Client provides support for interacting with the [TiKV](https://github.com/tikv/tikv) server in the form of a Go library.

![Stability Active](https://img.shields.io/badge/Stability-Active-yellow)
[![Go Reference](https://pkg.go.dev/badge/github.com/tikv/client-go/v2.svg)](https://pkg.go.dev/github.com/tikv/client-go/v2)

[![Unit Test](https://github.com/tikv/client-go/actions/workflows/test.yml/badge.svg)](https://github.com/tikv/client-go/actions/workflows/test.yml)
[![Integration Test](https://github.com/tikv/client-go/actions/workflows/integration.yml/badge.svg)](https://github.com/tikv/client-go/actions/workflows/integration.yml)
[![Compatibility Test](https://github.com/tikv/client-go/actions/workflows/compatibility.yml/badge.svg)](https://github.com/tikv/client-go/actions/workflows/compatibility.yml)

## Package versions

There are 2 major versions of the `client-go` package.

- `v2` is the latest stable version. This version was extracted from [pingcap/tidb](https://github.com/pingcap/tidb) and it includes new TiKV features like Follower Read, 1PC, Async Commit. The development of this version is on the `master` branch. The documentation for this version is below.

- `v1` is the previous stable version and is only maintained for bug fixes. You can read the documentation [here](https://tikv.org/docs/4.0/reference/clients/go/).

## Usage/Examples

```bash
  # valid versions be like @latest, @e5f932ae270887550b0cd221343dbd6b870b6c8f, @v2.0.0, @v2.0.1...
  go get github.com/tikv/client-go/v2@COMMIT_HASH_OR_TAG_VERSION
```

Explanation of related concepts and sample code can be found in the [Client-Go Wiki](https://github.com/tikv/client-go/wiki). There are also executable examples in [examples directory](https://github.com/tikv/client-go/tree/master/examples).

## Contributing to client-go

Pull Requests and issues are welcomed. Please check [CONTRIBUTING.md](./CONTRIBUTING.md).

## Developing

### Running Tests

Note: All the following tests are included in the [CI](https://github.com/tikv/client-go/actions) and you can submit a Pull Request directly to hand over the work.

To run unit tests, use following command

```bash
go test ./...
```

To run code linter, make sure `golangci-lint` is [installed](https://golangci-lint.run/usage/install/#local-installation). Then use following command

```bash
golangci-lint run
```

`integration_tests` can run against a real TiKV cluster. Here is an example:

```bash
./pd-server &
sleep 5
./tikv-server &
sleep 10
cd integration_tests
go test --with-tikv
```

### Test with TiDB

It is a common task to update client-go and then test it with TiDB.

If you only need to test locally, you can directly use the modified client-go on the same host by replacing:

```bash
go mod edit -replace=github.com/tikv/client-go/v2=/path/to/client-go
```

If you want to push your TiDB code to GitHub for running CI or for code review, you need to change the client-go used by TiDB to your developing branch using the following steps:

```bash
go get -d github.com/GITHUB_USERNAME/client-go/v2@DEV_BRANCH
# Output:
# go get: github.com/GITHUB_USERNAME/client-go/v2@none updating to
#         github.com/GITHUB_USERNAME/client-go/v2@v2.0.0-XXXXXXXXXXXXXX-XXXXXXXXXXXX: parsing go.mod:
#         module declares its path as: github.com/tikv/client-go/v2
#                 but was required as: github.com/GITHUB_USERNAME/client-go/v2
go mod edit -replace=github.com/tikv/client-go/v2=github.com/GITHUB_USERNAME/client-go/v2@v2.0.0-XXXXXXXXXXXXXX-XXXXXXXXXXXX
go mod download github.com/tikv/client-go/v2
```

## Used By

`client-go` is used by the following projects:

- [TiDB](https://github.com/pingcap/tidb): TiDB is an open source distributed HTAP database compatible with the MySQL protocol
- [BR](https://github.com/pingcap/br): A command-line tool for distributed backup and restoration of the TiDB cluster data
- [TiCDC](https://github.com/pingcap/ticdc): Change data capture for TiDB
- [go-ycsb](https://github.com/pingcap/go-ycsb): A Go port of Yahoo! Cloud Serving Benchmark (YCSB)
- [JuiceFS](https://github.com/juicedata/juicefs): JuiceFS is a distributed POSIX file system built on top of Redis and S3

## License

[Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)
