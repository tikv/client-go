# TiKV Go Client

TiKV Go Client provides support for interacting with the [TiKV](https://github.com/tikv/tikv) server in the form of a Go library.

![Stability Active](https://img.shields.io/badge/Stability-Active-yellow)
[![Go Reference](https://pkg.go.dev/badge/github.com/tikv/client-go/v2.svg)](https://pkg.go.dev/github.com/tikv/client-go/v2)
![Test Status](https://github.com/tikv/client-go/actions/workflows/test.yml/badge.svg?branch=v2)


## Package versions

There are 2 major versions of the `client-go` package.

- `v2` is the new stable and active version. This version was extracted from [pingcap/tidb](https://github.com/pingcap/tidb) and it includes new TiKV features like Follower Read, 1PC, Async Commit. The development of this version is on the `v2` branch. The documentation for this version is below.

- `v1` is the previous stable version and is only maintained for bug fixes. You can read the documentation [here](https://tikv.org/docs/4.0/reference/clients/go/).

## Usage/Examples

```bash
  go get github.com/tikv/client-go/v2@COMMIT_HASH_OR_TAG_VERSION
```

## Contributing to client-go

Pull Requests and issues are welcomed. Please check [CONTRIBUTING.md](./CONTRIBUTING.md).

## Developing

### Running Tests

Note: All the following tests are included in the [CI](https://github.com/tikv/client-go/actions) and you can submit a Pull Request directly to hand over the work.

To run unit tests, use following command

```bash
go test -v ./...
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
go test -v --with-tikv=true --pd-addrs=127.0.0.1:2379
```

## Contributing to client-go

Pull Requests and issues are welcomed. Please check [CONTRIBUTING.md](./CONTRIBUTING.md).

## Used By

This project is used by the following projects:

- TiDB: TiDB is an open source distributed HTAP database compatible with the MySQL protocol (https://github.com/pingcap/tidb)
- BR: A command-line tool for distributed backup and restoration of the TiDB cluster data (https://github.com/pingcap/br)

## License

[Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)
