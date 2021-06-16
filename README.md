# TiKV Go Client

TiKV Go Client provides support for interacting with the [TiKV](https://github.com/tikv/tikv) server in the form of a Go library.

## Package versions

There are 2 major versions of the `client-go` package.

- `v2` is the new stable and active version. This version was extracted from [pingcap/tidb](https://github.com/pingcap/tidb) and it includes new TiKV features like Follower Read, 1PC, Async Commit. The development of this version is on the `v2` branch. The documentation for this version is below.

- `v1` is the previous stable version and is only maintained for bug fixes. You can read the documentation [here](https://tikv.org/docs/4.0/reference/clients/go/).

## Usage/Examples

```bash
  go get github.com/tikv/client-go/v2@COMMIT_HASH_OR_TAG_VERSION
```

## Running Tests

To run tests, run the following command

```bash
  go test ./...
```

## Used By

This project is used by the following projects:

- TiDB (https://github.com/pingcap/tidb)


## License

[Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)
