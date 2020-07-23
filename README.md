# TiKV Go Client

TiKV Go Client provides support for interacting with TiKV server in the form of a Go library.

Its main code and structure are stripped from the [pingcap/tidb](https://github.com/pingcap/tidb) repository. The main reason for extracting this repo is to provide a cleaner option without direct accessing to `github.com/pingcap/tidb/store/tikv` and introduce a lot of unnecessary dependencies.

There are examples of how to use them in the `example/` directory. Please note that it is **not recommended or supported** to use both the raw and transactional APIs on the same keyspace.
