module integration_tests

go 1.16

require (
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00
	github.com/pingcap/kvproto v0.0.0-20220215045702-d229fcc888c8
	github.com/pingcap/tidb v0.0.0
	github.com/pingcap/tidb/parser v0.0.0-20220209083136-a850b044c134 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0
	github.com/tikv/pd/client v0.0.0-20220216080339-1b8f82378ee7
	go.uber.org/goleak v1.1.12
)

replace github.com/tikv/client-go/v2 => ../

replace github.com/pingcap/tidb => github.com/oh-my-tidb/tidb v1.1.0-beta.0.20220217132606-8ac6b60022d5

replace github.com/pingcap/tidb-tools => github.com/oh-my-tidb/tidb-tools v5.2.2-0.20220217062816-d70a73fd0527+incompatible
