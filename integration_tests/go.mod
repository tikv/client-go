module integration_tests

go 1.16

require (
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00
	github.com/pingcap/kvproto v0.0.0-20220304032058-ccd676426a27
	github.com/pingcap/tidb v1.1.0-beta.0.20220222031143-5988d0b2f46e
	github.com/pingcap/tidb/parser v0.0.0-20220222031143-5988d0b2f46e // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0
	github.com/tikv/pd/client v0.0.0-20220216080339-1b8f82378ee7
	go.uber.org/goleak v1.1.12
)

replace github.com/tikv/client-go/v2 => ../
