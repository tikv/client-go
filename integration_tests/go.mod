module integration_tests

go 1.16

require (
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00
	github.com/pingcap/kvproto v0.0.0-20211224055123-d1a140660c39
	github.com/pingcap/tidb v1.1.0-beta.0.20220208061135-294a094d9055
	github.com/pingcap/tidb/parser v0.0.0-20220208061135-294a094d9055 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0
	github.com/tikv/pd v1.1.0-beta.0.20220207063535-9268bed87199
	go.uber.org/goleak v1.1.12
)

replace github.com/pingcap/tidb => github.com/MyonKeminta/tidb v1.1.0-alpha.1.0.20220209062409-25dd32dba9ec

replace github.com/pingcap/tidb/parser => github.com/MyonKeminta/tidb/parser v0.0.0-20220209062409-25dd32dba9ec

replace github.com/tikv/client-go/v2 => ../

// cloud.google.com/go/storage will upgrade grpc to v1.40.0
// we need keep the replacement until go.etcd.io supports the higher version of grpc.
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
