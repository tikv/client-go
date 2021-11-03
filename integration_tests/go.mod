module integration_tests

go 1.16

require (
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20211011060348-d957056f1551
	github.com/pingcap/tidb v1.1.0-beta.0.20211031114450-f271148d792f
	github.com/pingcap/tidb/parser v0.0.0-20211031114450-f271148d792f // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0
	github.com/tikv/pd v1.1.0-beta.0.20211029083450-e65f0c55b6ae
	go.uber.org/goleak v1.1.11-0.20210813005559-691160354723
)

replace github.com/tikv/client-go/v2 => ../

// cloud.google.com/go/storage will upgrade grpc to v1.40.0
// we need keep the replacement until go.etcd.io supports the higher version of grpc.
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
