module integration_tests

go 1.16

require (
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20211109071446-a8b4d34474bc
	github.com/pingcap/tidb v1.1.0-beta.0.20211118075747-4fad89c46b3b
	github.com/pingcap/tidb/parser v0.0.0-20211118075747-4fad89c46b3b // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0
	github.com/tikv/pd v1.1.0-beta.0.20211118054146-02848d2660ee
	go.uber.org/goleak v1.1.12
)

replace github.com/tikv/client-go/v2 => ../

// cloud.google.com/go/storage will upgrade grpc to v1.40.0
// we need keep the replacement until go.etcd.io supports the higher version of grpc.
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

replace github.com/pingcap/kvproto => github.com/youjiali1995/kvproto v0.0.0-20211117113944-498284457dde

replace github.com/pingcap/tidb => github.com/youjiali1995/tidb v1.1.0-beta.0.20211118073243-bdfb8ddc8e42
