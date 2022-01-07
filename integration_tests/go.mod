module integration_tests

go 1.16

require (
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20211224055123-d1a140660c39
	github.com/pingcap/tidb v1.1.0-beta.0.20211229035549-783432895924
	github.com/pingcap/tidb/parser v0.0.0-20211229035549-783432895924 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0
	github.com/tikv/pd v1.1.0-beta.0.20220106095237-876f7e908a17
	go.uber.org/goleak v1.1.12
)

replace github.com/tikv/client-go/v2 => ../

replace github.com/pingcap/tidb v1.1.0-beta.0.20211229035549-783432895924 => github.com/lemonhx/tidb v1.1.0-beta.0.20220105043412-73f7d78ce64d

replace github.com/pingcap/tidb/parser v0.0.0-20211229035549-783432895924 => github.com/lemonhx/tidb/parser v0.0.0-20220105043412-73f7d78ce64d // indirect

// cloud.google.com/go/storage will upgrade grpc to v1.40.0
// we need keep the replacement until go.etcd.io supports the higher version of grpc.
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
