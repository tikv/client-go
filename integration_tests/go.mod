module integration_tests

go 1.16

require (
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20211122024046-03abd340988f
	github.com/pingcap/tidb v1.1.0-beta.0.20211130051352-37e0dac25981
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0
	github.com/tikv/pd v1.1.0-beta.0.20211118054146-02848d2660ee
	go.uber.org/goleak v1.1.12
)

replace github.com/pingcap/kvproto => github.com/MyonKeminta/kvproto v0.0.0-20211021102703-60266c23d5bd

replace github.com/pingcap/tidb => github.com/MyonKeminta/tidb v1.1.0-alpha.1.0.20211115083625-294c970209db

replace github.com/tikv/client-go/v2 => ../

// cloud.google.com/go/storage will upgrade grpc to v1.40.0
// we need keep the replacement until go.etcd.io supports the higher version of grpc.
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
