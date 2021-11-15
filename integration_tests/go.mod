module integration_tests

go 1.16

require (
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20210806074406-317f69fb54b4
	github.com/pingcap/parser v0.0.0-20210823071803-562fed23b4fb
	github.com/pingcap/tidb v1.1.0-beta.0.20210729073017-a27d306e65a0
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0
	github.com/tikv/pd v1.1.0-beta.0.20210818082359-acba1da0018d
	go.uber.org/goleak v1.1.10
)

replace github.com/pingcap/kvproto => github.com/MyonKeminta/kvproto v0.0.0-20211021102703-60266c23d5bd

replace github.com/pingcap/tidb => github.com/MyonKeminta/tidb v1.1.0-alpha.1.0.20211115083625-294c970209db

replace github.com/tikv/client-go/v2 => ../
