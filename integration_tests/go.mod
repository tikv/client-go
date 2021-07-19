module integration_tests

go 1.16

require (
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20210712050333-b66fdbd6bfd5
	github.com/pingcap/parser v0.0.0-20210707071004-31c87e37af5c
	github.com/pingcap/tidb v1.1.0-beta.0.20210713072132-d696ce33a79e
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0
	github.com/tikv/pd v1.1.0-beta.0.20210323121136-78679e5e209d
	go.uber.org/goleak v1.1.10
)

replace github.com/tikv/client-go/v2 => ../
