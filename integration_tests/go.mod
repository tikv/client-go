module integration_tests

go 1.16

require (
	github.com/klauspost/compress v1.15.4 // indirect
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20220423142525-ae43b7f4e5c3
	github.com/pingcap/kvproto v0.0.0-20220705053936-aa9c2d20cd2a
	github.com/pingcap/tidb v1.1.0-beta.0.20220706093502-562b03368993
	github.com/pingcap/tidb/parser v0.0.0-20220706093502-562b03368993 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.2-0.20220504104629-106ec21d14df
	github.com/tidwall/gjson v1.14.1
	github.com/tikv/client-go/v2 v2.0.1-0.20220627063500-947d923945fd
	github.com/tikv/pd/client v0.0.0-20220307081149-841fa61e9710
	go.uber.org/goleak v1.1.12
)

replace github.com/tikv/client-go/v2 => ../

replace github.com/pingcap/tidb => github.com/glorv/tidb v1.1.0-beta.0.20220706085636-88fb3fa642bb
