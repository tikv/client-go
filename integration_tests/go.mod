module integration_tests

go 1.16

require (
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20210611081648-a215b4e61d2f
	github.com/pingcap/parser v0.0.0-20210610080504-cb77169bfed9
	github.com/pingcap/tidb v1.1.0-beta.0.20210616023036-9461f5ba55b1
	github.com/tikv/client-go/v2 v2.0.0
	github.com/tikv/pd v1.1.0-beta.0.20210323121136-78679e5e209d
	go.uber.org/zap v1.17.0
)

replace github.com/tikv/client-go/v2 => ../
