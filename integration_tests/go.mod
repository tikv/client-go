module integration_tests

go 1.18

require (
	github.com/ninedraft/israce v0.0.3
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20220423142525-ae43b7f4e5c3
	github.com/pingcap/kvproto v0.0.0-20220705053936-aa9c2d20cd2a
	github.com/pingcap/tidb v1.1.0-beta.0.20220706093502-562b03368993
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.2-0.20220504104629-106ec21d14df
	github.com/tidwall/gjson v1.14.1
	github.com/tikv/client-go/v2 v2.0.1-0.20220627063500-947d923945fd
	github.com/tikv/pd/client v0.0.0-20220307081149-841fa61e9710
	go.uber.org/goleak v1.1.12
)

require (
	github.com/BurntSushi/toml v1.1.0 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/coocood/bbloom v0.0.0-20190830030839-58deb6228d64 // indirect
	github.com/coocood/freecache v1.2.1 // indirect
	github.com/coocood/rtutil v0.0.0-20190304133409-c84515f646f2 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/klauspost/compress v1.15.4 // indirect
	github.com/klauspost/cpuid v1.3.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/opentracing/basictracer-go v1.0.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pingcap/badger v1.5.1-0.20220314162537-ab58fbf40580 // indirect
	github.com/pingcap/goleveldb v0.0.0-20191226122134-f82aafb29989 // indirect
	github.com/pingcap/log v1.1.0 // indirect
	github.com/pingcap/tidb/parser v0.0.0-20220706093502-562b03368993 // indirect
	github.com/pingcap/tipb v0.0.0-20220706024432-7be3cc83a7d5 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.12.2 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/shirou/gopsutil/v3 v3.22.4 // indirect
	github.com/stathat/consistent v1.0.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/twmb/murmur3 v1.1.3 // indirect
	github.com/uber/jaeger-client-go v2.22.1+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/etcd/api/v3 v3.5.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.2 // indirect
	go.etcd.io/etcd/client/v3 v3.5.2 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/exp v0.0.0-20220428152302-39d4317da171 // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd // indirect
	golang.org/x/sync v0.0.0-20220513210516-0976fa681c29 // indirect
	golang.org/x/sys v0.0.0-20220622161953-175b2fd9d664 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20220224211638-0e9765cccd65 // indirect
	golang.org/x/tools v0.1.11 // indirect
	google.golang.org/genproto v0.0.0-20220216160803-4663080d8bc8 // indirect
	google.golang.org/grpc v1.44.0 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/tikv/client-go/v2 => ../

replace github.com/pingcap/tidb => github.com/glorv/tidb v1.1.0-beta.0.20220706085636-88fb3fa642bb
