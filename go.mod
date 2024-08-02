module github.com/tikv/client-go/v2

go 1.21

require (
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/docker/go-units v0.5.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.4
	github.com/google/btree v1.1.2
	github.com/google/uuid v1.6.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20220801062533-2eaa32854a6c
	github.com/pingcap/goleveldb v0.0.0-20191226122134-f82aafb29989
	github.com/pingcap/kvproto v0.0.0-20240620063548-118a4cab53e4
	github.com/pingcap/log v1.1.1-0.20221110025148-ca232912c9f3
	github.com/pkg/errors v0.9.1
	github.com/plar/go-adaptive-radix-tree v1.0.5
	github.com/prometheus/client_golang v1.18.0
	github.com/prometheus/client_model v0.5.0
	github.com/stretchr/testify v1.8.2
	github.com/tiancaiamao/gp v0.0.0-20221230034425-4025bc8a4d4a
	github.com/tikv/pd/client v0.0.0-20240620115049-049de1761e56
	github.com/twmb/murmur3 v1.1.3
	go.etcd.io/etcd/api/v3 v3.5.10
	go.etcd.io/etcd/client/v3 v3.5.10
	go.uber.org/atomic v1.11.0
	go.uber.org/goleak v1.2.0
	go.uber.org/zap v1.26.0
	golang.org/x/sync v0.6.0
	google.golang.org/grpc v1.63.2
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cloudfoundry/gosigar v1.3.6 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/onsi/gomega v1.20.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/common v0.46.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.10 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/exp v0.0.0-20230711005742-c3f37128e5a4 // indirect
	golang.org/x/net v0.24.0 // indirect
	golang.org/x/sys v0.19.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/tools v0.18.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240304212257-790db918fca8 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/plar/go-adaptive-radix-tree => github.com/you06/go-adaptive-radix-tree v0.0.0-20240523051018-0278e8bfcd2b
