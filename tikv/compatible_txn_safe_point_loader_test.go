package tikv

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
)

func TestIsCESKeyspaceLevelGC(t *testing.T) {
	testCases := []struct {
		name string
		meta *keyspacepb.KeyspaceMeta
		want bool
	}{
		{name: "nil keyspace"},
		{name: "missing config", meta: &keyspacepb.KeyspaceMeta{}},
		{name: "CES keyspace-level GC", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"safe_point_version": "v2"}}, want: true},
		{name: "case-sensitive version", meta: &keyspacepb.KeyspaceMeta{Config: map[string]string{"safe_point_version": "V2"}}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsCESKeyspaceLevelGC(tc.meta))
		})
	}
}

func TestCompatibleTxnSafePointPath(t *testing.T) {
	testCases := []struct {
		name string
		meta *keyspacepb.KeyspaceMeta
		want string
	}{
		{
			name: "null keyspace",
			want: unifiedTxnSafePointPath,
		},
		{
			name: "native keyspace-level GC",
			meta: &keyspacepb.KeyspaceMeta{Id: 1, Config: map[string]string{"gc_management_type": "keyspace_level"}},
			want: "/keyspaces/tidb/1/tidb/store/gcworker/saved_safe_point",
		},
		{
			name: "CES keyspace-level GC",
			meta: &keyspacepb.KeyspaceMeta{Id: 2, Config: map[string]string{"safe_point_version": "v2"}},
			want: "/keyspaces/tidb/2/tidb/store/gcworker/saved_safe_point",
		},
		{
			name: "unified GC",
			meta: &keyspacepb.KeyspaceMeta{Id: 3, Config: map[string]string{"gc_management_type": "unified"}},
			want: unifiedTxnSafePointPath,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, compatibleTxnSafePointPath(tc.meta))
		})
	}
}
