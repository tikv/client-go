package tikv

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/stretchr/testify/require"
)

func TestCompatibleTxnSafePointPath(t *testing.T) {
	tests := []struct {
		name string
		meta *keyspacepb.KeyspaceMeta
		want string
	}{
		{name: "null keyspace", meta: nil, want: unifiedTxnSafePointPath},
		{
			name: "native keyspace-level GC",
			meta: &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1}, Config: map[string]string{"gc_management_type": "keyspace_level"}},
			want: "/keyspaces/tidb/1/tidb/store/gcworker/saved_safe_point",
		},
		{
			name: "CES keyspace-level GC",
			meta: &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 2}, Config: map[string]string{"safe_point_version": "v2"}},
			want: "/keyspaces/tidb/2/tidb/store/gcworker/saved_safe_point",
		},
		{
			name: "unified GC",
			meta: &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 3}, Config: map[string]string{"gc_management_type": "unified"}},
			want: unifiedTxnSafePointPath,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given
			meta := test.meta

			// When
			got := compatibleTxnSafePointPath(meta)

			// Then
			require.Equal(t, test.want, got)
		})
	}
}
