package txnprotocol

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestDeclarationContextPreservesExplicitLegacyVersion(t *testing.T) {
	declaration := Declaration{}
	ctx := WithDeclaration(context.Background(), declaration)
	got, ok := DeclarationFrom(ctx)
	require.True(t, ok)
	require.Equal(t, declaration, got)

	_, ok = DeclarationFrom(context.Background())
	require.False(t, ok)
}

func useDefaultVersion(t *testing.T, version kvrpcpb.TxnProtocolVersion) {
	t.Helper()
	previous := tikvrpc.GetDefaultTxnProtocolVersion()
	require.NoError(t, tikvrpc.SetDefaultTxnProtocolVersion(version))
	t.Cleanup(func() { require.NoError(t, tikvrpc.SetDefaultTxnProtocolVersion(previous)) })
}

func TestRequiredTxnProtocolVersionFollowsPayload(t *testing.T) {
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put}},
	})
	required, protected := requiredTxnProtocolVersion(req)
	require.True(t, protected)
	require.Zero(t, required)

	req.Prewrite().Mutations = append(req.Prewrite().Mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Op_SharedLock})
	required, protected = requiredTxnProtocolVersion(req)
	require.True(t, protected)
	require.Equal(t, uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK), required)
}

func TestPrepareSelectsAndFailsClosed(t *testing.T) {
	useDefaultVersion(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	ordinary := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
	selection, err := Prepare(ordinary, 1, StoreRange{Present: true, Min: 0, Max: 1})
	require.NoError(t, err)
	require.Equal(t, uint32(1), selection.Selected)
	require.False(t, selection.NeedStoreReload())

	shared := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_SharedLock}},
	})
	selection, err = Prepare(shared, 7, StoreRange{Present: true, Min: 0, Max: 1})
	require.Error(t, err)
	require.True(t, selection.NeedStoreReload())
	var incompatible interface {
		GetReason() errorpb.IncompatibleRequestReason
	}
	require.ErrorAs(t, err, &incompatible)
	require.Equal(t, errorpb.IncompatibleRequestReason_IncompatibleRequestReasonUnknown, incompatible.GetReason())
}

func TestVersionForTransportValidatesCurrentPayload(t *testing.T) {
	useDefaultVersion(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put}},
	})

	ctx := WithDeclaration(context.Background(), Declaration{Version: 1})
	version, err := VersionForTransport(ctx, req)
	require.NoError(t, err)
	require.Equal(t, uint32(1), version)

	req.Prewrite().Mutations = append(req.Prewrite().Mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Op_SharedLock})
	_, err = VersionForTransport(ctx, req)
	require.Error(t, err)

	_, err = VersionForTransport(context.Background(), req)
	require.Error(t, err)
}
