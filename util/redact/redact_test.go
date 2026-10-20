package redact

import (
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
)

const (
	secretStart = "secret-start-key"
	secretEnd   = "secret-end-key"
)

func withRedactMode(t *testing.T, mode string) {
	old := errors.RedactLogEnabled.Load()
	errors.RedactLogEnabled.Store(mode)
	t.Cleanup(func() { errors.RedactLogEnabled.Store(old) })
}

func sampleRegion(id uint64) *metapb.Region {
	return &metapb.Region{
		Id:          id,
		StartKey:    []byte(secretStart),
		EndKey:      []byte(secretEnd),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 3, Version: 7},
		Peers:       []*metapb.Peer{{Id: 11, StoreId: 1}},
	}
}

func requireNoSecret(t *testing.T, s string) {
	require.NotContains(t, s, secretStart)
	require.NotContains(t, s, secretEnd)
	require.NotContains(t, s, "secret-key")
}

func TestRedactRegionMeta(t *testing.T) {
	region := sampleRegion(1)

	withRedactMode(t, errors.RedactLogDisable)
	require.Same(t, region, RegionMeta(region))
	require.Nil(t, RegionMeta(nil))

	for _, mode := range []string{errors.RedactLogEnable, errors.RedactLogMarker} {
		withRedactMode(t, mode)
		redacted := RegionMeta(region)
		require.NotSame(t, region, redacted)
		require.Equal(t, []byte("?"), redacted.StartKey)
		require.Equal(t, []byte("?"), redacted.EndKey)
		require.Equal(t, region.Id, redacted.Id)
		require.Equal(t, region.RegionEpoch, redacted.RegionEpoch)
		require.Equal(t, region.Peers, redacted.Peers)
		requireNoSecret(t, redacted.String())
		// The original region must be untouched.
		require.Equal(t, []byte(secretStart), region.StartKey)
		require.Equal(t, []byte(secretEnd), region.EndKey)
		// Empty keys mark unbounded ranges and are kept as is.
		require.Empty(t, RegionMeta(&metapb.Region{Id: 2}).StartKey)
		require.Nil(t, RegionMeta(nil))
	}
}

func TestRedactRegionMetas(t *testing.T) {
	regions := []*metapb.Region{sampleRegion(1), sampleRegion(2)}

	withRedactMode(t, errors.RedactLogDisable)
	require.Contains(t, fmt.Sprintf("%+v", RegionMetas(regions)), secretStart)

	withRedactMode(t, errors.RedactLogEnable)
	redacted := RegionMetas(regions)
	require.Len(t, redacted, 2)
	requireNoSecret(t, fmt.Sprintf("%+v", redacted))
	require.Equal(t, []byte(secretStart), regions[0].StartKey)
	require.Empty(t, RegionMetas(nil))
}

func TestRedactEpochNotMatch(t *testing.T) {
	e := &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{sampleRegion(1), sampleRegion(2)}}

	withRedactMode(t, errors.RedactLogDisable)
	require.Same(t, e, EpochNotMatch(e))
	require.Contains(t, EpochNotMatch(e).String(), secretStart)

	withRedactMode(t, errors.RedactLogEnable)
	redacted := EpochNotMatch(e)
	require.Len(t, redacted.CurrentRegions, 2)
	requireNoSecret(t, redacted.String())
	require.Contains(t, redacted.String(), "id:1")
	require.Equal(t, []byte(secretStart), e.CurrentRegions[0].StartKey)
	require.Nil(t, EpochNotMatch(nil))
}

func TestRedactRegionError(t *testing.T) {
	e := &errorpb.Error{
		Message: "region error",
		KeyNotInRegion: &errorpb.KeyNotInRegion{
			Key: []byte("secret-key"), RegionId: 1, StartKey: []byte(secretStart), EndKey: []byte(secretEnd),
		},
		EpochNotMatch:         &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{sampleRegion(1)}},
		BucketVersionNotMatch: &errorpb.BucketVersionNotMatch{Version: 5, Keys: [][]byte{[]byte(secretStart), {}}},
		NotLeader:             &errorpb.NotLeader{RegionId: 1},
	}

	withRedactMode(t, errors.RedactLogDisable)
	require.Same(t, e, RegionError(e))

	withRedactMode(t, errors.RedactLogEnable)
	redacted := RegionError(e)
	requireNoSecret(t, redacted.String())
	require.Equal(t, uint64(1), redacted.KeyNotInRegion.RegionId)
	require.Equal(t, uint64(5), redacted.BucketVersionNotMatch.Version)
	require.Equal(t, [][]byte{[]byte("?"), {}}, redacted.BucketVersionNotMatch.Keys)
	require.Same(t, e.NotLeader, redacted.NotLeader)
	require.Equal(t, "region error", redacted.Message)
	// The original error must be untouched.
	require.Equal(t, []byte("secret-key"), e.KeyNotInRegion.Key)
	require.Equal(t, []byte(secretStart), e.EpochNotMatch.CurrentRegions[0].StartKey)
	require.Equal(t, []byte(secretStart), e.BucketVersionNotMatch.Keys[0])
	require.Nil(t, RegionError(nil))
}

func TestRedactStringer(t *testing.T) {
	req := &kvrpcpb.GetRequest{Key: []byte("secret-key")}

	withRedactMode(t, errors.RedactLogDisable)
	require.Same(t, req, Stringer(req))

	withRedactMode(t, errors.RedactLogEnable)
	require.Equal(t, "?", Stringer(req).String())
}
