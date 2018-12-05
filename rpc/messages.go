package rpc

import (
	"reflect"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
)

func setContext(req interface{}, region *metapb.Region, peer *metapb.Peer) {
	ctx := kvrpcpb.Context{
		RegionId:    region.Id,
		RegionEpoch: region.RegionEpoch,
		Peer:        peer,
	}
	// Need generics in Go2.
	reflect.ValueOf(req).FieldByName("Context").SetPointer(unsafe.Pointer(&ctx))
}
