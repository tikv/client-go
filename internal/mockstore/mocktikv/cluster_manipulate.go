// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/mockstore/mocktikv/cluster_manipulate.go
//

// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mocktikv

import (
	"fmt"

	"github.com/pingcap/kvproto/pkg/metapb"
)

// BootstrapWithSingleStore initializes a Cluster with 1 Region and 1 Store.
func BootstrapWithSingleStore(cluster *Cluster) (storeID, peerID, regionID uint64) {
	ids := cluster.AllocIDs(3)
	storeID, peerID, regionID = ids[0], ids[1], ids[2]
	cluster.AddStore(storeID, fmt.Sprintf("store%d", storeID))
	cluster.Bootstrap(regionID, []uint64{storeID}, []uint64{peerID}, peerID)
	return
}

// BootstrapWithMultiStores initializes a Cluster with 1 Region and n Stores.
func BootstrapWithMultiStores(cluster *Cluster, n int) (storeIDs, peerIDs []uint64, regionID uint64, leaderPeer uint64) {
	storeIDs = cluster.AllocIDs(n)
	peerIDs = cluster.AllocIDs(n)
	leaderPeer = peerIDs[0]
	regionID = cluster.AllocID()
	for _, storeID := range storeIDs {
		labels := []*metapb.StoreLabel{
			{
				Key:   "id",
				Value: fmt.Sprintf("%v", storeID),
			},
		}
		cluster.AddStore(storeID, fmt.Sprintf("store%d", storeID), labels...)
	}
	cluster.Bootstrap(regionID, storeIDs, peerIDs, leaderPeer)
	return
}

// BootstrapWithMultiRegions initializes a Cluster with multiple Regions and 1
// Store. The number of Regions will be len(splitKeys) + 1.
func BootstrapWithMultiRegions(cluster *Cluster, splitKeys ...[]byte) (storeID uint64, regionIDs, peerIDs []uint64) {
	var firstRegionID, firstPeerID uint64
	storeID, firstPeerID, firstRegionID = BootstrapWithSingleStore(cluster)
	regionIDs = append([]uint64{firstRegionID}, cluster.AllocIDs(len(splitKeys))...)
	peerIDs = append([]uint64{firstPeerID}, cluster.AllocIDs(len(splitKeys))...)
	for i, k := range splitKeys {
		cluster.Split(regionIDs[i], regionIDs[i+1], k, []uint64{peerIDs[i]}, peerIDs[i])
	}
	return
}

// BootstrapWithMultiZones initializes a Cluster with 1 Region and n Zones and m Stores in each Zone.
func BootstrapWithMultiZones(cluster *Cluster, n, m int) (storeIDs, peerIDs []uint64, regionID uint64, leaderPeer uint64, store2zone map[uint64]string) {
	storeIDs = cluster.AllocIDs(n * m)
	peerIDs = cluster.AllocIDs(n)
	leaderPeer = peerIDs[0]
	regionID = cluster.AllocID()
	store2zone = make(map[uint64]string, n*m)
	for id, storeID := range storeIDs {
		zone := fmt.Sprintf("z%d", (id%n)+1)
		store2zone[storeID] = zone
		labels := []*metapb.StoreLabel{
			{
				Key:   "id",
				Value: fmt.Sprintf("%v", storeID),
			},
			{
				Key:   "zone",
				Value: zone,
			},
		}
		cluster.AddStore(storeID, fmt.Sprintf("store%d", storeID), labels...)
	}
	cluster.Bootstrap(regionID, storeIDs[:n], peerIDs, leaderPeer)
	return
}
