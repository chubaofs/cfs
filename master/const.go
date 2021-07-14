// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
)

// Keys in the request
const (
	addrKey                 = "addr"
	diskPathKey             = "disk"
	nameKey                 = "name"
	idKey                   = "id"
	countKey                = "count"
	startKey                = "start"
	enableKey               = "enable"
	thresholdKey            = "threshold"
	dataPartitionSizeKey    = "size"
	metaPartitionCountKey   = "mpCount"
	volCapacityKey          = "capacity"
	volOwnerKey             = "owner"
	volAuthKey              = "authKey"
	replicaNumKey           = "replicaNum"
	followerReadKey         = "followerRead"
	authenticateKey         = "authenticate"
	akKey                   = "ak"
	keywordsKey             = "keywords"
	zoneNameKey             = "zoneName"
	crossZoneKey            = "crossZone"
	defaultPriority         = "defaultPriority"
	userKey                 = "user"
	nodeHostsKey            = "hosts"
	nodeDeleteBatchCountKey = "batchCount"
	nodeMarkDeleteRateKey   = "markDeleteRate"
	nodeDeleteWorkerSleepMs = "deleteWorkerSleepMs"
	nodeAutoRepairRateKey   = "autoRepairRate"
	descriptionKey          = "description"
	dpSelectorNameKey       = "dpSelectorName"
	dpSelectorParmKey       = "dpSelectorParm"
	nodeTypeKey             = "nodeType"
	ratio                   = "ratio"
)

const (
	deleteIllegalReplicaErr       = "deleteIllegalReplicaErr "
	addMissingReplicaErr          = "addMissingReplicaErr "
	checkDataPartitionDiskErr     = "checkDataPartitionDiskErr  "
	dataNodeOfflineErr            = "dataNodeOfflineErr "
	diskOfflineErr                = "diskOfflineErr "
	handleDataPartitionOfflineErr = "handleDataPartitionOffLineErr "
)

const (
	underlineSeparator = "_"
)

const (
	LRUCacheSize    = 3 << 30
	WriteBufferSize = 4 * util.MB
)

const (
	defaultFaultDomainZoneCnt                    = 3
	defaultInitMetaPartitionCount                = 3
	defaultMaxInitMetaPartitionCount             = 100
	defaultMaxMetaPartitionInodeID        uint64 = 1<<63 - 1
	defaultMetaPartitionInodeIDStep       uint64 = 1 << 24
	defaultMetaNodeReservedMem            uint64 = 1 << 30
	runtimeStackBufSize                          = 4096
	spaceAvailableRate                           = 0.90
	defaultNodeSetCapacity                       = 18
	minNumOfRWDataPartitions                     = 10
	intervalToCheckMissingReplica                = 600
	intervalToWarnDataPartition                  = 600
	intervalToLoadDataPartition                  = 12 * 60 * 60
	defaultInitDataPartitionCnt                  = 10
	volExpansionRatio                            = 0.1
	maxNumberOfDataPartitionsForExpansion        = 100
	EmptyCrcValue                         uint32 = 4045511210
	DefaultZoneName                              = proto.DefaultZoneName
	retrySendSyncTaskInternal                    = 3 * time.Second
	defaultRangeOfCountDifferencesAllowed        = 50
	defaultMinusOfMaxInodeID                     = 1000
	defaultNodeSetGrpBatchCnt                    = 3
)

const (
	normal          uint8 = 0
	markDelete      uint8 = 1
	normalZone            = 0
	unavailableZone       = 1
	unavaliable           = 1
	metaNodesUnavaliable   = 2
	dataNodesUnavaliable   = 3
)



const (
	opSyncAddMetaNode          uint32 = 0x01
	opSyncAddDataNode          uint32 = 0x02
	opSyncAddDataPartition     uint32 = 0x03
	opSyncAddVol               uint32 = 0x04
	opSyncAddMetaPartition     uint32 = 0x05
	opSyncUpdateDataPartition  uint32 = 0x06
	opSyncUpdateMetaPartition  uint32 = 0x07
	opSyncDeleteDataNode       uint32 = 0x08
	opSyncDeleteMetaNode       uint32 = 0x09
	opSyncAllocDataPartitionID uint32 = 0x0A
	opSyncAllocMetaPartitionID uint32 = 0x0B
	opSyncAllocCommonID        uint32 = 0x0C
	opSyncPutCluster           uint32 = 0x0D
	opSyncUpdateVol            uint32 = 0x0E
	opSyncDeleteVol            uint32 = 0x0F
	opSyncDeleteDataPartition  uint32 = 0x10
	opSyncDeleteMetaPartition  uint32 = 0x11
	opSyncAddNodeSet           uint32 = 0x12
	opSyncUpdateNodeSet        uint32 = 0x13
	opSyncBatchPut             uint32 = 0x14
	opSyncUpdateDataNode       uint32 = 0x15
	opSyncUpdateMetaNode       uint32 = 0x16
	opSyncAddUserInfo          uint32 = 0x17
	opSyncDeleteUserInfo       uint32 = 0x18
	opSyncUpdateUserInfo       uint32 = 0x19
	opSyncAddAKUser            uint32 = 0x1A
	opSyncDeleteAKUser         uint32 = 0x1B
	opSyncAddVolUser           uint32 = 0x1C
	opSyncDeleteVolUser        uint32 = 0x1D
	opSyncUpdateVolUser        uint32 = 0x1E
	opSyncNodeSetGrp           uint32 = 0x1F
	opSyncExclueDomain         uint32 = 0x23
)

const (
	keySeparator          = "#"
	idSeparator           = "$" // To seperate ID of server that submits raft changes
	metaNodeAcronym       = "mn"
	dataNodeAcronym       = "dn"
	dataPartitionAcronym  = "dp"
	metaPartitionAcronym  = "mp"
	volAcronym            = "vol"
	clusterAcronym        = "c"
	nodeSetAcronym        = "s"
	nodeSetGrpAcronym     = "g"
	domainAcronym         = "zoneDomain"
	maxDataPartitionIDKey = keySeparator + "max_dp_id"
	maxMetaPartitionIDKey = keySeparator + "max_mp_id"
	maxCommonIDKey        = keySeparator + "max_common_id"
	metaNodePrefix        = keySeparator + metaNodeAcronym + keySeparator
	dataNodePrefix        = keySeparator + dataNodeAcronym + keySeparator
	dataPartitionPrefix   = keySeparator + dataPartitionAcronym + keySeparator
	volPrefix             = keySeparator + volAcronym + keySeparator
	metaPartitionPrefix   = keySeparator + metaPartitionAcronym + keySeparator
	clusterPrefix         = keySeparator + clusterAcronym + keySeparator
	nodeSetPrefix         = keySeparator + nodeSetAcronym + keySeparator
	nodeSetGrpPrefix      = keySeparator + nodeSetGrpAcronym + keySeparator
	DomainPrefix          = keySeparator + domainAcronym + keySeparator
	akAcronym             = "ak"
	userAcronym           = "user"
	volUserAcronym        = "voluser"
	akPrefix              = keySeparator + akAcronym + keySeparator
	userPrefix            = keySeparator + userAcronym + keySeparator
	volUserPrefix         = keySeparator + volUserAcronym + keySeparator
)
