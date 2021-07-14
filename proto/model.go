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

package proto

import (
	"time"
)

const (
	DefaultZoneName = "default"
)

// MetaNode defines the structure of a meta node
type MetaNodeInfo struct {
	ID                        uint64
	Addr                      string
	IsActive                  bool
	IsWriteAble               bool
	ZoneName                  string `json:"Zone"`
	MaxMemAvailWeight         uint64 `json:"MaxMemAvailWeight"`
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	Ratio                     float64
	SelectCount               uint64
	Carry                     float64
	Threshold                 float32
	ReportTime                time.Time
	MetaPartitionCount        int
	NodeSetID                 uint64
	PersistenceMetaPartitions []uint64
}

// DataNode stores all the information about a data node
type DataNodeInfo struct {
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	AvailableSpace            uint64
	ID                        uint64
	ZoneName                  string `json:"Zone"`
	Addr                      string
	ReportTime                time.Time
	IsActive                  bool
	IsWriteAble               bool
	UsageRatio                float64 // used / total space
	SelectedTimes             uint64  // number times that this datanode has been selected as the location for a data partition.
	Carry                     float64 // carry is a factor used in cacluate the node's weight
	DataPartitionReports      []*PartitionReport
	DataPartitionCount        uint32
	NodeSetID                 uint64
	PersistenceDataPartitions []uint64
	BadDisks                  []string
}

// MetaPartition defines the structure of a meta partition
type MetaPartitionInfo struct {
	PartitionID   uint64
	Start         uint64
	End           uint64
	MaxInodeID    uint64
	InodeCount    uint64
	DentryCount   uint64
	VolName       string
	Replicas      []*MetaReplicaInfo
	ReplicaNum    uint8
	Status        int8
	IsRecover     bool
	Hosts         []string
	Peers         []Peer
	Zones         []string
	OfflinePeerID uint64
	MissNodes     map[string]int64
	LoadResponse  []*MetaPartitionLoadResponse
}

// MetaReplica defines the replica of a meta partition
type MetaReplicaInfo struct {
	Addr       string
	ReportTime int64
	Status     int8 // unavailable, readOnly, readWrite
	IsLeader   bool
}

// ClusterView provides the view of a cluster.
type ClusterView struct {
	Name                string
	LeaderAddr          string
	DisableAutoAlloc    bool
	MetaNodeThreshold   float32
	Applied             uint64
	MaxDataPartitionID  uint64
	MaxMetaNodeID       uint64
	MaxMetaPartitionID  uint64
	DataNodeStatInfo    *NodeStatInfo
	MetaNodeStatInfo    *NodeStatInfo
	VolStatInfo         []*VolStatInfo
	BadPartitionIDs     []BadPartitionView
	BadMetaPartitionIDs []BadPartitionView
	MetaNodes           []NodeView
	DataNodes           []NodeView
}

// NodeView provides the view of the data or meta node.
type NodeView struct {
	Addr       string
	Status     bool
	ID         uint64
	IsWritable bool
}

type BadPartitionView struct {
	Path         string
	PartitionIDs []uint64
}

type ClusterStatInfo struct {
	DataNodeStatInfo *NodeStatInfo
	MetaNodeStatInfo *NodeStatInfo
	ZoneStatInfo     map[string]*ZoneStat
}

type ZoneStat struct {
	DataNodeStat *ZoneNodesStat
	MetaNodeStat *ZoneNodesStat
}
type ZoneNodesStat struct {
	Total         float64 `json:"TotalGB"`
	Used          float64 `json:"UsedGB"`
	Avail         float64 `json:"AvailGB"`
	UsedRatio     float64
	TotalNodes    int
	WritableNodes int
}

type NodeStatInfo struct {
	TotalGB     uint64
	UsedGB      uint64
	IncreasedGB int64
	UsedRatio   string
}

type VolStatInfo struct {
	Name        string
	TotalSize   uint64
	UsedSize    uint64
	UsedRatio   string
	EnableToken bool
}

// DataPartition represents the structure of storing the file contents.
type DataPartitionInfo struct {
	PartitionID             uint64
	LastLoadedTime          int64
	ReplicaNum              uint8
	Status                  int8
	Replicas                []*DataReplica
	Hosts                   []string // host addresses
	Peers                   []Peer
	Zones                   []string
	MissingNodes            map[string]int64 // key: address of the missing node, value: when the node is missing
	VolName                 string
	VolID                   uint64
	OfflinePeerID           uint64
	FileInCoreMap           map[string]*FileInCore
	FilesWithMissingReplica map[string]int64 // key: file name, value: last time when a missing replica is found
}

//FileInCore define file in data partition
type FileInCore struct {
	Name          string
	LastModify    int64
	MetadataArray []*FileMetadata
}

// FileMetadata defines the file metadata on a dataNode
type FileMetadata struct {
	Crc     uint32
	LocAddr string
	Size    uint32
}

// DataReplica represents the replica of a data partition
type DataReplica struct {
	Addr            string
	ReportTime      int64
	FileCount       uint32
	Status          int8
	HasLoadResponse bool   // if there is any response when loading
	Total           uint64 `json:"TotalSize"`
	Used            uint64 `json:"UsedSize"`
	IsLeader        bool
	NeedsToCompare  bool
	DiskPath        string
}

// data partition diagnosis represents the inactive data nodes, corrupt data partitions, and data partitions lack of replicas
type DataPartitionDiagnosis struct {
	InactiveDataNodes           []string
	CorruptDataPartitionIDs     []uint64
	LackReplicaDataPartitionIDs []uint64
	BadDataPartitionIDs         []BadPartitionView
}

// meta partition diagnosis represents the inactive meta nodes, corrupt meta partitions, and meta partitions lack of replicas
type MetaPartitionDiagnosis struct {
	InactiveMetaNodes           []string
	CorruptMetaPartitionIDs     []uint64
	LackReplicaMetaPartitionIDs []uint64
	BadMetaPartitionIDs         []BadPartitionView
}
