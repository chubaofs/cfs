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

// api
const (
	// Admin APIs
	AdminGetCluster                = "/admin/getCluster"
	AdminGetDataPartition          = "/dataPartition/get"
	AdminLoadDataPartition         = "/dataPartition/load"
	AdminCreateDataPartition       = "/dataPartition/create"
	AdminDecommissionDataPartition = "/dataPartition/decommission"
	AdminDiagnoseDataPartition     = "/dataPartition/diagnose"
	AdminDeleteDataReplica         = "/dataReplica/delete"
	AdminAddDataReplica            = "/dataReplica/add"
	AdminDeleteVol                 = "/vol/delete"
	AdminUpdateVol                 = "/vol/update"
	AdminVolShrink                 = "/vol/shrink"
	AdminVolExpand                 = "/vol/expand"
	AdminCreateVol                 = "/admin/createVol"
	AdminGetVol                    = "/admin/getVol"
	AdminClusterFreeze             = "/cluster/freeze"
	AdminClusterStat               = "/cluster/stat"
	AdminGetIP                = "/admin/getIp"
	AdminCreateMetaPartition      = "/metaPartition/create"
	AdminSetMetaNodeThreshold     = "/threshold/set"
	AdminListVols                 = "/vol/list"
	AdminSetNodeInfo              = "/admin/setNodeInfo"
	AdminGetNodeInfo              = "/admin/getNodeInfo"
	AdminGetAllNodeSetGrpInfo     = "/admin/getDomainInfo"
	AdminGetNodeSetGrpInfo        = "/admin/getDomainNodeSetGrpInfo"
	AdminGetIsDomainOn            = "/admin/getIsDomainOn"
	AdminUpdateNodeSetCapcity     = "/admin/updateNodeSetCapcity"
	AdminUpdateNodeSetId          = "/admin/updateNodeSetId"
	AdminUpdateDomainDataUseRatio = "/admin/updateDomainDataRatio"
	AdminUpdateZoneExcludeRatio   = "/admin/updateZoneExcludeRatio"
	//graphql master api
	AdminClusterAPI = "/api/cluster"
	AdminUserAPI    = "/api/user"
	AdminVolumeAPI  = "/api/volume"

	//graphql coonsole api
	ConsoleIQL        = "/iql"
	ConsoleLoginAPI   = "/login"
	ConsoleMonitorAPI = "/cfs_monitor"
	ConsoleFile       = "/file"
	ConsoleFileDown   = "/file/down"
	ConsoleFileUpload = "/file/upload"

	// Client APIs
	ClientDataPartitions = "/client/partitions"
	ClientVol            = "/client/vol"
	ClientMetaPartition  = "/metaPartition/get"
	ClientVolStat        = "/client/volStat"
	ClientMetaPartitions = "/client/metaPartitions"

	//raft node APIs
	AddRaftNode    = "/raftNode/add"
	RemoveRaftNode = "/raftNode/remove"

	// Node APIs
	AddDataNode                    = "/dataNode/add"
	DecommissionDataNode           = "/dataNode/decommission"
	DecommissionDisk               = "/disk/decommission"
	GetDataNode                    = "/dataNode/get"
	AddMetaNode                    = "/metaNode/add"
	DecommissionMetaNode           = "/metaNode/decommission"
	GetMetaNode                    = "/metaNode/get"
	AdminUpdateMetaNode            = "/metaNode/update"
	AdminUpdateDataNode            = "/dataNode/update"
	AdminGetInvalidNodes           = "/invalid/nodes"
	AdminLoadMetaPartition         = "/metaPartition/load"
	AdminDiagnoseMetaPartition     = "/metaPartition/diagnose"
	AdminDecommissionMetaPartition = "/metaPartition/decommission"
	AdminAddMetaReplica            = "/metaReplica/add"
	AdminDeleteMetaReplica         = "/metaReplica/delete"

	// Operation response
	GetMetaNodeTaskResponse = "/metaNode/response" // Method: 'POST', ContentType: 'application/json'
	GetDataNodeTaskResponse = "/dataNode/response" // Method: 'POST', ContentType: 'application/json'

	GetTopologyView = "/topo/get"
	UpdateZone      = "/zone/update"
	GetAllZones     = "/zone/list"

	// Header keys
	SkipOwnerValidation = "Skip-Owner-Validation"
	ForceDelete         = "Force-Delete"

	// APIs for user management
	UserCreate          = "/user/create"
	UserDelete          = "/user/delete"
	UserUpdate          = "/user/update"
	UserUpdatePolicy    = "/user/updatePolicy"
	UserRemovePolicy    = "/user/removePolicy"
	UserDeleteVolPolicy = "/user/deleteVolPolicy"
	UserGetInfo         = "/user/info"
	UserGetAKInfo       = "/user/akInfo"
	UserTransferVol     = "/user/transferVol"
	UserList            = "/user/list"
	UsersOfVol          = "/vol/users"
	//graphql api for header
	HeadAuthorized  = "Authorization"
	ParamAuthorized = "_authorization"
	UserKey         = "_user_key"
	UserInfoKey     = "_user_info_key"
)

const TimeFormat = "2006-01-02 15:04:05"

// HTTPReply uniform response structure
type HTTPReply struct {
	Code int32       `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// RegisterMetaNodeResp defines the response to register a meta node.
type RegisterMetaNodeResp struct {
	ID uint64
}

// ClusterInfo defines the cluster infomation.
type ClusterInfo struct {
	Cluster                     string
	Ip                          string
	MetaNodeDeleteBatchCount    uint64
	MetaNodeDeleteWorkerSleepMs uint64
	DataNodeDeleteLimitRate     uint64
	DataNodeAutoRepairLimitRate uint64
}

// CreateDataPartitionRequest defines the request to create a data partition.
type CreateDataPartitionRequest struct {
	PartitionType string
	PartitionId   uint64
	PartitionSize int
	VolumeId      string
	IsRandomWrite bool
	Members       []Peer
	Hosts         []string
	CreateType    int
}

// CreateDataPartitionResponse defines the response to the request of creating a data partition.
type CreateDataPartitionResponse struct {
	PartitionId uint64
	Status      uint8
	Result      string
}

// DeleteDataPartitionRequest defines the request to delete a data partition.
type DeleteDataPartitionRequest struct {
	DataPartitionType string
	PartitionId       uint64
	PartitionSize     int
}

// DeleteDataPartitionResponse defines the response to the request of deleting a data partition.
type DeleteDataPartitionResponse struct {
	Status      uint8
	Result      string
	PartitionId uint64
}

// DataPartitionDecommissionRequest defines the request of decommissioning a data partition.
type DataPartitionDecommissionRequest struct {
	PartitionId uint64
	RemovePeer  Peer
	AddPeer     Peer
}

// AddDataPartitionRaftMemberRequest defines the request of add raftMember a data partition.
type AddDataPartitionRaftMemberRequest struct {
	PartitionId uint64
	AddPeer     Peer
}

// RemoveDataPartitionRaftMemberRequest defines the request of add raftMember a data partition.
type RemoveDataPartitionRaftMemberRequest struct {
	PartitionId uint64
	RemovePeer  Peer
}

// AddMetaPartitionRaftMemberRequest defines the request of add raftMember a meta partition.
type AddMetaPartitionRaftMemberRequest struct {
	PartitionId uint64
	AddPeer     Peer
}

// RemoveMetaPartitionRaftMemberRequest defines the request of add raftMember a meta partition.
type RemoveMetaPartitionRaftMemberRequest struct {
	PartitionId uint64
	RemovePeer  Peer
}

// LoadDataPartitionRequest defines the request of loading a data partition.
type LoadDataPartitionRequest struct {
	PartitionId uint64
}

// LoadDataPartitionResponse defines the response to the request of loading a data partition.
type LoadDataPartitionResponse struct {
	PartitionId       uint64
	Used              uint64
	PartitionSnapshot []*File
	Status            uint8
	PartitionStatus   int
	Result            string
	VolName           string
}

// File defines the file struct.
type File struct {
	Name     string
	Crc      uint32
	Size     uint32
	Modified int64
}

// LoadMetaPartitionMetricRequest defines the request of loading the meta partition metrics.
type LoadMetaPartitionMetricRequest struct {
	PartitionID uint64
	Start       uint64
	End         uint64
}

// LoadMetaPartitionMetricResponse defines the response to the request of loading the meta partition metrics.
type LoadMetaPartitionMetricResponse struct {
	Start    uint64
	End      uint64
	MaxInode uint64
	Status   uint8
	Result   string
}

// HeartBeatRequest define the heartbeat request.
type HeartBeatRequest struct {
	CurrTime   int64
	MasterAddr string
}

// PartitionReport defines the partition report.
type PartitionReport struct {
	VolName         string
	PartitionID     uint64
	PartitionStatus int
	Total           uint64
	Used            uint64
	DiskPath        string
	IsLeader        bool
	ExtentCount     int
	NeedCompare     bool
}

// DataNodeHeartbeatResponse defines the response to the data node heartbeat.
type DataNodeHeartbeatResponse struct {
	Total               uint64
	Used                uint64
	Available           uint64
	TotalPartitionSize  uint64 // volCnt * volsize
	RemainingCapacity   uint64 // remaining capacity to create partition
	CreatedPartitionCnt uint32
	MaxCapacity         uint64 // maximum capacity to create partition
	ZoneName            string
	PartitionReports    []*PartitionReport
	Status              uint8
	Result              string
	BadDisks            []string
}

// MetaPartitionReport defines the meta partition report.
type MetaPartitionReport struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Status      int
	MaxInodeID  uint64
	IsLeader    bool
	VolName     string
	InodeCnt    uint64
	DentryCnt   uint64
}

// MetaNodeHeartbeatResponse defines the response to the meta node heartbeat request.
type MetaNodeHeartbeatResponse struct {
	ZoneName             string
	Total                uint64
	Used                 uint64
	MetaPartitionReports []*MetaPartitionReport
	Status               uint8
	Result               string
}

// DeleteFileRequest defines the request to delete a file.
type DeleteFileRequest struct {
	VolId uint64
	Name  string
}

// DeleteFileResponse defines the response to the request of deleting a file.
type DeleteFileResponse struct {
	Status uint8
	Result string
	VolId  uint64
	Name   string
}

// DeleteMetaPartitionRequest defines the request of deleting a meta partition.
type DeleteMetaPartitionRequest struct {
	PartitionID uint64
}

// DeleteMetaPartitionResponse defines the response to the request of deleting a meta partition.
type DeleteMetaPartitionResponse struct {
	PartitionID uint64
	Status      uint8
	Result      string
}

// UpdateMetaPartitionRequest defines the request to update a meta partition.
type UpdateMetaPartitionRequest struct {
	PartitionID uint64
	VolName     string
	Start       uint64
	End         uint64
}

// UpdateMetaPartitionResponse defines the response to the request of updating the meta partition.
type UpdateMetaPartitionResponse struct {
	PartitionID uint64
	VolName     string
	End         uint64
	Status      uint8
	Result      string
}

// MetaPartitionDecommissionRequest defines the request of decommissioning a meta partition.
type MetaPartitionDecommissionRequest struct {
	PartitionID uint64
	VolName     string
	RemovePeer  Peer
	AddPeer     Peer
}

// MetaPartitionDecommissionResponse defines the response to the request of decommissioning a meta partition.
type MetaPartitionDecommissionResponse struct {
	PartitionID uint64
	VolName     string
	Status      uint8
	Result      string
}

// MetaPartitionLoadRequest defines the request to load meta partition.
type MetaPartitionLoadRequest struct {
	PartitionID uint64
}

// MetaPartitionLoadResponse defines the response to the request of loading meta partition.
type MetaPartitionLoadResponse struct {
	PartitionID uint64
	DoCompare   bool
	ApplyID     uint64
	MaxInode    uint64
	DentryCount uint64
	InodeCount  uint64
	Addr        string
}

// DataPartitionResponse defines the response from a data node to the master that is related to a data partition.
type DataPartitionResponse struct {
	PartitionID uint64
	Status      int8
	ReplicaNum  uint8
	Hosts       []string
	LeaderAddr  string
	Epoch       uint64
	IsRecover   bool
}

// DataPartitionsView defines the view of a data partition
type DataPartitionsView struct {
	DataPartitions []*DataPartitionResponse
}

func NewDataPartitionsView() (dataPartitionsView *DataPartitionsView) {
	dataPartitionsView = new(DataPartitionsView)
	dataPartitionsView.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

// MetaPartitionView defines the view of a meta partition
type MetaPartitionView struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	MaxInodeID  uint64
	InodeCount  uint64
	DentryCount uint64
	IsRecover   bool
	Members     []string
	LeaderAddr  string
	Status      int8
}

type OSSSecure struct {
	AccessKey string
	SecretKey string
}

// VolView defines the view of a volume
type VolView struct {
	Name           string
	Owner          string
	Status         uint8
	FollowerRead   bool
	MetaPartitions []*MetaPartitionView
	DataPartitions []*DataPartitionResponse
	DomainOn       bool
	OSSSecure      *OSSSecure
	CreateTime     int64
}

func (v *VolView) SetOwner(owner string) {
	v.Owner = owner
}

func (v *VolView) SetOSSSecure(accessKey, secretKey string) {
	v.OSSSecure = &OSSSecure{AccessKey: accessKey, SecretKey: secretKey}
}

func NewVolView(name string, status uint8, followerRead bool, createTime int64) (view *VolView) {
	view = new(VolView)
	view.Name = name
	view.FollowerRead = followerRead
	view.CreateTime = createTime
	view.Status = status
	view.MetaPartitions = make([]*MetaPartitionView, 0)
	view.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

func NewMetaPartitionView(partitionID, start, end uint64, status int8) (mpView *MetaPartitionView) {
	mpView = new(MetaPartitionView)
	mpView.PartitionID = partitionID
	mpView.Start = start
	mpView.End = end
	mpView.Status = status
	mpView.Members = make([]string, 0)
	return
}

// SimpleVolView defines the simple view of a volume
type SimpleVolView struct {
	ID                 uint64
	Name               string
	Owner              string
	ZoneName           string
	DpReplicaNum       uint8
	MpReplicaNum       uint8
	InodeCount         uint64
	DentryCount        uint64
	MaxMetaPartitionID uint64
	Status             uint8
	Capacity           uint64 // GB
	RwDpCnt            int
	MpCnt              int
	DpCnt              int
	FollowerRead       bool
	NeedToLowerReplica bool
	Authenticate       bool
	CrossZone          bool
	DefaultPriority    bool
	DomainOn           bool
	CreateTime         string
	EnableToken        bool
	Description        string
	DpSelectorName     string
	DpSelectorParm     string
	DefaultZonePrior   bool
}
type NodeSetInfo struct {
	ID        uint64
	ZoneName  string
	Capacity  int
	DataUseRatio float64
	MetaUseRatio float64
	MetaUsed  uint64
	MetaTotal uint64
	MetaNodes []*MetaNodeInfo
	DataUsed  uint64
	DataTotal uint64
	DataNodes []*DataNodeInfo
}
type SimpleNodeSetGrpInfo struct {
	ID        uint64
	Status    uint8
	NodeSetInfo []NodeSetInfo
}

type SimpleNodeSetGrpInfoList struct {
	DomainOn             bool
	DataRatioLimit       float64
	ZoneExcludeRatioLimit float64
	NeedDomain           bool
	Status               uint8
	ExcludeZones         []string
	SimpleNodeSetGrpInfo []*SimpleNodeSetGrpInfo
}
// MasterAPIAccessResp defines the response for getting meta partition
type MasterAPIAccessResp struct {
	APIResp APIAccessResp `json:"api_resp"`
	Data    []byte        `json:"data"`
}

type VolInfo struct {
	Name       string
	Owner      string
	CreateTime int64
	Status     uint8
	TotalSize  uint64
	UsedSize   uint64
}

func NewVolInfo(name, owner string, createTime int64, status uint8, totalSize, usedSize uint64) *VolInfo {
	return &VolInfo{
		Name:       name,
		Owner:      owner,
		CreateTime: createTime,
		Status:     status,
		TotalSize:  totalSize,
		UsedSize:   usedSize,
	}
}

//ZoneView define the view of zone
type ZoneView struct {
	Name    string
	Status  string
	NodeSet map[uint64]*NodeSetView
}

type NodeSetView struct {
	DataNodeLen int
	MetaNodeLen int
	MetaNodes   []NodeView
	DataNodes   []NodeView
}

// TopologyView provides the view of the topology view of the cluster
type TopologyView struct {
	Zones []*ZoneView
}
