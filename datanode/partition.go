// Copyright 2018 The Containerfs Authors.
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

package datanode

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/master"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util/log"
	raftProto "github.com/tiglabs/raft/proto"
	"sort"
	"syscall"
)

const (
	DataPartitionPrefix       = "datapartition"
	DataPartitionMetaFileName = "META"
	TempMetaFileName          = ".meta"
	ApplyIndexFile            = "APPLY"
	TempApplyIndexFile        = ".apply"
	TimeLayout                = "2006-01-02 15:04:05"
)

var (
	AdminGetDataPartition = master.AdminGetDataPartition
)

type DataPartitionMeta struct {
	VolumeID      string
	PartitionID   uint64
	PartitionSize int
	CreateTime    string
	RandomWrite   bool
	Peers         []proto.Peer
}

type sortPeers []proto.Peer

func (sp sortPeers) Len() int {
	return len(sp)
}
func (sp sortPeers) Less(i, j int) bool {
	return sp[i].ID < sp[j].ID
}

func (sp sortPeers) Swap(i, j int) {
	sp[i], sp[j] = sp[j], sp[i]
}

func (meta *DataPartitionMeta) Validate() (err error) {
	meta.VolumeID = strings.TrimSpace(meta.VolumeID)
	if len(meta.VolumeID) == 0 || meta.PartitionID == 0 || meta.PartitionSize == 0 {
		err = errors.New("illegal data partition meta")
		return
	}
	return
}

type DataPartition struct {
	clusterID       string
	volumeID        string
	partitionID     uint64
	partitionStatus int
	partitionSize   int
	replicaHosts    []string
	disk            *Disk
	isLeader        bool
	isRaftLeader    bool
	path            string
	used            int
	extentStore     *storage.ExtentStore
	raftPartition   raftstore.Partition
	config          *dataPartitionCfg
	applyID         uint64
	lastTruncateID  uint64
	minAppliedID    uint64
	maxAppliedID    uint64
	repairC         chan uint64
	storeC          chan uint64
	stopC           chan bool

	runtimeMetrics          *DataPartitionMetrics
	updateReplicationTime   int64
	isFirstFixTinyExtents   bool
	snapshot                []*proto.File
	snapshotLock            sync.RWMutex
	loadExtentHeaderStatus  int
	updatePartitionSizeTime int64
}

func CreateDataPartition(dpCfg *dataPartitionCfg, disk *Disk) (dp *DataPartition, err error) {

	if dp, err = newDataPartition(dpCfg, disk); err != nil {
		return
	}
	go dp.ForceLoadHeader()

	// Start raft for random write
	if dpCfg.RandomWrite {
		go dp.StartSchedule()
		go dp.WaitingRepairedAndStartRaft()
	}

	// Store meta information into meta file.
	err = dp.StoreMeta()
	return
}

// LoadDataPartition load and returns partition instance from specified directory.
// This method will read the partition meta file stored under the specified directory
// and create partition instance.
func LoadDataPartition(partitionDir string, disk *Disk) (dp *DataPartition, err error) {
	var (
		metaFileData []byte
	)
	if metaFileData, err = ioutil.ReadFile(path.Join(partitionDir, DataPartitionMetaFileName)); err != nil {
		return
	}
	meta := &DataPartitionMeta{}
	if err = json.Unmarshal(metaFileData, meta); err != nil {
		return
	}
	if err = meta.Validate(); err != nil {
		return
	}

	dpCfg := &dataPartitionCfg{
		VolName:       meta.VolumeID,
		PartitionSize: meta.PartitionSize,
		PartitionID:   meta.PartitionID,
		RandomWrite:   meta.RandomWrite,
		Peers:         meta.Peers,
		RaftStore:     disk.space.GetRaftStore(),
		NodeID:        disk.space.GetNodeID(),
		ClusterID:     disk.space.GetClusterID(),
	}
	if dp, err = newDataPartition(dpCfg, disk); err != nil {
		return
	}
	go dp.ForceLoadHeader()
	if dpCfg.RandomWrite {
		if err = dp.LoadApplyIndex(); err != nil {
			log.LogErrorf("action[loadApplyIndex] %v", err)
		}

		if err = dp.StartRaft(); err != nil {
			return
		}

		go dp.StartSchedule()
	}
	return
}

func newDataPartition(dpCfg *dataPartitionCfg, disk *Disk) (dp *DataPartition, err error) {
	partitionID := dpCfg.PartitionID
	dataPath := path.Join(disk.Path, fmt.Sprintf(DataPartitionPrefix+"_%v_%v", partitionID, dpCfg.PartitionSize))
	partition := &DataPartition{
		volumeID:               dpCfg.VolName,
		clusterID:              dpCfg.ClusterID,
		partitionID:            partitionID,
		disk:                   disk,
		path:                   dataPath,
		partitionSize:          dpCfg.PartitionSize,
		replicaHosts:           make([]string, 0),
		stopC:                  make(chan bool, 0),
		repairC:                make(chan uint64, 0),
		storeC:                 make(chan uint64, 128),
		partitionStatus:        proto.ReadWrite,
		runtimeMetrics:         NewDataPartitionMetrics(),
		config:                 dpCfg,
		loadExtentHeaderStatus: StartLoadDataPartitionExtentHeader,
	}
	partition.extentStore, err = storage.NewExtentStore(partition.path, dpCfg.PartitionID, dpCfg.PartitionSize)
	if err != nil {
		return
	}
	partition.isFirstFixTinyExtents = true
	disk.AttachDataPartition(partition)
	dp = partition
	go partition.statusUpdateScheduler()
	return
}

func (dp *DataPartition) ID() uint64 {
	return dp.partitionID
}

func (dp *DataPartition) GetExtentCount() int {
	return dp.extentStore.GetExtentCount()
}

func (dp *DataPartition) Path() string {
	return dp.path
}

func (dp *DataPartition) IsRaftLeader() (leaderAddr string, ok bool) {
	if dp.raftPartition == nil {
		return
	}
	leaderID, _ := dp.raftPartition.LeaderTerm()
	if leaderID == 0 {
		return
	}
	ok = leaderID == dp.config.NodeID
	for _, peer := range dp.config.Peers {
		if leaderID == peer.ID {
			leaderAddr = peer.Addr
			return
		}
	}
	return
}

func (dp *DataPartition) ReplicaHosts() []string {
	return dp.replicaHosts
}

func (dp *DataPartition) LoadExtentHeaderStatus() int {
	return dp.loadExtentHeaderStatus
}

func (dp *DataPartition) ReloadSnapshot() {
	if dp.loadExtentHeaderStatus != FinishLoadDataPartitionExtentHeader {
		return
	}
	files, err := dp.extentStore.SnapShot()
	if err != nil {
		return
	}
	dp.snapshotLock.Lock()
	dp.snapshot = files
	dp.snapshotLock.Unlock()
}

func (dp *DataPartition) GetSnapShot() (files []*proto.File) {
	dp.snapshotLock.RLock()
	defer dp.snapshotLock.RUnlock()

	return dp.snapshot
}

func (dp *DataPartition) Stop() {
	if dp.stopC != nil {
		close(dp.stopC)
	}
	// Close all store and backup partition data file.
	dp.extentStore.Close()
	dp.stopRaft()
}

func (dp *DataPartition) FlushDelete() (err error) {
	err = dp.extentStore.FlushDelete()
	return
}

func (dp *DataPartition) Disk() *Disk {
	return dp.disk
}

func (dp *DataPartition) Status() int {
	return dp.partitionStatus
}

func (dp *DataPartition) Size() int {
	return dp.partitionSize
}

func (dp *DataPartition) Used() int {
	return dp.used
}

func (dp *DataPartition) Available() int {
	return dp.partitionSize - dp.used
}

func (dp *DataPartition) ChangeStatus(status int) {
	switch status {
	case proto.ReadOnly, proto.ReadWrite, proto.Unavaliable:
		dp.partitionStatus = status
	}
}

func (dp *DataPartition) ForceLoadHeader() {
	dp.extentStore.BackEndLoadExtent()
	dp.loadExtentHeaderStatus = FinishLoadDataPartitionExtentHeader
}

func (dp *DataPartition) StoreMeta() (err error) {
	// Store meta information into meta file.
	var (
		metaFile *os.File
		metaData []byte
	)
	tempFileName := path.Join(dp.Path(), TempMetaFileName)
	if metaFile, err = os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	defer func() {
		metaFile.Sync()
		metaFile.Close()
		os.Remove(tempFileName)
	}()

	sp := sortPeers(dp.config.Peers)
	sort.Sort(sp)

	meta := &DataPartitionMeta{
		VolumeID:      dp.config.VolName,
		PartitionID:   dp.config.PartitionID,
		PartitionSize: dp.config.PartitionSize,
		Peers:         dp.config.Peers,
		RandomWrite:   dp.config.RandomWrite,
		CreateTime:    time.Now().Format(TimeLayout),
	}
	if metaData, err = json.Marshal(meta); err != nil {
		return
	}
	if _, err = metaFile.Write(metaData); err != nil {
		return
	}

	err = os.Rename(tempFileName, path.Join(dp.Path(), DataPartitionMetaFileName))
	return
}

func (dp *DataPartition) statusUpdateScheduler() {
	ticker := time.NewTicker(10 * time.Second)
	metricTicker := time.NewTicker(5 * time.Second)
	var index int
	for {
		select {
		case <-ticker.C:
			index++
			dp.statusUpdate()
			if index >= math.MaxUint32 {
				index = 0
			}
			if index%2 == 0 {
				dp.LaunchRepair(proto.TinyExtentMode)
			} else {
				dp.LaunchRepair(proto.NormalExtentMode)
			}
			dp.ReloadSnapshot()
		case <-dp.stopC:
			ticker.Stop()
			metricTicker.Stop()
			return
		case <-metricTicker.C:
			dp.runtimeMetrics.recomputeLatency()
		}
	}
}

func (dp *DataPartition) statusUpdate() {
	status := proto.ReadWrite
	dp.computeUsage()
	if dp.used >= dp.partitionSize {
		status = proto.ReadOnly
	}
	if dp.extentStore.GetExtentCount() >= MaxActiveExtents {
		status = proto.ReadOnly
	}
	dp.partitionStatus = int(math.Min(float64(status), float64(dp.disk.Status)))
}

func ParseExtentID(filename string) (extentID uint64, isExtent bool) {
	if isExtent = storage.RegexpExtentFile.MatchString(filename); !isExtent {
		return
	}
	var (
		err error
	)
	if extentID, err = strconv.ParseUint(filename, 10, 64); err != nil {
		isExtent = false
		return
	}
	isExtent = true
	return
}

func (dp *DataPartition) getRealSize(path string, finfo os.FileInfo) (size int64) {
	name := finfo.Name()
	extentID, isExtent := ParseExtentID(name)
	if !isExtent {
		return finfo.Size()
	}
	if storage.IsTinyExtent(extentID) {
		stat := new(syscall.Stat_t)
		err := syscall.Stat(fmt.Sprintf("%v/%v", path, finfo.Name()), stat)
		if err != nil {
			return finfo.Size()
		}
		return stat.Blocks * DiskSectorSize
	}

	return finfo.Size()
}

func (dp *DataPartition) computeUsage() {
	var (
		used  int64
		files []os.FileInfo
		err   error
	)
	if time.Now().Unix()-dp.updatePartitionSizeTime < UpdatePartitionSizeTime {
		return
	}
	if files, err = ioutil.ReadDir(dp.path); err != nil {
		return
	}
	for _, file := range files {
		used += dp.getRealSize(dp.path, file)
	}
	dp.used = int(used)
	dp.updatePartitionSizeTime = time.Now().Unix()
}

func (dp *DataPartition) GetStore() *storage.ExtentStore {
	return dp.extentStore
}

func (dp *DataPartition) String() (m string) {
	return fmt.Sprintf(DataPartitionPrefix+"_%v_%v", dp.partitionID, dp.partitionSize)
}

func (dp *DataPartition) LaunchRepair(fixExtentType uint8) {
	if dp.partitionStatus == proto.Unavaliable {
		return
	}
	if err := dp.updateReplicaHosts(); err != nil {
		log.LogErrorf("action[LaunchRepair] err(%v).", err)
		return
	}
	if !dp.isLeader {
		return
	}
	if dp.extentStore.GetUnAvaliExtentLen() == 0 {
		dp.extentStore.MoveAvaliExtentToUnavali(MinFixTinyExtents)
	}
	dp.extentFileRepair(fixExtentType)
}

func (dp *DataPartition) updateReplicaHosts() (err error) {
	if time.Now().Unix()-dp.updateReplicationTime <= UpdateReplicationHostsTime {
		return
	}
	dp.isLeader = false
	isLeader, replicas, err := dp.fetchReplicaHosts()
	if err != nil {
		return
	}
	if !dp.compareReplicaHosts(dp.replicaHosts, replicas) {
		log.LogInfof("action[updateReplicaHosts] partition(%v) replicaHosts changed from (%v) to (%v).",
			dp.partitionID, dp.replicaHosts, replicas)
	}
	dp.isLeader = isLeader
	dp.replicaHosts = replicas
	dp.updateReplicationTime = time.Now().Unix()
	log.LogInfof(fmt.Sprintf("ActionUpdateReplicationHosts partiton[%v]", dp.partitionID))

	return
}

func (dp *DataPartition) compareReplicaHosts(v1, v2 []string) (equals bool) {
	// Compare fetched replica hosts with local stored hosts.
	equals = true
	if len(v1) == len(v2) {
		for i := 0; i < len(v1); i++ {
			if v1[i] != v2[i] {
				equals = false
				return
			}
		}
		equals = true
		return
	}
	equals = false
	return
}

func (dp *DataPartition) fetchReplicaHosts() (isLeader bool, replicaHosts []string, err error) {
	var (
		HostsBuf []byte
	)
	params := make(map[string]string)
	params["id"] = strconv.Itoa(int(dp.partitionID))
	if HostsBuf, err = MasterHelper.Request("GET", AdminGetDataPartition, params, nil); err != nil {
		isLeader = false
		return
	}
	response := &master.DataPartition{}
	replicaHosts = make([]string, 0)
	if err = json.Unmarshal(HostsBuf, &response); err != nil {
		isLeader = false
		replicaHosts = nil
		return
	}
	for _, host := range response.PersistenceHosts {
		replicaHosts = append(replicaHosts, host)
	}
	if response.PersistenceHosts != nil && len(response.PersistenceHosts) >= 1 {
		leaderAddr := response.PersistenceHosts[0]
		leaderAddrParts := strings.Split(leaderAddr, ":")
		if len(leaderAddrParts) == 2 && strings.TrimSpace(leaderAddrParts[0]) == LocalIP {
			isLeader = true
		}
	}
	return
}

func (dp *DataPartition) Load() (response *proto.LoadDataPartitionResponse) {
	response = &proto.LoadDataPartitionResponse{}
	response.PartitionId = uint64(dp.partitionID)
	response.PartitionStatus = dp.partitionStatus
	response.Used = uint64(dp.Used())
	var err error
	if dp.loadExtentHeaderStatus != FinishLoadDataPartitionExtentHeader {
		response.PartitionSnapshot = make([]*proto.File, 0)
	} else {
		response.PartitionSnapshot = dp.GetSnapShot()
	}
	if err != nil {
		response.Status = proto.TaskFail
		response.Result = err.Error()
		return
	}
	return
}

func (dp *DataPartition) GetAllExtentsMeta() (files []*storage.ExtentInfo, err error) {
	files, err = dp.extentStore.GetAllWatermark(storage.GetStableExtentFilter())
	if err != nil {
		return nil, err
	}

	return
}

func (dp *DataPartition) MergeExtentStoreRepair(metas *DataPartitionRepairTask) {
	store := dp.extentStore
	for _, addExtent := range metas.AddExtentsTasks {
		if storage.IsTinyExtent(addExtent.FileID) {
			continue
		}
		if store.IsExistExtent(uint64(addExtent.FileID)) {
			fixFileSizeTask := &storage.ExtentInfo{Source: addExtent.Source, FileID: addExtent.FileID, Size: addExtent.Size}
			metas.FixExtentSizeTasks = append(metas.FixExtentSizeTasks, fixFileSizeTask)
			continue
		}
		err := store.Create(uint64(addExtent.FileID), addExtent.Inode)
		if err != nil {
			continue
		}
		fixFileSizeTask := &storage.ExtentInfo{Source: addExtent.Source, FileID: addExtent.FileID, Size: addExtent.Size}
		metas.FixExtentSizeTasks = append(metas.FixExtentSizeTasks, fixFileSizeTask)
	}
	var (
		wg           *sync.WaitGroup
		recoverIndex int
	)
	wg = new(sync.WaitGroup)
	for _, fixExtent := range metas.FixExtentSizeTasks {
		if !store.IsExistExtent(uint64(fixExtent.FileID)) {
			continue
		}
		wg.Add(1)
		go dp.doStreamExtentFixRepair(wg, fixExtent)
		recoverIndex++
		if recoverIndex%SimultaneouslyRecoverFiles == 0 {
			wg.Wait()
		}
	}
	wg.Wait()

}

func (dp *DataPartition) AddWriteMetrics(latency uint64) {
	dp.runtimeMetrics.AddWriteMetrics(latency)
}

func (dp *DataPartition) AddReadMetrics(latency uint64) {
	dp.runtimeMetrics.AddReadMetrics(latency)
}

func (dp *DataPartition) ChangeRaftMember(changeType raftProto.ConfChangeType, peer raftProto.Peer, context []byte) (resp interface{}, err error) {
	resp, err = dp.raftPartition.ChangeMember(changeType, peer, context)
	return
}
