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
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"bytes"
	"io/ioutil"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/cryptoutil"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

// NodeView provides the view of the data or meta node.
type NodeView struct {
	Addr       string
	Status     bool
	ID         uint64
	IsWritable bool
}

// NodeView provides the view of the data or meta node.
type InvalidNodeView struct {
	Addr     string
	ID       uint64
	OldID    uint64
	NodeType string
}

// TopologyView provides the view of the topology view of the cluster
type TopologyView struct {
	Zones []*ZoneView
}

type NodeSetView struct {
	DataNodeLen int
	MetaNodeLen int
	MetaNodes   []proto.NodeView
	DataNodes   []proto.NodeView
}

func newNodeSetView(dataNodeLen, metaNodeLen int) *NodeSetView {
	return &NodeSetView{DataNodes: make([]proto.NodeView, 0), MetaNodes: make([]proto.NodeView, 0), DataNodeLen: dataNodeLen, MetaNodeLen: metaNodeLen}
}

//ZoneView define the view of zone
type ZoneView struct {
	Name    string
	Status  string
	NodeSet map[uint64]*NodeSetView
}

func newZoneView(name string) *ZoneView {
	return &ZoneView{NodeSet: make(map[uint64]*NodeSetView, 0), Name: name}
}

type badPartitionView = proto.BadPartitionView

// Set the threshold of the memory usage on each meta node.
// If the memory usage reaches this threshold, then all the mata partition will be marked as readOnly.
func (m *Server) setMetaNodeThreshold(w http.ResponseWriter, r *http.Request) {
	var (
		threshold float64
		err       error
	)
	if threshold, err = parseAndExtractThreshold(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.setMetaNodeThreshold(float32(threshold)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set threshold to %v successfully", threshold)))
}

// Turn on or off the automatic allocation of the data partitions.
// If DisableAutoAllocate == off, then we WILL NOT automatically allocate new data partitions for the volume when:
// 	1. the used space is below the max capacity,
//	2. and the number of r&w data partition is less than 20.
//
// If DisableAutoAllocate == on, then we WILL automatically allocate new data partitions for the volume when:
// 	1. the used space is below the max capacity,
//	2. and the number of r&w data partition is less than 20.
func (m *Server) setupAutoAllocation(w http.ResponseWriter, r *http.Request) {
	var (
		status bool
		err    error
	)
	if status, err = parseAndExtractStatus(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.setDisableAutoAllocate(status); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set DisableAutoAllocate to %v successfully", status)))
}

// View the topology of the cluster.
func (m *Server) getTopology(w http.ResponseWriter, r *http.Request) {
	tv := &TopologyView{
		Zones: make([]*ZoneView, 0),
	}
	zones := m.cluster.t.getAllZones()
	for _, zone := range zones {
		cv := newZoneView(zone.name)
		cv.Status = zone.getStatusToString()
		tv.Zones = append(tv.Zones, cv)
		nsc := zone.getAllNodeSet()
		for _, ns := range nsc {
			nsView := newNodeSetView(ns.dataNodeLen(), ns.metaNodeLen())
			cv.NodeSet[ns.ID] = nsView
			ns.dataNodes.Range(func(key, value interface{}) bool {
				dataNode := value.(*DataNode)
				nsView.DataNodes = append(nsView.DataNodes, proto.NodeView{ID: dataNode.ID, Addr: dataNode.Addr, Status: dataNode.isActive, IsWritable: dataNode.isWriteAble()})
				return true
			})
			ns.metaNodes.Range(func(key, value interface{}) bool {
				metaNode := value.(*MetaNode)
				nsView.MetaNodes = append(nsView.MetaNodes, proto.NodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive, IsWritable: metaNode.isWritable()})
				return true
			})
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(tv))
}

func (m *Server) updateZone(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		err  error
	)
	if name = r.FormValue(nameKey); name == "" {
		err = keyNotFound(nameKey)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	status, err := extractStatus(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	zone, err := m.cluster.t.getZone(name)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeZoneNotExists, Msg: err.Error()})
		return
	}
	if status {
		zone.setStatus(normalZone)
	} else {
		zone.setStatus(unavailableZone)
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("update zone status to [%v] successfully", status)))
}

func (m *Server) listZone(w http.ResponseWriter, r *http.Request) {
	zones := m.cluster.t.getAllZones()
	zoneViews := make([]*ZoneView, 0)
	for _, zone := range zones {
		cv := newZoneView(zone.name)
		cv.Status = zone.getStatusToString()
		zoneViews = append(zoneViews, cv)
	}
	sendOkReply(w, r, newSuccessHTTPReply(zoneViews))
}

func (m *Server) clusterStat(w http.ResponseWriter, r *http.Request) {
	cs := &proto.ClusterStatInfo{
		DataNodeStatInfo: m.cluster.dataNodeStatInfo,
		MetaNodeStatInfo: m.cluster.metaNodeStatInfo,
		ZoneStatInfo:     make(map[string]*proto.ZoneStat, 0),
	}
	for zoneName, zoneStat := range m.cluster.zoneStatInfos {
		cs.ZoneStatInfo[zoneName] = zoneStat
	}
	sendOkReply(w, r, newSuccessHTTPReply(cs))
}

func (m *Server) getCluster(w http.ResponseWriter, r *http.Request) {
	cv := &proto.ClusterView{
		Name:                m.cluster.Name,
		LeaderAddr:          m.leaderInfo.addr,
		DisableAutoAlloc:    m.cluster.DisableAutoAllocate,
		MetaNodeThreshold:   m.cluster.cfg.MetaNodeThreshold,
		Applied:             m.fsm.applied,
		MaxDataPartitionID:  m.cluster.idAlloc.dataPartitionID,
		MaxMetaNodeID:       m.cluster.idAlloc.commonID,
		MaxMetaPartitionID:  m.cluster.idAlloc.metaPartitionID,
		MetaNodes:           make([]proto.NodeView, 0),
		DataNodes:           make([]proto.NodeView, 0),
		VolStatInfo:         make([]*proto.VolStatInfo, 0),
		BadPartitionIDs:     make([]proto.BadPartitionView, 0),
		BadMetaPartitionIDs: make([]proto.BadPartitionView, 0),
	}

	vols := m.cluster.allVolNames()
	cv.MetaNodes = m.cluster.allMetaNodes()
	cv.DataNodes = m.cluster.allDataNodes()
	cv.DataNodeStatInfo = m.cluster.dataNodeStatInfo
	cv.MetaNodeStatInfo = m.cluster.metaNodeStatInfo
	for _, name := range vols {
		stat, ok := m.cluster.volStatInfo.Load(name)
		if !ok {
			cv.VolStatInfo = append(cv.VolStatInfo, newVolStatInfo(name, 0, 0, "0.0001"))
			continue
		}
		cv.VolStatInfo = append(cv.VolStatInfo, stat.(*volStatInfo))
	}
	cv.BadPartitionIDs = m.cluster.getBadDataPartitionsView()
	cv.BadMetaPartitionIDs = m.cluster.getBadMetaPartitionsView()

	sendOkReply(w, r, newSuccessHTTPReply(cv))
}

func (m *Server) getIPAddr(w http.ResponseWriter, r *http.Request) {
	m.cluster.loadClusterValue()
	batchCount := atomic.LoadUint64(&m.cluster.cfg.MetaNodeDeleteBatchCount)
	limitRate := atomic.LoadUint64(&m.cluster.cfg.DataNodeDeleteLimitRate)
	deleteSleepMs := atomic.LoadUint64(&m.cluster.cfg.MetaNodeDeleteWorkerSleepMs)
	autoRepairRate := atomic.LoadUint64(&m.cluster.cfg.DataNodeAutoRepairLimitRate)
	cInfo := &proto.ClusterInfo{
		Cluster:                     m.cluster.Name,
		MetaNodeDeleteBatchCount:    batchCount,
		MetaNodeDeleteWorkerSleepMs: deleteSleepMs,
		DataNodeDeleteLimitRate:     limitRate,
		DataNodeAutoRepairLimitRate: autoRepairRate,
		Ip:                          strings.Split(r.RemoteAddr, ":")[0],
	}
	sendOkReply(w, r, newSuccessHTTPReply(cInfo))
}

func (m *Server) createMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		volName string
		start   uint64
		err     error
	)

	if volName, start, err = validateRequestToCreateMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.updateInodeIDRange(volName, start); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprint("create meta partition successfully")))
}

func (m *Server) createDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg                     string
		volName                    string
		vol                        *Vol
		reqCreateCount             int
		lastTotalDataPartitions    int
		clusterTotalDataPartitions int
		err                        error
	)

	if reqCreateCount, volName, err = parseRequestToCreateDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	lastTotalDataPartitions = len(vol.dataPartitions.partitions)
	clusterTotalDataPartitions = m.cluster.getDataPartitionCount()
	err = m.cluster.batchCreateDataPartition(vol, reqCreateCount)
	rstMsg = fmt.Sprintf(" createDataPartition succeeeds. "+
		"clusterLastTotalDataPartitions[%v],vol[%v] has %v data partitions previously and %v data partitions now",
		clusterTotalDataPartitions, volName, lastTotalDataPartitions, len(vol.dataPartitions.partitions))
	if err != nil {
		log.LogErrorf("create data partition fail: volume(%v) err(%v)", volName, err)
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	_ = sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) getDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		dp          *DataPartition
		partitionID uint64
		volName     string
		vol         *Vol
		err         error
	)
	if partitionID, volName, err = parseRequestToGetDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if volName != "" {
		if vol, err = m.cluster.getVol(volName); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
			return
		}
		if dp, err = vol.getDataPartitionByID(partitionID); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
			return
		}
	} else {
		if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
			return
		}
	}

	sendOkReply(w, r, newSuccessHTTPReply(dp.ToProto(m.cluster)))
}

// Load the data partition.
func (m *Server) loadDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		dp          *DataPartition
		partitionID uint64
		err         error
	)

	if partitionID, err = parseRequestToLoadDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	m.cluster.loadDataPartition(dp)
	msg = fmt.Sprintf(proto.AdminLoadDataPartition+"partitionID :%v  load data partition successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) addDataReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		dp          *DataPartition
		partitionID uint64
		err         error
	)

	if partitionID, addr, err = parseRequestToAddDataReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	if err = m.cluster.addDataReplica(dp, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	dp.Status = proto.ReadOnly
	dp.isRecover = true
	m.cluster.putBadDataPartitionIDs(nil, addr, dp.PartitionID)
	msg = fmt.Sprintf("data partitionID :%v  add replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) deleteDataReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		dp          *DataPartition
		partitionID uint64
		err         error
	)

	if partitionID, addr, err = parseRequestToRemoveDataReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}

	if err = m.cluster.removeDataReplica(dp, addr, true); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("data partitionID :%v  delete replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) addMetaReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		mp          *MetaPartition
		partitionID uint64
		err         error
	)

	if partitionID, addr, err = parseRequestToAddMetaReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	if err = m.cluster.addMetaReplica(mp, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	mp.IsRecover = true
	m.cluster.putBadMetaPartitions(addr, mp.PartitionID)
	msg = fmt.Sprintf("meta partitionID :%v  add replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) deleteMetaReplica(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		addr        string
		mp          *MetaPartition
		partitionID uint64
		err         error
	)

	if partitionID, addr, err = parseRequestToRemoveMetaReplica(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	if err = m.cluster.deleteMetaReplica(mp, addr, true); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("meta partitionID :%v  delete replica [%v] successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// Decommission a data partition. This usually happens when disk error has been reported.
// This function needs to be called manually by the admin.
func (m *Server) decommissionDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		rstMsg      string
		dp          *DataPartition
		addr        string
		partitionID uint64
		err         error
	)

	if partitionID, addr, err = parseRequestToDecommissionDataPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if dp, err = m.cluster.getDataPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataPartitionNotExists))
		return
	}
	if err = m.cluster.decommissionDataPartition(addr, dp, handleDataPartitionOfflineErr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf(proto.AdminDecommissionDataPartition+" dataPartitionID :%v  on node:%v successfully", partitionID, addr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) diagnoseDataPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err               error
		rstMsg            *proto.DataPartitionDiagnosis
		inactiveNodes     []string
		corruptDps        []*DataPartition
		lackReplicaDps    []*DataPartition
		corruptDpIDs      []uint64
		lackReplicaDpIDs  []uint64
		badDataPartitions []badPartitionView
	)
	corruptDpIDs = make([]uint64, 0)
	lackReplicaDpIDs = make([]uint64, 0)
	if inactiveNodes, corruptDps, err = m.cluster.checkCorruptDataPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if lackReplicaDps, err = m.cluster.checkLackReplicaDataPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	for _, dp := range corruptDps {
		corruptDpIDs = append(corruptDpIDs, dp.PartitionID)
	}
	for _, dp := range lackReplicaDps {
		lackReplicaDpIDs = append(lackReplicaDpIDs, dp.PartitionID)
	}
	badDataPartitions = m.cluster.getBadDataPartitionsView()
	rstMsg = &proto.DataPartitionDiagnosis{
		InactiveDataNodes:           inactiveNodes,
		CorruptDataPartitionIDs:     corruptDpIDs,
		LackReplicaDataPartitionIDs: lackReplicaDpIDs,
		BadDataPartitionIDs:         badDataPartitions,
	}
	log.LogInfof("diagnose dataPartition[%v] inactiveNodes:[%v], corruptDpIDs:[%v], lackReplicaDpIDs:[%v]", m.cluster.Name, inactiveNodes, corruptDpIDs, lackReplicaDpIDs)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Mark the volume as deleted, which will then be deleted later.
func (m *Server) markDeleteVol(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		authKey string
		err     error
		msg     string
	)

	if name, authKey, err = parseRequestToDeleteVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.markDeleteVol(name, authKey); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	if err = m.user.deleteVolPolicy(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("delete vol[%v] successfully,from[%v]", name, r.RemoteAddr)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) updateVol(w http.ResponseWriter, r *http.Request) {
	var (
		name           string
		authKey        string
		err            error
		msg            string
		capacity       uint64
		replicaNum     int
		followerRead   bool
		authenticate   bool
		zoneName       string
		description    string
		dpSelectorName string
		dpSelectorParm string
		vol            *Vol
	)

	if name, authKey, description, err = parseRequestToUpdateVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	if zoneName, capacity, replicaNum, dpSelectorName, dpSelectorParm, err =
		parseDefaultInfoToUpdateVol(r, vol); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if replicaNum != 0 && !(replicaNum == 2 || replicaNum == 3) {
		err = fmt.Errorf("replicaNum can only be 2 and 3,received replicaNum is[%v]", replicaNum)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if followerRead, authenticate, err = parseBoolFieldToUpdateVol(r, vol); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	newArgs := getVolVarargs(vol)

	newArgs.zoneName = zoneName
	newArgs.description = description
	newArgs.capacity = capacity
	newArgs.followerRead = followerRead
	newArgs.authenticate = authenticate
	newArgs.dpSelectorName = dpSelectorName
	newArgs.dpSelectorParm = dpSelectorParm

	if err = m.cluster.updateVol(name, authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("update vol[%v] successfully\n", name)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) volExpand(w http.ResponseWriter, r *http.Request) {
	var (
		name     string
		authKey  string
		err      error
		msg      string
		capacity int
		vol      *Vol
	)
	if name, authKey, capacity, err = parseRequestToSetVolCapacity(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	if uint64(capacity) <= vol.Capacity {
		err = fmt.Errorf("expand capacity[%v] should be larger than the old capacity[%v]", capacity, vol.Capacity)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	newArgs := getVolVarargs(vol)
	newArgs.capacity = uint64(capacity)

	if err = m.cluster.updateVol(name, authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("update vol[%v] successfully\n", name)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) volShrink(w http.ResponseWriter, r *http.Request) {
	var (
		name     string
		authKey  string
		err      error
		msg      string
		capacity int
		vol      *Vol
	)
	if name, authKey, capacity, err = parseRequestToSetVolCapacity(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}
	if uint64(capacity) >= vol.Capacity {
		err = fmt.Errorf("shrink capacity[%v] should be less than the old capacity[%v]", capacity, vol.Capacity)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	newArgs := getVolVarargs(vol)
	newArgs.capacity = uint64(capacity)

	if err = m.cluster.updateVol(name, authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("update vol[%v] successfully\n", name)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) createVol(w http.ResponseWriter, r *http.Request) {
	var (
		name         string
		owner        string
		err          error
		msg          string
		size         int
		mpCount      int
		dpReplicaNum int
		capacity     int
		vol          *Vol
		followerRead bool
		authenticate bool
		crossZone    bool
		defaultPriority bool
		zoneName     string
		description  string
	)

	if name, owner, zoneName, description,
				mpCount, dpReplicaNum, size,
				capacity, followerRead,
				authenticate, crossZone, defaultPriority,
				err = parseRequestToCreateVol(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !(dpReplicaNum == 2 || dpReplicaNum == 3) {
		err = fmt.Errorf("replicaNum can only be 2 and 3,received replicaNum is[%v]", dpReplicaNum)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.createVol(name, owner, zoneName, description,
					mpCount, dpReplicaNum, size, capacity,
					followerRead, authenticate, crossZone,
					defaultPriority); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	if err = m.associateVolWithUser(owner, name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("create vol[%v] successfully, has allocate [%v] data partitions", name, len(vol.dataPartitions.partitions))
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) getVolSimpleInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		name    string
		vol     *Vol
		volView *proto.SimpleVolView
	)
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	volView = newSimpleView(vol)
	sendOkReply(w, r, newSuccessHTTPReply(volView))
}

func newSimpleView(vol *Vol) *proto.SimpleVolView {
	var (
		volInodeCount  uint64
		volDentryCount uint64
	)
	for _, mp := range vol.MetaPartitions {
		volDentryCount = volDentryCount + mp.DentryCount
		volInodeCount = volInodeCount + mp.InodeCount
	}
	maxPartitionID := vol.maxPartitionID()
	return &proto.SimpleVolView{
		ID:                 vol.ID,
		Name:               vol.Name,
		Owner:              vol.Owner,
		ZoneName:           vol.zoneName,
		DpReplicaNum:       vol.dpReplicaNum,
		MpReplicaNum:       vol.mpReplicaNum,
		InodeCount:         volInodeCount,
		DentryCount:        volDentryCount,
		MaxMetaPartitionID: maxPartitionID,
		Status:             vol.Status,
		Capacity:           vol.Capacity,
		FollowerRead:       vol.FollowerRead,
		NeedToLowerReplica: vol.NeedToLowerReplica,
		Authenticate:       vol.authenticate,
		CrossZone:          vol.crossZone,
		DefaultPriority:    vol.defaultPriority,
		DomainOn:           vol.domainOn,
		RwDpCnt:            vol.dataPartitions.readableAndWritableCnt,
		MpCnt:              len(vol.MetaPartitions),
		DpCnt:              len(vol.dataPartitions.partitionMap),
		CreateTime:         time.Unix(vol.createTime, 0).Format(proto.TimeFormat),
		Description:        vol.description,
		DpSelectorName:     vol.dpSelectorName,
		DpSelectorParm:     vol.dpSelectorParm,
		DefaultZonePrior:   vol.defaultPriority,
	}
}

func checkIp(addr string) bool {
	var arr []string
	if arr = strings.Split(addr, ":"); len(arr) < 2 {
		return false
	}
	if id, err := strconv.ParseUint(arr[1], 10, 64);err != nil || id > 65535 || id < 1024 {
		return false
	}
	ip := strings.Trim(addr, " ")
	regStr := `^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.)(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){2}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])`
	if match, _ := regexp.MatchString(regStr, ip); match {
		return true
	}
	return false
}

func (m *Server) addDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr  string
		zoneName  string
		id        uint64
		err       error
		nodesetId uint64
	)
	if nodeAddr, zoneName, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !checkIp(nodeAddr) {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("addr not legal").Error()})
		return
	}
	var value string
	if value = r.FormValue(idKey); value == "" {
		nodesetId = 0
	} else {
		if nodesetId, err = strconv.ParseUint(value, 10, 64); err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
	}
	if id, err = m.cluster.addDataNode(nodeAddr, zoneName, nodesetId); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) getDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr     string
		dataNode     *DataNode
		dataNodeInfo *proto.DataNodeInfo
		err          error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if dataNode, err = m.cluster.dataNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	dataNode.PersistenceDataPartitions = m.cluster.getAllDataPartitionIDByDatanode(nodeAddr)

	dataNodeInfo = &proto.DataNodeInfo{
		Total:                     dataNode.Total,
		Used:                      dataNode.Used,
		AvailableSpace:            dataNode.AvailableSpace,
		ID:                        dataNode.ID,
		ZoneName:                  dataNode.ZoneName,
		Addr:                      dataNode.Addr,
		ReportTime:                dataNode.ReportTime,
		IsActive:                  dataNode.isActive,
		IsWriteAble:               dataNode.isWriteAble(),
		UsageRatio:                dataNode.UsageRatio,
		SelectedTimes:             dataNode.SelectedTimes,
		Carry:                     dataNode.Carry,
		DataPartitionReports:      dataNode.DataPartitionReports,
		DataPartitionCount:        dataNode.DataPartitionCount,
		NodeSetID:                 dataNode.NodeSetID,
		PersistenceDataPartitions: dataNode.PersistenceDataPartitions,
		BadDisks:                  dataNode.BadDisks,
	}

	sendOkReply(w, r, newSuccessHTTPReply(dataNodeInfo))
}

// Decommission a data node. This will decommission all the data partition on that node.
func (m *Server) decommissionDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		node        *DataNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	if err = m.cluster.decommissionDataNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommission data node [%v] successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) setNodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
	if params, err = parseAndExtractSetNodeInfoParams(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if batchCount, ok := params[nodeDeleteBatchCountKey]; ok {
		if bc, ok := batchCount.(uint64); ok {
			if err = m.cluster.setMetaNodeDeleteBatchCount(bc); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}
	if val, ok := params[nodeMarkDeleteRateKey]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setDataNodeDeleteLimitRate(v); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[nodeAutoRepairRateKey]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setDataNodeAutoRepairLimitRate(v); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}

	if val, ok := params[nodeDeleteWorkerSleepMs]; ok {
		if v, ok := val.(uint64); ok {
			if err = m.cluster.setMetaNodeDeleteWorkerSleepMs(v); err != nil {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodeinfo params %v successfully", params)))

}
func (m *Server) updateDataUseRatio(ratio float64) (err error) {
	m.cluster.nodeSetGrpManager.Lock()
	defer m.cluster.nodeSetGrpManager.Unlock()

	m.cluster.nodeSetGrpManager.dataRatioLimit = ratio
	err = m.cluster.putZoneDomain(false)
	return
}
func (m *Server) updateExcludeZoneUseRatio(ratio float64) (err error) {
	m.cluster.nodeSetGrpManager.Lock()
	defer m.cluster.nodeSetGrpManager.Unlock()

	m.cluster.nodeSetGrpManager.excludeZoneUseRatio = ratio
	err = m.cluster.putZoneDomain(false)
	return
}
func (m *Server) updateNodesetId(zoneName string, destNodesetId uint64, nodeType uint64, addr string) (err error) {
	var (
		nsId     uint64
		dstNs    *nodeSet
		srcNs    *nodeSet
		ok       bool
		value    interface{}
		metaNode *MetaNode
		dataNode *DataNode
	)
	defer func() {
		log.LogInfof("action[updateNodesetId] step out")
	}()
	log.LogWarnf("action[updateNodesetId] zonename[%v] destNodesetId[%v] nodeType[%v] addr[%v]",
			zoneName, destNodesetId, nodeType, addr)

	if value, ok = m.cluster.t.zoneMap.Load(zoneName); !ok {
		return fmt.Errorf("zonename [%v] not found", zoneName)
	}
	zone := value.(*Zone)
	if dstNs, ok = zone.nodeSetMap[destNodesetId]; !ok {
		return fmt.Errorf("%v destNodesetId not found", destNodesetId)
	}
	if uint32(nodeType) == TypeDataPartion {
		value, ok = zone.dataNodes.Load(addr)
		if !ok {
			return fmt.Errorf("addr %v not found", addr)
		}
		nsId = value.(*DataNode).NodeSetID
	} else if uint32(nodeType) == TypeMetaPartion {
		value, ok = zone.metaNodes.Load(addr)
		if !ok {
			return fmt.Errorf("addr %v not found", addr)
		}
		nsId = value.(*MetaNode).NodeSetID
	} else {
		return fmt.Errorf("%v wrong type", nodeType)
	}
	log.LogInfof("action[updateNodesetId] zonename[%v] destNodesetId[%v] nodeType[%v] addr[%v] get destid[%v]",
		zoneName, destNodesetId, nodeType, addr, dstNs.ID)
	srcNs = zone.nodeSetMap[nsId]
	if srcNs.ID == dstNs.ID {
		return fmt.Errorf("addr belong to same nodeset")
	} else if srcNs.ID < dstNs.ID {
		// take parallel call updatenodeid and defer unlock order into consider
		srcNs.Lock()
		dstNs.Lock()
		defer srcNs.Unlock()
		defer dstNs.Unlock()
	} else {
		// take parallel call updatenodeid and defer unlock order into consider
		dstNs.Lock()
		srcNs.Lock()
		defer dstNs.Unlock()
		defer srcNs.Unlock()
	}

	// the nodeset capcity not enlarged if node be added,capacity can be adjust by
	// AdminUpdateNodeSetCapcity
	if uint32(nodeType) == TypeDataPartion {
		if value, ok = srcNs.dataNodes.Load(addr); !ok {
			return fmt.Errorf("addr not found in srcNs.dataNodes")
		}
		dataNode = value.(*DataNode)
		dataNode.NodeSetID = dstNs.ID
		dstNs.putDataNode(dataNode)
		srcNs.deleteDataNode(dataNode)
		if err = m.cluster.syncUpdateDataNode(dataNode); err != nil {
			dataNode.NodeSetID = srcNs.ID
			return
		}
	} else {
		if value, ok = srcNs.metaNodes.Load(addr); !ok {
			return fmt.Errorf("ddr not found in srcNs.metaNodes")
		}
		metaNode = value.(*MetaNode)
		metaNode.NodeSetID = dstNs.ID
		dstNs.putMetaNode(metaNode)
		srcNs.deleteMetaNode(metaNode)
		if err = m.cluster.syncUpdateMetaNode(metaNode); err != nil {
			dataNode.NodeSetID = srcNs.ID
			return
		}
	}
	if err = m.cluster.syncUpdateNodeSet(dstNs); err != nil {
		return fmt.Errorf("warn:syncUpdateNodeSet dst srcNs [%v] failed", dstNs.ID)
	}
	if err = m.cluster.syncUpdateNodeSet(srcNs); err != nil {
		return fmt.Errorf("warn:syncUpdateNodeSet src srcNs [%v] failed", srcNs.ID)
	}

	return
}

func (m *Server) updateNodesetCapcity(zoneName string, nodesetId uint64, capcity int) (err error) {
	var ns  *nodeSet
	var ok bool
	var value interface{}
	if capcity < defaultReplicaNum || capcity > 100 {
		err = fmt.Errorf("capcity [%v] value out of scope", capcity)
		return
	}
	if value, ok = m.cluster.t.zoneMap.Load(zoneName); !ok {
		err = fmt.Errorf("zonename [%v] not found", zoneName)
		return
	}
	zone := value.(*Zone)
	if ns, ok = zone.nodeSetMap[nodesetId]; !ok {
		err = fmt.Errorf("nodesetId [%v] not found", nodesetId)
		return
	}
	ns.Lock()
	defer ns.Unlock()

	ns.Capacity = capcity
	m.cluster.syncUpdateNodeSet(ns)
	return
}

func (m *Server) buildNodeSetGrpInfoByID(id uint64) (*proto.SimpleNodeSetGrpInfo, error) {
	nsgm := m.cluster.nodeSetGrpManager
	var index int
	for index = 0; index < len(nsgm.nodeSetGrpMap); index++ {
		if nsgm.nodeSetGrpMap[index].ID == id {
			break
		}
		if nsgm.nodeSetGrpMap[index].ID > id {
			return nil, fmt.Errorf("id not found")
		}
	}
	if index == len(nsgm.nodeSetGrpMap) {
		return nil, fmt.Errorf("id not found")
	}
	return m.buildNodeSetGrpInfo(index), nil
}

func (m *Server) buildNodeSetGrpInfo(index int) *proto.SimpleNodeSetGrpInfo {
	nsgm := m.cluster.nodeSetGrpManager
	nsg := nsgm.nodeSetGrpMap[index]
	nsgStat := new(proto.SimpleNodeSetGrpInfo)
	nsgStat.ID = nsg.ID
	nsgStat.Status = nsg.status
	for i := 0; i < len(nsg.nodeSets); i++ {
		var nsStat proto.NodeSetInfo
		nsStat.ID = nsg.nodeSets[i].ID
		nsStat.Capacity = nsg.nodeSets[i].Capacity
		nsStat.ZoneName = nsg.nodeSets[i].zoneName
		nsg.nodeSets[i].dataNodes.Range(func(key, value interface{}) bool {
			node := value.(*DataNode)
			nsStat.DataTotal += node.Total
			if node.isWriteAble() {
				nsStat.DataUsed += node.Used
			} else {
				nsStat.DataUsed += node.Total
			}
			log.LogInfof("nodeset index[%v], datanode nodeset id[%v],zonename[%v], addr[%v] inner nodesetid[%v]",
				i, nsStat.ID, node.ZoneName, node.Addr, node.NodeSetID)

			dataNodeInfo := &proto.DataNodeInfo{
				Total:                     node.Total,
				Used:                      node.Used,
				AvailableSpace:            node.AvailableSpace,
				ID:                        node.ID,
				ZoneName:                  node.ZoneName,
				Addr:                      node.Addr,
				ReportTime:                node.ReportTime,
				IsActive:                  node.isActive,
				IsWriteAble:               node.isWriteAble(),
				UsageRatio:                node.UsageRatio,
				SelectedTimes:             node.SelectedTimes,
				Carry:                     node.Carry,
				DataPartitionCount:        node.DataPartitionCount,
				NodeSetID:                 node.NodeSetID,
			}
			nsStat.DataNodes = append(nsStat.DataNodes, dataNodeInfo)
			return true
		})
		nsStat.DataUseRatio , _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(nsStat.DataUsed) / float64(nsStat.DataTotal)), 64)

		nsg.nodeSets[i].metaNodes.Range(func(key, value interface{}) bool {
			node := value.(*MetaNode)
			nsStat.MetaTotal += node.Total
			nsStat.MetaUsed += node.Used
			log.LogInfof("nodeset index[%v], metanode nodeset id[%v],zonename[%v], addr[%v] inner nodesetid[%v]",
				i, nsStat.ID, node.ZoneName, node.Addr, node.NodeSetID)

			metaNodeInfo := &proto.MetaNodeInfo{
				ID:                        node.ID,
				Addr:                      node.Addr,
				IsActive:                  node.IsActive,
				IsWriteAble:               node.isWritable(),
				ZoneName:                  node.ZoneName,
				MaxMemAvailWeight:         node.MaxMemAvailWeight,
				Total:                     node.Total,
				Used:                      node.Used,
				Ratio:                     node.Ratio,
				SelectCount:               node.SelectCount,
				Carry:                     node.Carry,
				Threshold:                 node.Threshold,
				ReportTime:                node.ReportTime,
				MetaPartitionCount:        node.MetaPartitionCount,
				NodeSetID:                 node.NodeSetID,
			}


			nsStat.MetaNodes = append(nsStat.MetaNodes, metaNodeInfo)
			return true
		})
		nsStat.MetaUseRatio, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(nsStat.MetaUsed) / float64(nsStat.MetaTotal)), 64)
		nsgStat.NodeSetInfo = append(nsgStat.NodeSetInfo, nsStat)
		log.LogInfof("nodeset index[%v], nodeset id[%v],capacity[%v], datatotal[%v] dataused[%v] metatotal[%v] metaused[%v], metanode[%v], datanodes[%v]",
				i, nsStat.ID, nsStat.Capacity, nsStat.DataTotal, nsStat.DataUsed, nsStat.MetaTotal, nsStat.MetaUsed, nsStat.MetaNodes, nsStat.DataNodes)
	}
	return nsgStat
}
func (m *Server) updateNodeSetCapacityHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
	if params, err = parseAndExtractSetNodeSetInfoParams(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if  err:= m.updateNodesetCapcity(params[zoneNameKey].(string), params[idKey].(uint64), params[countKey].(int)); err == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodesetinfo params %v successfully", params)))
}

func (m *Server) updateDataUseRatioHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
	var value string
	if value = r.FormValue(ratio); value == "" {
		err = keyNotFound(ratio)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var ratioVal float64
	if ratioVal, err = strconv.ParseFloat(value, 64); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if ratioVal == 0 || ratioVal > 1{
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if  err = m.updateDataUseRatio(ratioVal); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodesetinfo params %v successfully", params)))
}

func (m *Server) updateZoneExcludeRatioHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params map[string]interface{}
		err    error
	)
	var value string
	if value = r.FormValue(ratio); value == "" {
		err = keyNotFound(ratio)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	var ratioVal float64
	if ratioVal, err = strconv.ParseFloat(value, 64); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if  err = m.updateExcludeZoneUseRatio(ratioVal); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set nodesetinfo params %v successfully", params)))
}

func (m *Server) updateNodeSetIdHandler(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr     string
		id           uint64
		zoneName     string
		err          error
		nodeType     uint64
		value        string
	)
	defer func() {
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
	}()
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if id, err = extractNodeID(r); err != nil {
		return
	}
	if value = r.FormValue(nodeTypeKey); value == "" {
		err = fmt.Errorf("need param nodeType")
		return
	}

	if nodeType, err = strconv.ParseUint(value, 10, 64); err != nil {
		return
	}

	if  err = m.updateNodesetId(zoneName, id, nodeType, nodeAddr); err != nil {
		return
	}

	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("update node setid successfully")))
}

// get metanode some interval params
func (m *Server) getNodeSetGrpInfoHandler(w http.ResponseWriter, r *http.Request) {
	var err    error
	if err = r.ParseForm(); err != nil {
		sendOkReply(w, r, newErrHTTPReply(err))
	}
	var value string
	var id uint64
	if value = r.FormValue(idKey); value != "" {
		id, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
	}
	log.LogInfof("action[getNodeSetGrpInfoHandler] id [%v]", id)
	var info *proto.SimpleNodeSetGrpInfo
	if info, err = m.buildNodeSetGrpInfoByID(id); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
	sendOkReply(w, r, newSuccessHTTPReply(info))
}
func (m *Server) getIsDomainOn(w http.ResponseWriter, r *http.Request) {
	type SimpleDomainInfo struct {
		DomainOn       bool
	}
	nsglStat := new(SimpleDomainInfo)
	nsglStat.DomainOn = m.cluster.FaultDomain
	sendOkReply(w, r, newSuccessHTTPReply(nsglStat))
}

// get metanode some interval params
func (m *Server) getAllNodeSetGrpInfoHandler(w http.ResponseWriter, r *http.Request) {
	var i int
	nsgm := m.cluster.nodeSetGrpManager
	nsglStat := new(proto.SimpleNodeSetGrpInfoList)
	nsglStat.DomainOn = m.cluster.FaultDomain
	nsglStat.NeedDomain = m.cluster.needFaultDomain
	nsglStat.DataRatioLimit = nsgm.dataRatioLimit
	nsglStat.ZoneExcludeRatioLimit = nsgm.excludeZoneUseRatio
	nsglStat.Status = nsgm.status
	nsglStat.ExcludeZones = nsgm.c.t.domainExcludeZones
	for i = 0; i < len(nsgm.nodeSetGrpMap) ; i++ {
		log.LogInfof("action[getAllNodeSetGrpInfoHandler] index [%v],id [%v],Print inner nodeset now!", i, nsgm.nodeSetGrpMap[i].ID)
		nsglStat.SimpleNodeSetGrpInfo = append(nsglStat.SimpleNodeSetGrpInfo, m.buildNodeSetGrpInfo(i))
	}
	sendOkReply(w, r, newSuccessHTTPReply(nsglStat))
}

// get metanode some interval params
func (m *Server) getNodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	resp := make(map[string]string)
	resp[nodeDeleteBatchCountKey] = fmt.Sprintf("%v", m.cluster.cfg.MetaNodeDeleteBatchCount)
	resp[nodeMarkDeleteRateKey] = fmt.Sprintf("%v", m.cluster.cfg.DataNodeDeleteLimitRate)
	resp[nodeDeleteWorkerSleepMs] = fmt.Sprintf("%v", m.cluster.cfg.MetaNodeDeleteWorkerSleepMs)
	resp[nodeAutoRepairRateKey] = fmt.Sprintf("%v", m.cluster.cfg.DataNodeAutoRepairLimitRate)

	sendOkReply(w, r, newSuccessHTTPReply(resp))
}

func (m *Server) diagnoseMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err               error
		rstMsg            *proto.MetaPartitionDiagnosis
		inactiveNodes     []string
		corruptMps        []*MetaPartition
		lackReplicaMps    []*MetaPartition
		corruptMpIDs      []uint64
		lackReplicaMpIDs  []uint64
		badMetaPartitions []badPartitionView
	)
	corruptMpIDs = make([]uint64, 0)
	lackReplicaMpIDs = make([]uint64, 0)
	if inactiveNodes, corruptMps, err = m.cluster.checkCorruptMetaPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}

	if lackReplicaMps, err = m.cluster.checkLackReplicaMetaPartitions(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
	}
	for _, mp := range corruptMps {
		corruptMpIDs = append(corruptMpIDs, mp.PartitionID)
	}
	for _, mp := range lackReplicaMps {
		lackReplicaMpIDs = append(lackReplicaMpIDs, mp.PartitionID)
	}
	badMetaPartitions = m.cluster.getBadMetaPartitionsView()
	rstMsg = &proto.MetaPartitionDiagnosis{
		InactiveMetaNodes:           inactiveNodes,
		CorruptMetaPartitionIDs:     corruptMpIDs,
		LackReplicaMetaPartitionIDs: lackReplicaMpIDs,
		BadMetaPartitionIDs:         badMetaPartitions,
	}
	log.LogInfof("diagnose metaPartition[%v] inactiveNodes:[%v], corruptMpIDs:[%v], lackReplicaMpIDs:[%v]", m.cluster.Name, inactiveNodes, corruptMpIDs, lackReplicaMpIDs)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// Decommission a disk. This will decommission all the data partitions on this disk.
func (m *Server) decommissionDisk(w http.ResponseWriter, r *http.Request) {
	var (
		node                  *DataNode
		rstMsg                string
		offLineAddr, diskPath string
		err                   error
		badPartitionIds       []uint64
		badPartitions         []*DataPartition
	)

	if offLineAddr, diskPath, err = parseRequestToDecommissionNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.dataNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	badPartitions = node.badPartitions(diskPath, m.cluster)
	if len(badPartitions) == 0 {
		rstMsg = fmt.Sprintf("receive decommissionDisk node[%v] no any partitions on disk[%v],offline successfully",
			node.Addr, diskPath)
		sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
		return
	}
	for _, bdp := range badPartitions {
		badPartitionIds = append(badPartitionIds, bdp.PartitionID)
	}
	rstMsg = fmt.Sprintf("receive decommissionDisk node[%v] disk[%v], badPartitionIds[%v] has offline successfully",
		node.Addr, diskPath, badPartitionIds)
	if err = m.cluster.decommissionDisk(node, diskPath, badPartitions); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	Warn(m.clusterName, rstMsg)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

// handle tasks such as heartbeat，loadDataPartition，deleteDataPartition, etc.
func (m *Server) handleDataNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	tr, err := parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleDataNodeTaskResponse(tr.OperatorAddr, tr)
}

func (m *Server) addMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		zoneName string
		id       uint64
		err      error
		nodesetId uint64
	)
	if nodeAddr, zoneName, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if !checkIp(nodeAddr) {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("addr not legal").Error()})
		return
	}
	var value string
	if value = r.FormValue(idKey); value == "" {
		nodesetId = 0
	} else {
		if nodesetId, err = strconv.ParseUint(value, 10, 64); err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		}
	}
	if id, err = m.cluster.addMetaNode(nodeAddr, zoneName, nodesetId); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) checkInvalidIDNodes(w http.ResponseWriter, r *http.Request) {
	nodes := m.cluster.getInvalidIDNodes()
	sendOkReply(w, r, newSuccessHTTPReply(nodes))
}

func (m *Server) updateDataNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, id, err = parseRequestForUpdateMetaNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.updateDataNodeBaseInfo(nodeAddr, id); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) updateMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, id, err = parseRequestForUpdateMetaNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.updateMetaNodeBaseInfo(nodeAddr, id); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) getMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr     string
		metaNode     *MetaNode
		metaNodeInfo *proto.MetaNodeInfo
		err          error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if metaNode, err = m.cluster.metaNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}
	metaNode.PersistenceMetaPartitions = m.cluster.getAllMetaPartitionIDByMetaNode(nodeAddr)
	metaNodeInfo = &proto.MetaNodeInfo{
		ID:                        metaNode.ID,
		Addr:                      metaNode.Addr,
		IsActive:                  metaNode.IsActive,
		IsWriteAble:               metaNode.isWritable(),
		ZoneName:                  metaNode.ZoneName,
		MaxMemAvailWeight:         metaNode.MaxMemAvailWeight,
		Total:                     metaNode.Total,
		Used:                      metaNode.Used,
		Ratio:                     metaNode.Ratio,
		SelectCount:               metaNode.SelectCount,
		Carry:                     metaNode.Carry,
		Threshold:                 metaNode.Threshold,
		ReportTime:                metaNode.ReportTime,
		MetaPartitionCount:        metaNode.MetaPartitionCount,
		NodeSetID:                 metaNode.NodeSetID,
		PersistenceMetaPartitions: metaNode.PersistenceMetaPartitions,
	}
	sendOkReply(w, r, newSuccessHTTPReply(metaNodeInfo))
}

func (m *Server) decommissionMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		nodeAddr    string
		mp          *MetaPartition
		msg         string
		err         error
	)
	if partitionID, nodeAddr, err = parseRequestToDecommissionMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}
	if err = m.cluster.decommissionMetaPartition(nodeAddr, mp); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf(proto.AdminDecommissionMetaPartition+" partitionID :%v  decommissionMetaPartition successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) loadMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		msg         string
		mp          *MetaPartition
		partitionID uint64
		err         error
	)

	if partitionID, err = parseRequestToLoadMetaPartition(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	m.cluster.loadMetaPartitionAndCheckResponse(mp)
	msg = fmt.Sprintf(proto.AdminLoadMetaPartition+" partitionID :%v Load successfully", partitionID)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) decommissionMetaNode(w http.ResponseWriter, r *http.Request) {
	var (
		metaNode    *MetaNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if metaNode, err = m.cluster.metaNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaNodeNotExists))
		return
	}
	if err = m.cluster.decommissionMetaNode(metaNode); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommissionMetaNode metaNode [%v] has offline successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) handleMetaNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	tr, err := parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleMetaNodeTaskResponse(tr.OperatorAddr, tr)
}

// Dynamically add a raft node (replica) for the master.
// By using this function, there is no need to stop all the master services. Adding a new raft node is performed online.
func (m *Server) addRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRequestForRaftNode(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = m.cluster.addRaftNode(id, addr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("add  raft node id :%v, addr:%v successfully \n", id, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// Dynamically remove a master node. Similar to addRaftNode, this operation is performed online.
func (m *Server) removeRaftNode(w http.ResponseWriter, r *http.Request) {
	var msg string
	id, addr, err := parseRequestForRaftNode(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	err = m.cluster.removeRaftNode(id, addr)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg = fmt.Sprintf("remove  raft node id :%v,adr:%v successfully\n", id, addr)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

// Parse the request that adds/deletes a raft node.
func parseRequestForRaftNode(r *http.Request) (id uint64, host string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var idStr string
	if idStr = r.FormValue(idKey); idStr == "" {
		err = keyNotFound(idKey)
		return
	}

	if id, err = strconv.ParseUint(idStr, 10, 64); err != nil {
		return
	}
	if host = r.FormValue(addrKey); host == "" {
		err = keyNotFound(addrKey)
		return
	}

	if arr := strings.Split(host, colonSplit); len(arr) < 2 {
		err = unmatchedKey(addrKey)
		return
	}
	return
}

func parseRequestForUpdateMetaNode(r *http.Request) (nodeAddr string, id uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if id, err = extractNodeID(r); err != nil {
		return
	}
	return
}

func parseRequestForAddNode(r *http.Request) (nodeAddr, zoneName string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}
	return
}

func parseAndExtractNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractNodeAddr(r)
}

func parseRequestToDecommissionNode(r *http.Request) (nodeAddr, diskPath string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	nodeAddr, err = extractNodeAddr(r)
	if err != nil {
		return
	}
	diskPath, err = extractDiskPath(r)
	return
}

func parseRequestToGetTaskResponse(r *http.Request) (tr *proto.AdminTask, err error) {
	var body []byte
	if err = r.ParseForm(); err != nil {
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		return
	}
	tr = &proto.AdminTask{}
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(body)))
	decoder.UseNumber()
	err = decoder.Decode(tr)
	return
}

func parseVolName(r *http.Request) (name string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	return
}

type getVolParameter struct {
	name                string
	authKey             string
	skipOwnerValidation bool
}

func parseGetVolParameter(r *http.Request) (p *getVolParameter, err error) {
	p = &getVolParameter{}
	skipOwnerValidationVal := r.Header.Get(proto.SkipOwnerValidation)
	if len(skipOwnerValidationVal) > 0 {
		if p.skipOwnerValidation, err = strconv.ParseBool(skipOwnerValidationVal); err != nil {
			return
		}
	}
	if p.name = r.FormValue(nameKey); p.name == "" {
		err = keyNotFound(nameKey)
		return
	}
	if !volNameRegexp.MatchString(p.name) {
		err = errors.New("name can only be number and letters")
		return
	}
	if p.authKey = r.FormValue(volAuthKey); !p.skipOwnerValidation && len(p.authKey) == 0 {
		err = keyNotFound(volAuthKey)
		return
	}
	return
}

func parseVolNameAndAuthKey(r *http.Request) (name, authKey string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	return

}

func parseRequestToDeleteVol(r *http.Request) (name, authKey string, err error) {
	return parseVolNameAndAuthKey(r)

}

func parseRequestToUpdateVol(r *http.Request) (name, authKey, description string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	description = r.FormValue(descriptionKey)
	return
}

func parseDefaultInfoToUpdateVol(r *http.Request, vol *Vol) (zoneName string, capacity uint64, replicaNum int,
	dpSelectorName string, dpSelectorParm string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = vol.zoneName
	}
	if capacityStr := r.FormValue(volCapacityKey); capacityStr != "" {
		var capacityInt int
		if capacityInt, err = strconv.Atoi(capacityStr); err != nil {
			err = unmatchedKey(volCapacityKey)
			return
		}
		capacity = uint64(capacityInt)
	} else {
		capacity = vol.Capacity
	}
	if replicaNumStr := r.FormValue(replicaNumKey); replicaNumStr != "" {
		if replicaNum, err = strconv.Atoi(replicaNumStr); err != nil {
			err = unmatchedKey(replicaNumKey)
			return
		}
	} else {
		replicaNum = int(vol.dpReplicaNum)
	}
	dpSelectorName = r.FormValue(dpSelectorNameKey)
	dpSelectorParm = r.FormValue(dpSelectorParmKey)
	if (dpSelectorName == "") || (dpSelectorParm == "") {
		if (dpSelectorName != "") || (dpSelectorParm != "") {
			err = keyNotFound(dpSelectorNameKey + " or " + dpSelectorParmKey)
			return
		}
		dpSelectorName = vol.dpSelectorName
		dpSelectorParm = vol.dpSelectorParm
	}
	return
}

func parseBoolFieldToUpdateVol(r *http.Request, vol *Vol) (followerRead, authenticate bool, err error) {
	if followerReadStr := r.FormValue(followerReadKey); followerReadStr != "" {
		if followerRead, err = strconv.ParseBool(followerReadStr); err != nil {
			err = unmatchedKey(followerReadKey)
			return
		}
	} else {
		followerRead = vol.FollowerRead
	}
	if authenticateStr := r.FormValue(authenticateKey); authenticateStr != "" {
		if authenticate, err = strconv.ParseBool(authenticateStr); err != nil {
			err = unmatchedKey(authenticateKey)
			return
		}
	} else {
		authenticate = vol.authenticate
	}
	return
}

func parseRequestToSetVolCapacity(r *http.Request) (name, authKey string, capacity int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	if capacity, err = extractCapacity(r); err != nil {
		return
	}
	return
}

func parseRequestToCreateVol(r *http.Request) (name, owner, zoneName, description string,
		mpCount, dpReplicaNum, size,
		capacity int, followerRead,
		authenticate, crossZone, defaultPriority bool,
		err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if owner, err = extractOwner(r); err != nil {
		return
	}

	if mpCountStr := r.FormValue(metaPartitionCountKey); mpCountStr != "" {
		if mpCount, err = strconv.Atoi(mpCountStr); err != nil {
			mpCount = defaultInitMetaPartitionCount
		}
	}

	if replicaStr := r.FormValue(replicaNumKey); replicaStr == "" {
		dpReplicaNum = defaultReplicaNum
	} else if dpReplicaNum, err = strconv.Atoi(replicaStr); err != nil {
		err = unmatchedKey(replicaNumKey)
		return
	}

	if sizeStr := r.FormValue(dataPartitionSizeKey); sizeStr != "" {
		if size, err = strconv.Atoi(sizeStr); err != nil {
			err = unmatchedKey(dataPartitionSizeKey)
			return
		}
	}

	if capacity, err = extractCapacity(r); err != nil {
		return
	}

	if followerRead, err = extractFollowerRead(r); err != nil {
		return
	}

	if authenticate, err = extractAuthenticate(r); err != nil {
		return
	}

	if crossZone, err = extractCrossZone(r); err != nil {
		return
	}
	if defaultPriority, err = extractDefaulPriority(r); err != nil {
		return
	}

	zoneName = r.FormValue(zoneNameKey)
	description = r.FormValue(descriptionKey)
	return
}

func parseRequestToCreateDataPartition(r *http.Request) (count int, name string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if countStr := r.FormValue(countKey); countStr == "" {
		err = keyNotFound(countKey)
		return
	} else if count, err = strconv.Atoi(countStr); err != nil || count == 0 {
		err = unmatchedKey(countKey)
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	return
}

func parseRequestToGetDataPartition(r *http.Request) (ID uint64, volName string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	volName = r.FormValue(nameKey)
	return
}

func parseRequestToLoadDataPartition(r *http.Request) (ID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	return
}

func parseRequestToAddMetaReplica(r *http.Request) (ID uint64, addr string, err error) {
	return extractMetaPartitionIDAndAddr(r)
}

func parseRequestToRemoveMetaReplica(r *http.Request) (ID uint64, addr string, err error) {
	return extractMetaPartitionIDAndAddr(r)
}

func extractMetaPartitionIDAndAddr(r *http.Request) (ID uint64, addr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	if addr, err = extractNodeAddr(r); err != nil {
		return
	}
	return
}

func parseRequestToAddDataReplica(r *http.Request) (ID uint64, addr string, err error) {
	return extractDataPartitionIDAndAddr(r)
}

func parseRequestToRemoveDataReplica(r *http.Request) (ID uint64, addr string, err error) {
	return extractDataPartitionIDAndAddr(r)
}

func extractDataPartitionIDAndAddr(r *http.Request) (ID uint64, addr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	if addr, err = extractNodeAddr(r); err != nil {
		return
	}
	return
}

func extractDataPartitionID(r *http.Request) (ID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseRequestToDecommissionDataPartition(r *http.Request) (ID uint64, nodeAddr string, err error) {
	return extractDataPartitionIDAndAddr(r)
}

func extractNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if nodeAddr = r.FormValue(addrKey); nodeAddr == "" {
		err = keyNotFound(addrKey)
		return
	}
	return
}

func extractNodeID(r *http.Request) (ID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func extractDiskPath(r *http.Request) (diskPath string, err error) {
	if diskPath = r.FormValue(diskPathKey); diskPath == "" {
		err = keyNotFound(diskPathKey)
		return
	}
	return
}

func parseRequestToLoadMetaPartition(r *http.Request) (partitionID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	return
}

func parseRequestToDecommissionMetaPartition(r *http.Request) (partitionID uint64, nodeAddr string, err error) {
	return extractMetaPartitionIDAndAddr(r)
}

func parseAndExtractStatus(r *http.Request) (status bool, err error) {

	if err = r.ParseForm(); err != nil {
		return
	}
	return extractStatus(r)
}

func extractStatus(r *http.Request) (status bool, err error) {
	var value string
	if value = r.FormValue(enableKey); value == "" {
		err = keyNotFound(enableKey)
		return
	}
	if status, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractFollowerRead(r *http.Request) (followerRead bool, err error) {
	var value string
	if value = r.FormValue(followerReadKey); value == "" {
		followerRead = false
		return
	}
	if followerRead, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractAuthenticate(r *http.Request) (authenticate bool, err error) {
	var value string
	if value = r.FormValue(authenticateKey); value == "" {
		authenticate = false
		return
	}
	if authenticate, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractCrossZone(r *http.Request) (crossZone bool, err error) {
	var value string
	if value = r.FormValue(crossZoneKey); value == "" {
		crossZone = false
		return
	}
	if crossZone, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractDefaulPriority(r *http.Request) (defaultPrior bool, err error) {
	var value string
	if value = r.FormValue(defaultPriority); value == "" {
		defaultPrior = false
		return
	}
	if defaultPrior, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}


func parseAndExtractThreshold(r *http.Request) (threshold float64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue(thresholdKey); value == "" {
		err = keyNotFound(thresholdKey)
		return
	}
	if threshold, err = strconv.ParseFloat(value, 64); err != nil {
		return
	}
	return
}
func parseAndExtractSetNodeSetInfoParams(r *http.Request) (params map[string]interface{}, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	params = make(map[string]interface{})
	if value = r.FormValue(countKey); value != "" {
		var count = uint64(0)
		count, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(countKey)
			return
		}
		params[countKey] = count
	}
	var zoneName string
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}
	if value = r.FormValue(idKey); value != "" {
		var nodesetId = uint64(0)
		nodesetId, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(idKey)
			return
		}
		params[idKey] = nodesetId
	}
	return
}
func parseAndExtractSetNodeInfoParams(r *http.Request) (params map[string]interface{}, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	noParams := true
	params = make(map[string]interface{})
	if value = r.FormValue(nodeDeleteBatchCountKey); value != "" {
		noParams = false
		var batchCount = uint64(0)
		batchCount, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeDeleteBatchCountKey)
			return
		}
		params[nodeDeleteBatchCountKey] = batchCount
	}
	if value = r.FormValue(nodeMarkDeleteRateKey); value != "" {
		noParams = false
		var val = uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeMarkDeleteRateKey)
			return
		}
		params[nodeMarkDeleteRateKey] = val
	}

	if value = r.FormValue(nodeAutoRepairRateKey); value != "" {
		noParams = false
		var val = uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeAutoRepairRateKey)
			return
		}
		params[nodeAutoRepairRateKey] = val
	}

	if value = r.FormValue(nodeDeleteWorkerSleepMs); value != "" {
		noParams = false
		var val = uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeMarkDeleteRateKey)
			return
		}
		params[nodeDeleteWorkerSleepMs] = val
	}
	if noParams {
		err = keyNotFound(nodeDeleteBatchCountKey)
		return
	}
	return
}

func validateRequestToCreateMetaPartition(r *http.Request) (volName string, start uint64, err error) {
	if volName, err = extractName(r); err != nil {
		return
	}

	var value string
	if value = r.FormValue(startKey); value == "" {
		err = keyNotFound(startKey)
		return
	}
	start, err = strconv.ParseUint(value, 10, 64)
	return
}

func newSuccessHTTPReply(data interface{}) *proto.HTTPReply {
	return &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: proto.ErrSuc.Error(), Data: data}
}

func newErrHTTPReply(err error) *proto.HTTPReply {
	if err == nil {
		return newSuccessHTTPReply("")
	}
	code, ok := proto.Err2CodeMap[err]
	if ok {
		return &proto.HTTPReply{Code: code, Msg: err.Error()}
	}
	return &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()}
}

func sendOkReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) (err error) {
	switch httpReply.Data.(type) {
	case *DataPartition:
		dp := httpReply.Data.(*DataPartition)
		dp.RLock()
		defer dp.RUnlock()
	case *MetaPartition:
		mp := httpReply.Data.(*MetaPartition)
		mp.RLock()
		defer mp.RUnlock()
	case *MetaNode:
		mn := httpReply.Data.(*MetaNode)
		mn.RLock()
		defer mn.RUnlock()
	case *DataNode:
		dn := httpReply.Data.(*DataNode)
		dn.RLock()
		defer dn.RUnlock()
	}
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	send(w, r, reply)
	return
}

func send(w http.ResponseWriter, r *http.Request, reply []byte) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
		return
	}
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	return
}

func sendErrReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	log.LogInfof("URL[%v],remoteAddr[%v],response err[%v]", r.URL, r.RemoteAddr, httpReply)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}
	return
}

func (m *Server) getMetaPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		vol  *Vol
		err  error
	)
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	mpsCache := vol.getMpsCache()
	if len(mpsCache) == 0 {
		vol.updateViewCache(m.cluster)
		mpsCache = vol.getMpsCache()
	}
	send(w, r, mpsCache)
	return
}

// Obtain all the data partitions in a volume.
func (m *Server) getDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		name string
		vol  *Vol
		err  error
	)
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if body, err = vol.getDataPartitionsView(); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	send(w, r, body)
}

func (m *Server) getVol(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		vol     *Vol
		message string
		jobj    proto.APIAccessReq
		ticket  cryptoutil.Ticket
		ts      int64
		param   *getVolParameter
	)
	if param, err = parseGetVolParameter(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(param.name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if !param.skipOwnerValidation && !matchKey(vol.Owner, param.authKey) {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolAuthKeyNotMatch))
		return
	}
	viewCache := vol.getViewCache()
	if len(viewCache) == 0 {
		vol.updateViewCache(m.cluster)
		viewCache = vol.getViewCache()
	}
	if !param.skipOwnerValidation && vol.authenticate {
		if jobj, ticket, ts, err = parseAndCheckTicket(r, m.cluster.MasterSecretKey, param.name); err != nil {
			if err == proto.ErrExpiredTicket {
				sendErrReply(w, r, newErrHTTPReply(err))
				return
			}
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInvalidTicket, Msg: err.Error()})
			return
		}
		if message, err = genRespMessage(viewCache, &jobj, ts, ticket.SessionKey.Key); err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeMasterAPIGenRespError, Msg: err.Error()})
			return
		}
		sendOkReply(w, r, newSuccessHTTPReply(message))
	} else {
		send(w, r, viewCache)
	}
}

// Obtain the volume information such as total capacity and used space, etc.
func (m *Server) getVolStatInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		name string
		vol  *Vol
	)
	if name, err = parseAndExtractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(volStat(vol)))
}

func volStat(vol *Vol) (stat *proto.VolStatInfo) {
	stat = new(proto.VolStatInfo)
	stat.Name = vol.Name
	stat.TotalSize = vol.Capacity * util.GB
	stat.UsedSize = vol.totalUsedSpace()
	if stat.UsedSize > stat.TotalSize {
		stat.UsedSize = stat.TotalSize
	}
	stat.UsedRatio = strconv.FormatFloat(float64(stat.UsedSize)/float64(stat.TotalSize), 'f', 2, 32)
	log.LogDebugf("total[%v],usedSize[%v]", stat.TotalSize, stat.UsedSize)
	return
}

func getMetaPartitionView(mp *MetaPartition) (mpView *proto.MetaPartitionView) {
	mpView = proto.NewMetaPartitionView(mp.PartitionID, mp.Start, mp.End, mp.Status)
	mp.Lock()
	defer mp.Unlock()
	for _, host := range mp.Hosts {
		mpView.Members = append(mpView.Members, host)
	}
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		return
	}
	mpView.LeaderAddr = mr.Addr
	mpView.MaxInodeID = mp.MaxInodeID
	mpView.InodeCount = mp.InodeCount
	mpView.DentryCount = mp.DentryCount
	mpView.IsRecover = mp.IsRecover
	return
}

func (m *Server) getMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		err         error
		partitionID uint64
		mp          *MetaPartition
	)
	if partitionID, err = parseAndExtractPartitionInfo(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrMetaPartitionNotExists))
		return
	}

	var toInfo = func(mp *MetaPartition) *proto.MetaPartitionInfo {
		mp.RLock()
		defer mp.RUnlock()
		var replicas = make([]*proto.MetaReplicaInfo, len(mp.Replicas))
		zones := make([]string, len(mp.Hosts))
		for idx, host := range mp.Hosts {
			metaNode, err := m.cluster.metaNode(host)
			if err == nil {
				zones[idx] = metaNode.ZoneName
			}
		}
		for i := 0; i < len(replicas); i++ {
			replicas[i] = &proto.MetaReplicaInfo{
				Addr:       mp.Replicas[i].Addr,
				ReportTime: mp.Replicas[i].ReportTime,
				Status:     mp.Replicas[i].Status,
				IsLeader:   mp.Replicas[i].IsLeader,
			}
		}
		var mpInfo = &proto.MetaPartitionInfo{
			PartitionID:   mp.PartitionID,
			Start:         mp.Start,
			End:           mp.End,
			VolName:       mp.volName,
			MaxInodeID:    mp.MaxInodeID,
			InodeCount:    mp.InodeCount,
			DentryCount:   mp.DentryCount,
			Replicas:      replicas,
			ReplicaNum:    mp.ReplicaNum,
			Status:        mp.Status,
			IsRecover:     mp.IsRecover,
			Hosts:         mp.Hosts,
			Peers:         mp.Peers,
			Zones:         zones,
			MissNodes:     mp.MissNodes,
			OfflinePeerID: mp.OfflinePeerID,
			LoadResponse:  mp.LoadResponse,
		}
		return mpInfo
	}

	sendOkReply(w, r, newSuccessHTTPReply(toInfo(mp)))
}

func (m *Server) listVols(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		keywords string
		vol      *Vol
		volsInfo []*proto.VolInfo
	)
	if keywords, err = parseKeywords(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	volsInfo = make([]*proto.VolInfo, 0)
	for _, name := range m.cluster.allVolNames() {
		if strings.Contains(name, keywords) {
			if vol, err = m.cluster.getVol(name); err != nil {
				sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
				return
			}
			stat := volStat(vol)
			volInfo := proto.NewVolInfo(vol.Name, vol.Owner, vol.createTime, vol.status(), stat.TotalSize, stat.UsedSize)
			volsInfo = append(volsInfo, volInfo)
		}
	}
	sendOkReply(w, r, newSuccessHTTPReply(volsInfo))
}

func parseAndExtractPartitionInfo(r *http.Request) (partitionID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	return
}

func extractMetaPartitionID(r *http.Request) (partitionID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func extractCapacity(r *http.Request) (capacity int, err error) {
	var capacityStr string
	if capacityStr = r.FormValue(volCapacityKey); capacityStr == "" {
		err = keyNotFound(volCapacityKey)
		return
	}
	if capacity, err = strconv.Atoi(capacityStr); err != nil {
		err = unmatchedKey(volCapacityKey)
	}
	return
}

func extractAuthKey(r *http.Request) (authKey string, err error) {
	if authKey = r.FormValue(volAuthKey); authKey == "" {
		err = keyNotFound(volAuthKey)
		return
	}
	return
}

func parseAndExtractName(r *http.Request) (name string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractName(r)
}

func extractName(r *http.Request) (name string, err error) {
	if name = r.FormValue(nameKey); name == "" {
		err = keyNotFound(nameKey)
		return
	}
	if !volNameRegexp.MatchString(name) {
		return "", errors.New("name can only be number and letters")
	}

	return
}

func extractOwner(r *http.Request) (owner string, err error) {
	if owner = r.FormValue(volOwnerKey); owner == "" {
		err = keyNotFound(volOwnerKey)
		return
	}
	if !ownerRegexp.MatchString(owner) {
		return "", errors.New("owner can only be number and letters")
	}

	return
}

func parseAndCheckTicket(r *http.Request, key []byte, volName string) (jobj proto.APIAccessReq, ticket cryptoutil.Ticket, ts int64, err error) {
	var (
		plaintext []byte
	)

	if err = r.ParseForm(); err != nil {
		return
	}

	if plaintext, err = extractClientReqInfo(r); err != nil {
		return
	}

	if err = json.Unmarshal([]byte(plaintext), &jobj); err != nil {
		return
	}

	if err = proto.VerifyAPIAccessReqIDs(&jobj); err != nil {
		return
	}

	ticket, ts, err = extractTicketMess(&jobj, key, volName)

	return
}

func extractClientReqInfo(r *http.Request) (plaintext []byte, err error) {
	var (
		message string
	)
	if err = r.ParseForm(); err != nil {
		return
	}

	if message = r.FormValue(proto.ClientMessage); message == "" {
		err = keyNotFound(proto.ClientMessage)
		return
	}

	if plaintext, err = cryptoutil.Base64Decode(message); err != nil {
		return
	}

	return
}

func extractTicketMess(req *proto.APIAccessReq, key []byte, volName string) (ticket cryptoutil.Ticket, ts int64, err error) {
	if ticket, err = proto.ExtractTicket(req.Ticket, key); err != nil {
		err = fmt.Errorf("extractTicket failed: %s", err.Error())
		return
	}
	if time.Now().Unix() >= ticket.Exp {
		err = proto.ErrExpiredTicket
		return
	}
	if ts, err = proto.ParseVerifier(req.Verifier, ticket.SessionKey.Key); err != nil {
		err = fmt.Errorf("parseVerifier failed: %s", err.Error())
		return
	}
	if err = proto.CheckAPIAccessCaps(&ticket, proto.APIRsc, req.Type, proto.APIAccess); err != nil {
		err = fmt.Errorf("CheckAPIAccessCaps failed: %s", err.Error())
		return
	}
	if err = proto.CheckVOLAccessCaps(&ticket, volName, proto.VOLAccess, proto.MasterNode); err != nil {
		err = fmt.Errorf("CheckVOLAccessCaps failed: %s", err.Error())
		return
	}
	return
}

func genRespMessage(data []byte, req *proto.APIAccessReq, ts int64, key []byte) (message string, err error) {
	var (
		jresp []byte
		resp  proto.MasterAPIAccessResp
	)

	resp.Data = data

	resp.APIResp.Type = req.Type + 1
	resp.APIResp.ClientID = req.ClientID
	resp.APIResp.ServiceID = req.ServiceID
	resp.APIResp.Verifier = ts + 1 // increase ts by one for client verify server

	if jresp, err = json.Marshal(resp); err != nil {
		err = fmt.Errorf("json marshal for response failed %s", err.Error())
		return
	}

	if message, err = cryptoutil.EncodeMessage(jresp, key); err != nil {
		err = fmt.Errorf("encdoe message for response failed %s", err.Error())
		return
	}

	return
}

func (m *Server) associateVolWithUser(userID, volName string) error {
	var err error
	var userInfo *proto.UserInfo
	if userInfo, err = m.user.getUserInfo(userID); err != nil && err != proto.ErrUserNotExists {
		return err
	}
	if err == proto.ErrUserNotExists {
		var param = proto.UserCreateParam{
			ID:       userID,
			Password: DefaultUserPassword,
			Type:     proto.UserTypeNormal,
		}
		if userInfo, err = m.user.createKey(&param); err != nil {
			return err
		}
	}
	if _, err = m.user.addOwnVol(userInfo.UserID, volName); err != nil {
		return err
	}
	return nil
}
