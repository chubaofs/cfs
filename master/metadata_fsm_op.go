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
	"strconv"
	"strings"
	"sync/atomic"

	bsProto "github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tiglabs/raft/proto"
)

/* We defines several "values" such as clusterValue, metaPartitionValue, dataPartitionValue, volValue, dataNodeValue,
   nodeSetValue, and metaNodeValue here. Those are the value objects that will be marshaled as a byte array to
   transferred over the network. */

type clusterValue struct {
	Name                        string
	Threshold                   float32
	DisableAutoAllocate         bool
	DataNodeDeleteLimitRate     uint64
	MetaNodeDeleteBatchCount    uint64
	MetaNodeDeleteWorkerSleepMs uint64
	DataNodeAutoRepairLimitRate uint64
	FaultDomain					bool
}

func newClusterValue(c *Cluster) (cv *clusterValue) {
	cv = &clusterValue{
		Name:                        c.Name,
		Threshold:                   c.cfg.MetaNodeThreshold,
		DataNodeDeleteLimitRate:     c.cfg.DataNodeDeleteLimitRate,
		MetaNodeDeleteBatchCount:    c.cfg.MetaNodeDeleteBatchCount,
		MetaNodeDeleteWorkerSleepMs: c.cfg.MetaNodeDeleteWorkerSleepMs,
		DataNodeAutoRepairLimitRate: c.cfg.DataNodeAutoRepairLimitRate,
		DisableAutoAllocate:         c.DisableAutoAllocate,
		FaultDomain:                 c.FaultDomain,
	}
	return cv
}

type metaPartitionValue struct {
	PartitionID   uint64
	Start         uint64
	End           uint64
	VolID         uint64
	ReplicaNum    uint8
	Status        int8
	VolName       string
	Hosts         string
	OfflinePeerID uint64
	Peers         []bsProto.Peer
	IsRecover     bool
}

func newMetaPartitionValue(mp *MetaPartition) (mpv *metaPartitionValue) {
	mpv = &metaPartitionValue{
		PartitionID:   mp.PartitionID,
		Start:         mp.Start,
		End:           mp.End,
		VolID:         mp.volID,
		ReplicaNum:    mp.ReplicaNum,
		Status:        mp.Status,
		VolName:       mp.volName,
		Hosts:         mp.hostsToString(),
		Peers:         mp.Peers,
		OfflinePeerID: mp.OfflinePeerID,
		IsRecover:     mp.IsRecover,
	}
	return
}

type dataPartitionValue struct {
	PartitionID   uint64
	ReplicaNum    uint8
	Hosts         string
	Peers         []bsProto.Peer
	Status        int8
	VolID         uint64
	VolName       string
	OfflinePeerID uint64
	Replicas      []*replicaValue
	IsRecover     bool
}

type replicaValue struct {
	Addr     string
	DiskPath string
}

func newDataPartitionValue(dp *DataPartition) (dpv *dataPartitionValue) {
	dpv = &dataPartitionValue{
		PartitionID:   dp.PartitionID,
		ReplicaNum:    dp.ReplicaNum,
		Hosts:         dp.hostsToString(),
		Peers:         dp.Peers,
		Status:        dp.Status,
		VolID:         dp.VolID,
		VolName:       dp.VolName,
		OfflinePeerID: dp.OfflinePeerID,
		Replicas:      make([]*replicaValue, 0),
		IsRecover:     dp.isRecover,
	}
	for _, replica := range dp.Replicas {
		rv := &replicaValue{Addr: replica.Addr, DiskPath: replica.DiskPath}
		dpv.Replicas = append(dpv.Replicas, rv)
	}
	return
}

type volValue struct {
	ID                uint64
	Name              string
	ReplicaNum        uint8
	DpReplicaNum      uint8
	Status            uint8
	DataPartitionSize uint64
	Capacity          uint64
	Owner             string
	FollowerRead      bool
	Authenticate      bool
	CrossZone         bool
	DomainOn          bool
	ZoneName          string
	OSSAccessKey      string
	OSSSecretKey      string
	CreateTime        int64
	Description       string
	DpSelectorName    string
	DpSelectorParm    string
	DefaultPriority   bool
}

func (v *volValue) Bytes() (raw []byte, err error) {
	raw, err = json.Marshal(v)
	return
}

func newVolValue(vol *Vol) (vv *volValue) {
	vv = &volValue{
		ID:                vol.ID,
		Name:              vol.Name,
		ReplicaNum:        vol.mpReplicaNum,
		DpReplicaNum:      vol.dpReplicaNum,
		Status:            vol.Status,
		DataPartitionSize: vol.dataPartitionSize,
		Capacity:          vol.Capacity,
		Owner:             vol.Owner,
		FollowerRead:      vol.FollowerRead,
		Authenticate:      vol.authenticate,
		CrossZone:         vol.crossZone,
		DomainOn:          vol.domainOn,
		ZoneName:          vol.zoneName,
		OSSAccessKey:      vol.OSSAccessKey,
		OSSSecretKey:      vol.OSSSecretKey,
		CreateTime:        vol.createTime,
		Description:       vol.description,
		DpSelectorName:    vol.dpSelectorName,
		DpSelectorParm:    vol.dpSelectorParm,
		DefaultPriority:   vol.defaultPriority,
	}
	return
}

func newVolValueFromBytes(raw []byte) (*volValue, error) {
	vv := &volValue{}
	if err := json.Unmarshal(raw, vv); err != nil {
		return nil, err
	}
	return vv, nil
}

type dataNodeValue struct {
	ID        uint64
	NodeSetID uint64
	Addr      string
	ZoneName  string
}

func newDataNodeValue(dataNode *DataNode) *dataNodeValue {
	return &dataNodeValue{
		ID:        dataNode.ID,
		NodeSetID: dataNode.NodeSetID,
		Addr:      dataNode.Addr,
		ZoneName:  dataNode.ZoneName,
	}
}

type metaNodeValue struct {
	ID        uint64
	NodeSetID uint64
	Addr      string
	ZoneName  string
}

func newMetaNodeValue(metaNode *MetaNode) *metaNodeValue {
	return &metaNodeValue{
		ID:        metaNode.ID,
		NodeSetID: metaNode.NodeSetID,
		Addr:      metaNode.Addr,
		ZoneName:  metaNode.ZoneName,
	}
}

type nodeSetValue struct {
	ID       uint64
	Capacity int
	ZoneName string
}

type nodeSetGrpValue struct {
	ID       	uint64
	NodeSetsIds []uint64
	Status    	uint8
}

type zoneDomainValue struct {
	ExcludeZoneMap  map[string]int
	NeedFaultDomain bool
	DataRatio       float64
	ExcludeZoneUseRatio    float64
}
func newZoneDomainValue() (ev *zoneDomainValue) {
	ev = &zoneDomainValue{
		ExcludeZoneMap: make(map[string]int),
	}
	return
}
func newNodeSetValue(nset *nodeSet) (nsv *nodeSetValue) {
	nsv = &nodeSetValue{
		ID:       nset.ID,
		Capacity: nset.Capacity,
		ZoneName: nset.zoneName,
	}
	return
}
func newNodeSetGrpValue(nset *nodeSetGroup) (nsv *nodeSetGrpValue) {
	nsv = &nodeSetGrpValue{
		ID:       nset.ID,
		NodeSetsIds: nset.nodeSetsIds,
		Status: nset.status,
	}
	return
}
// RaftCmd defines the Raft commands.
type RaftCmd struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

// Marshal converts the RaftCmd to a byte array.
func (m *RaftCmd) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts the byte array to a RaftCmd.
func (m *RaftCmd) Unmarshal(data []byte) (err error) {
	return json.Unmarshal(data, m)
}

func (m *RaftCmd) setOpType() {
	keyArr := strings.Split(m.K, keySeparator)
	if len(keyArr) < 2 {
		log.LogWarnf("action[setOpType] invalid length[%v]", keyArr)
		return
	}
	switch keyArr[1] {
	case metaNodeAcronym:
		m.Op = opSyncAddMetaNode
	case dataNodeAcronym:
		m.Op = opSyncAddDataNode
	case dataPartitionAcronym:
		m.Op = opSyncAddDataPartition
	case metaPartitionAcronym:
		m.Op = opSyncAddMetaPartition
	case volAcronym:
		m.Op = opSyncAddVol
	case clusterAcronym:
		m.Op = opSyncPutCluster
	case nodeSetAcronym:
		m.Op = opSyncAddNodeSet
	case maxDataPartitionIDKey:
		m.Op = opSyncAllocDataPartitionID
	case maxMetaPartitionIDKey:
		m.Op = opSyncAllocMetaPartitionID
	case maxCommonIDKey:
		m.Op = opSyncAllocCommonID
	case userAcronym:
		m.Op = opSyncAddUserInfo
	case akAcronym:
		m.Op = opSyncAddAKUser
	case volUserAcronym:
		m.Op = opSyncAddVolUser
	default:
		log.LogWarnf("action[setOpType] unknown opCode[%v]", keyArr[1])
	}
}


//key=#c#name
func (c *Cluster) syncPutCluster() (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opSyncPutCluster
	metadata.K = clusterPrefix + c.Name
	cv := newClusterValue(c)
	metadata.V, err = json.Marshal(cv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

// key=#s#id
func (c *Cluster) syncAddNodeSet(nset *nodeSet) (err error) {
	return c.putNodeSetInfo(opSyncAddNodeSet, nset)
}

func (c *Cluster) syncUpdateNodeSet(nset *nodeSet) (err error) {
	return c.putNodeSetInfo(opSyncUpdateNodeSet, nset)
}

func (c *Cluster) putNodeSetInfo(opType uint32, nset *nodeSet) (err error) {
	log.LogInfof("action[putNodeSetInfo], type:[%v], ID:[%v], name:[%v]", opType, nset.ID, nset.zoneName)
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = nodeSetPrefix + strconv.FormatUint(nset.ID, 10)
	nsv := newNodeSetValue(nset)
	metadata.V, err = json.Marshal(nsv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) putNodeSetGrpInfo(opType uint32, nsg *nodeSetGroup) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = nodeSetGrpPrefix + strconv.FormatUint(nsg.ID, 10)
	log.LogInfof("action[putNodeSetGrpInfo] nsg id[%v] status[%v] ids[%v]", nsg.ID, nsg.status, nsg.nodeSetsIds)
	nsv := newNodeSetGrpValue(nsg)
	log.LogInfof("action[putNodeSetGrpInfo] nsv id[%v] status[%v] ids[%v]", nsv.ID, nsv.Status, nsv.NodeSetsIds)
	metadata.V, err = json.Marshal(nsv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

// key=#dp#volID#partitionID,value=json.Marshal(dataPartitionValue)
func (c *Cluster) syncAddDataPartition(dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(opSyncAddDataPartition, dp)
}

func (c *Cluster) syncUpdateDataPartition(dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(opSyncUpdateDataPartition, dp)
}

func (c *Cluster) syncDeleteDataPartition(dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(opSyncDeleteDataPartition, dp)
}

func (c *Cluster) putDataPartitionInfo(opType uint32, dp *DataPartition) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = dataPartitionPrefix + strconv.FormatUint(dp.VolID, 10) + keySeparator + strconv.FormatUint(dp.PartitionID, 10)
	dpv := newDataPartitionValue(dp)
	metadata.V, err = json.Marshal(dpv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) submit(metadata *RaftCmd) (err error) {
	cmd, err := metadata.Marshal()
	if err != nil {
		return errors.New(err.Error())
	}
	if _, err = c.partition.Submit(cmd); err != nil {
		msg := fmt.Sprintf("action[metadata_submit] err:%v", err.Error())
		return errors.New(msg)
	}
	return
}

//key=#vol#volID,value=json.Marshal(vv)
func (c *Cluster) syncAddVol(vol *Vol) (err error) {
	return c.syncPutVolInfo(opSyncAddVol, vol)
}

func (c *Cluster) syncUpdateVol(vol *Vol) (err error) {
	return c.syncPutVolInfo(opSyncUpdateVol, vol)
}

func (c *Cluster) syncDeleteVol(vol *Vol) (err error) {
	return c.syncPutVolInfo(opSyncDeleteVol, vol)
}

func (c *Cluster) syncPutVolInfo(opType uint32, vol *Vol) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = volPrefix + strconv.FormatUint(vol.ID, 10)
	vv := newVolValue(vol)
	if metadata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

// key=#mp#volID#metaPartitionID,value=json.Marshal(metaPartitionValue)
func (c *Cluster) syncAddMetaPartition(mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(opSyncAddMetaPartition, mp)
}

func (c *Cluster) syncUpdateMetaPartition(mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(opSyncUpdateMetaPartition, mp)
}

func (c *Cluster) syncDeleteMetaPartition(mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(opSyncDeleteMetaPartition, mp)
}

func (c *Cluster) putMetaPartitionInfo(opType uint32, mp *MetaPartition) (err error) {
	metadata, err := c.buildMetaPartitionRaftCmd(opType, mp)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) buildMetaPartitionRaftCmd(opType uint32, mp *MetaPartition) (metadata *RaftCmd, err error) {
	metadata = new(RaftCmd)
	metadata.Op = opType
	partitionID := strconv.FormatUint(mp.PartitionID, 10)
	metadata.K = metaPartitionPrefix + strconv.FormatUint(mp.volID, 10) + keySeparator + partitionID
	mpv := newMetaPartitionValue(mp)
	if metadata.V, err = json.Marshal(mpv); err != nil {
		return metadata, errors.New(err.Error())
	}
	return
}

func (c *Cluster) syncBatchCommitCmd(cmdMap map[string]*RaftCmd) (err error) {
	value, err := json.Marshal(cmdMap)
	if err != nil {
		return
	}
	cmd := &RaftCmd{
		Op: opSyncBatchPut,
		K:  "batch_put",
		V:  value,
	}
	return c.submit(cmd)
}

// key=#mn#id#addr,value = nil
func (c *Cluster) syncAddMetaNode(metaNode *MetaNode) (err error) {
	return c.syncPutMetaNode(opSyncAddMetaNode, metaNode)
}

func (c *Cluster) syncDeleteMetaNode(metaNode *MetaNode) (err error) {
	return c.syncPutMetaNode(opSyncDeleteMetaNode, metaNode)
}

func (c *Cluster) syncUpdateMetaNode(metaNode *MetaNode) (err error) {
	return c.syncPutMetaNode(opSyncUpdateMetaNode, metaNode)
}

func (c *Cluster) syncPutMetaNode(opType uint32, metaNode *MetaNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = metaNodePrefix + strconv.FormatUint(metaNode.ID, 10) + keySeparator + metaNode.Addr
	mnv := newMetaNodeValue(metaNode)
	metadata.V, err = json.Marshal(mnv)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

// key=#dn#id#Addr,value = json.Marshal(dnv)
func (c *Cluster) syncAddDataNode(dataNode *DataNode) (err error) {
	return c.syncPutDataNodeInfo(opSyncAddDataNode, dataNode)
}

func (c *Cluster) syncDeleteDataNode(dataNode *DataNode) (err error) {
	return c.syncPutDataNodeInfo(opSyncDeleteDataNode, dataNode)
}

func (c *Cluster) syncUpdateDataNode(dataNode *DataNode) (err error) {
	return c.syncPutDataNodeInfo(opSyncUpdateDataNode, dataNode)
}

func (c *Cluster) syncPutDataNodeInfo(opType uint32, dataNode *DataNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = dataNodePrefix + strconv.FormatUint(dataNode.ID, 10) + keySeparator + dataNode.Addr
	dnv := newDataNodeValue(dataNode)
	metadata.V, err = json.Marshal(dnv)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) addRaftNode(nodeID uint64, addr string) (err error) {
	peer := proto.Peer{ID: nodeID}
	_, err = c.partition.ChangeMember(proto.ConfAddNode, peer, []byte(addr))
	if err != nil {
		return errors.New("action[addRaftNode] error: " + err.Error())
	}
	return nil
}

func (c *Cluster) removeRaftNode(nodeID uint64, addr string) (err error) {
	peer := proto.Peer{ID: nodeID}
	_, err = c.partition.ChangeMember(proto.ConfRemoveNode, peer, []byte(addr))
	if err != nil {
		return errors.New("action[removeRaftNode] error: " + err.Error())
	}
	return nil
}

func (c *Cluster) updateMetaNodeDeleteBatchCount(val uint64) {
	atomic.StoreUint64(&c.cfg.MetaNodeDeleteBatchCount, val)
}

func (c *Cluster) updateMetaNodeDeleteWorkerSleepMs(val uint64) {
	atomic.StoreUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs, val)
}

func (c *Cluster) updateDataNodeAutoRepairLimit(val uint64) {
	atomic.StoreUint64(&c.cfg.DataNodeAutoRepairLimitRate, val)
}

func (c *Cluster) updateDataNodeDeleteLimitRate(val uint64) {
	atomic.StoreUint64(&c.cfg.DataNodeDeleteLimitRate, val)
}

func (c *Cluster) loadClusterValue() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(clusterPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadClusterValue],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		cv := &clusterValue{}
		if err = json.Unmarshal(value, cv); err != nil {
			log.LogErrorf("action[loadClusterValue], unmarshal err:%v", err.Error())
			return err
		}
		c.cfg.MetaNodeThreshold = cv.Threshold
		c.DisableAutoAllocate = cv.DisableAutoAllocate
		c.updateMetaNodeDeleteBatchCount(cv.MetaNodeDeleteBatchCount)
		c.updateMetaNodeDeleteWorkerSleepMs(cv.MetaNodeDeleteWorkerSleepMs)
		c.updateDataNodeDeleteLimitRate(cv.DataNodeDeleteLimitRate)
		c.updateDataNodeAutoRepairLimit(cv.DataNodeAutoRepairLimitRate)
		log.LogInfof("action[loadClusterValue], metaNodeThreshold[%v]", cv.Threshold)
	}
	return
}

func (c *Cluster) loadNodeSets() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(nodeSetPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadNodeSets],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		nsv := &nodeSetValue{}
		if err = json.Unmarshal(value, nsv); err != nil {
			log.LogErrorf("action[loadNodeSets], unmarshal err:%v", err.Error())
			return err
		}
		if nsv.ZoneName == "" {
			nsv.ZoneName = DefaultZoneName
		}
		ns := newNodeSet(nsv.ID, c.cfg.nodeSetCapacity, nsv.ZoneName)
		zone, err := c.t.getZone(nsv.ZoneName)
		if err != nil {
			log.LogErrorf("action[loadNodeSets], getZone err:%v", err)
			zone = newZone(nsv.ZoneName)
			c.t.putZoneIfAbsent(zone)
		}
		zone.putNodeSet(ns)
		log.LogInfof("action[addNodeSetGrp] nodeSet[%v]",ns.ID)
		if err = c.addNodeSetGrp(ns, true); err != nil {
			log.LogErrorf("action[createNodeSet] nodeSet[%v] err[%v]",ns.ID, err)
			return err
		}
		log.LogInfof("action[loadNodeSets], nsId[%v],zone[%v]", ns.ID, zone.name)
	}
	return nil
}
// put exclude zone only be used one time when master update and restart
func (c *Cluster) putZoneDomain(init bool) (err error) {
	log.LogInfof("action[putZoneDomain]")
	metadata := new(RaftCmd)
	metadata.Op = opSyncExclueDomain
	metadata.K = DomainPrefix
	if init {
		for i := 0; i < len(c.t.zones); i++ {
			c.nodeSetGrpManager.excludeZoneListDomain[c.t.zones[i].name] = 0
			c.t.domainExcludeZones = append(c.t.domainExcludeZones, c.t.zones[i].name)
		}
		if len(c.t.zones) == 0 {
			c.needFaultDomain = true
		}
	}
	domainValue := newZoneDomainValue()
	domainValue.ExcludeZoneMap = c.nodeSetGrpManager.excludeZoneListDomain
	domainValue.NeedFaultDomain = c.needFaultDomain
	if c.nodeSetGrpManager.dataRatioLimit > 0 {
		domainValue.DataRatio = c.nodeSetGrpManager.dataRatioLimit
	} else {
		domainValue.DataRatio = defaultZoneUsageThreshold
	}
	if c.nodeSetGrpManager.excludeZoneUseRatio > 0 && c.nodeSetGrpManager.excludeZoneUseRatio <= 1{
		domainValue.DataRatio = c.nodeSetGrpManager.excludeZoneUseRatio
	} else {
		domainValue.DataRatio = defaultZoneUsageThreshold
	}

	metadata.V, err = json.Marshal(domainValue)
	if err != nil {
		return
	}
	return c.submit(metadata)
}
func (c *Cluster) loadZoneDomain() (ok bool, err error) {
	log.LogInfof("action[loadZoneDomain]")
	result, err := c.fsm.store.SeekForPrefix([]byte(DomainPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadZoneDomain],err:%v", err.Error())
		log.LogInfof("action[loadZoneDomain] err[%v]", err)
		return false, err
	}
	if len(result) == 0 {
		err = fmt.Errorf("action[loadZoneDomain],err:not found")
		log.LogInfof("action[loadZoneDomain] err[%v]", err)
		return false,nil
	}
	for _, value := range result {
		nsv := &zoneDomainValue{}
		if err = json.Unmarshal(value, nsv); err != nil {
			log.LogErrorf("action[loadNodeSets], unmarshal err:%v", err.Error())
			return true, err
		}
		log.LogInfof("action[loadZoneDomain] get value!exclue map[%v],need domain[%v]", nsv.ExcludeZoneMap, nsv.NeedFaultDomain)
		c.nodeSetGrpManager.excludeZoneListDomain = nsv.ExcludeZoneMap
		for zoneName, _ := range nsv.ExcludeZoneMap {
			c.t.domainExcludeZones = append(c.t.domainExcludeZones, zoneName)
		}
		c.needFaultDomain = nsv.NeedFaultDomain
		c.nodeSetGrpManager.dataRatioLimit = nsv.DataRatio
		c.nodeSetGrpManager.excludeZoneUseRatio = nsv.ExcludeZoneUseRatio
		break
	}
	log.LogInfof("action[loadZoneDomain] success!")
	return true, nil
}

func (c *Cluster) loadNodeSetGrps() (err error) {
	log.LogInfof("action[loadNodeSetGrps]")
	result, err := c.fsm.store.SeekForPrefix([]byte(nodeSetGrpPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadNodeSets],err:%v", err.Error())
		log.LogInfof("action[loadNodeSetGrps] seek failed, nsgId[%v]", err)
		return err
	}
	if len(result) > 0  {
		log.LogInfof("action[loadNodeSetGrps] get result len[%v]", len(result))
		c.nodeSetGrpManager.start()
	}
	log.LogInfof("action[loadNodeSetGrps] get result len[%v] before decode", len(result))
	for _, value := range result {
		nsv := &nodeSetGrpValue{}
		if err = json.Unmarshal(value, nsv); err != nil {
			log.LogFatal("action[loadNodeSets], unmarshal err:%v", err.Error())
			return err
		}
		log.LogInfof("action[loadNodeSetGrps] get result nsv id[%v],status[%v],ids[%v]", nsv.ID, nsv.Status,nsv.NodeSetsIds)
		nsg := newNodeSetGrp(c)
		nsg.nodeSetsIds = nsv.NodeSetsIds
		nsg.ID = nsv.ID
		nsg.status = nsv.Status
		c.nodeSetGrpManager.nodeSetGrpMap = append(c.nodeSetGrpManager.nodeSetGrpMap, nsg)
		var j int
		for j = 0; j < len(nsv.NodeSetsIds); j++ {
			c.nodeSetGrpManager.nsId2NsGrpMap[nsv.NodeSetsIds[j]] = len(c.nodeSetGrpManager.nodeSetGrpMap) - 1
			log.LogInfof("action[loadNodeSetGrps] get result index[%v] nodesetid[%v] nodesetgrp index [%v]", nsv.ID, nsv.NodeSetsIds[j], nsv.Status)
		}
		log.LogInfof("action[loadNodeSetGrps], nsgId[%v],status[%v]", nsg.ID, nsg.status)
	}
	return
}

func (c *Cluster) loadDataNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(dataNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadDataNodes],err:%v", err.Error())
		return err
	}

	for _, value := range result {
		dnv := &dataNodeValue{}
		if err = json.Unmarshal(value, dnv); err != nil {
			err = fmt.Errorf("action[loadDataNodes],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		if dnv.ZoneName == "" {
			dnv.ZoneName = DefaultZoneName
		}
		dataNode := newDataNode(dnv.Addr, dnv.ZoneName, c.Name)
		dataNode.ID = dnv.ID
		dataNode.NodeSetID = dnv.NodeSetID
		olddn, ok := c.dataNodes.Load(dataNode.Addr)
		if ok {
			if olddn.(*DataNode).ID <= dataNode.ID {
				continue
			}
		}
		c.dataNodes.Store(dataNode.Addr, dataNode)
		log.LogInfof("action[loadDataNodes],dataNode[%v],dataNodeID[%v],zone[%v],ns[%v]", dataNode.Addr, dataNode.ID, dnv.ZoneName, dnv.NodeSetID)
	}
	return
}

func (c *Cluster) loadMetaNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(metaNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadMetaNodes],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		mnv := &metaNodeValue{}
		if err = json.Unmarshal(value, mnv); err != nil {
			err = fmt.Errorf("action[loadMetaNodes],unmarshal err:%v", err.Error())
			return err
		}
		if mnv.ZoneName == "" {
			mnv.ZoneName = DefaultZoneName
		}
		metaNode := newMetaNode(mnv.Addr, mnv.ZoneName, c.Name)
		metaNode.ID = mnv.ID
		metaNode.NodeSetID = mnv.NodeSetID
		oldmn, ok := c.metaNodes.Load(metaNode.Addr)
		if ok {
			if oldmn.(*MetaNode).ID <= metaNode.ID {
				continue
			}
		}
		c.metaNodes.Store(metaNode.Addr, metaNode)
		log.LogInfof("action[loadMetaNodes],metaNode[%v], metaNodeID[%v],zone[%v],ns[%v]", metaNode.Addr, metaNode.ID, mnv.ZoneName, mnv.NodeSetID)
	}
	return
}

func (c *Cluster) loadVols() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(volPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadVols],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		var vv *volValue
		if vv, err = newVolValueFromBytes(value); err != nil {
			err = fmt.Errorf("action[loadVols],value:%v,unmarshal err:%v", string(value), err)
			return err
		}
		vol := newVolFromVolValue(vv)
		vol.Status = vv.Status
		c.putVol(vol)
		log.LogInfof("action[loadVols],vol[%v]", vol.Name)
	}
	return
}

func (c *Cluster) loadMetaPartitions() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(metaPartitionPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadMetaPartitions],err:%v", err.Error())
		return err
	}

	for _, value := range result {
		mpv := &metaPartitionValue{}
		if err = json.Unmarshal(value, mpv); err != nil {
			err = fmt.Errorf("action[loadMetaPartitions],value:%v,unmarshal err:%v", string(value), err)
			return err
		}
		vol, err1 := c.getVol(mpv.VolName)
		if err1 != nil {
			log.LogErrorf("action[loadMetaPartitions] err:%v", err1.Error())
			continue
		}
		if vol.ID != mpv.VolID {
			Warn(c.Name, fmt.Sprintf("action[loadMetaPartitions] has duplicate vol[%v],vol.ID[%v],mpv.VolID[%v]", mpv.VolName, vol.ID, mpv.VolID))
			continue
		}
		for i := 0; i < len(mpv.Peers); i++ {
			mn, ok := c.metaNodes.Load(mpv.Peers[i].Addr)
			if ok && mn.(*MetaNode).ID != mpv.Peers[i].ID {
				mpv.Peers[i].ID = mn.(*MetaNode).ID
			}
		}
		mp := newMetaPartition(mpv.PartitionID, mpv.Start, mpv.End, vol.mpReplicaNum, vol.Name, mpv.VolID)
		mp.setHosts(strings.Split(mpv.Hosts, underlineSeparator))
		mp.setPeers(mpv.Peers)
		mp.OfflinePeerID = mpv.OfflinePeerID
		mp.IsRecover = mpv.IsRecover
		vol.addMetaPartition(mp)
		log.LogInfof("action[loadMetaPartitions],vol[%v],mp[%v]", vol.Name, mp.PartitionID)
	}
	return
}

func (c *Cluster) loadDataPartitions() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(dataPartitionPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadDataPartitions],err:%v", err.Error())
		return err
	}
	for _, value := range result {

		dpv := &dataPartitionValue{}
		if err = json.Unmarshal(value, dpv); err != nil {
			err = fmt.Errorf("action[loadDataPartitions],value:%v,unmarshal err:%v", string(value), err)
			return err
		}
		vol, err1 := c.getVol(dpv.VolName)
		if err1 != nil {
			log.LogErrorf("action[loadDataPartitions] err:%v", err1.Error())
			continue
		}
		if vol.ID != dpv.VolID {
			Warn(c.Name, fmt.Sprintf("action[loadDataPartitions] has duplicate vol[%v],vol.ID[%v],mpv.VolID[%v]", dpv.VolName, vol.ID, dpv.VolID))
			continue
		}
		for i := 0; i < len(dpv.Peers); i++ {
			dn, ok := c.dataNodes.Load(dpv.Peers[i].Addr)
			if ok && dn.(*DataNode).ID != dpv.Peers[i].ID {
				dpv.Peers[i].ID = dn.(*DataNode).ID
			}
		}
		dp := newDataPartition(dpv.PartitionID, dpv.ReplicaNum, dpv.VolName, dpv.VolID)
		dp.Hosts = strings.Split(dpv.Hosts, underlineSeparator)
		dp.Peers = dpv.Peers
		dp.OfflinePeerID = dpv.OfflinePeerID
		dp.isRecover = dpv.IsRecover
		for _, rv := range dpv.Replicas {
			if !contains(dp.Hosts, rv.Addr) {
				continue
			}
			dp.afterCreation(rv.Addr, rv.DiskPath, c)
		}
		vol.dataPartitions.put(dp)
		log.LogInfof("action[loadDataPartitions],vol[%v],dp[%v]", vol.Name, dp.PartitionID)
	}
	return
}


