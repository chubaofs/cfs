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
	"sync"

	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"math"
	"strings"
	"time"
)

// MetaReplica defines the replica of a meta partition
type MetaReplica struct {
	Addr        string
	start       uint64 // lower bound of the inode id
	end         uint64 // upper bound of the inode id
	nodeID      uint64
	MaxInodeID  uint64
	InodeCount  uint64
	DentryCount uint64
	ReportTime  int64
	Status      int8 // unavailable, readOnly, readWrite
	IsLeader    bool
	metaNode    *MetaNode
}

// MetaPartition defines the structure of a meta partition
type MetaPartition struct {
	PartitionID   uint64
	Start         uint64
	End           uint64
	MaxInodeID    uint64
	InodeCount    uint64
	DentryCount   uint64
	Replicas      []*MetaReplica
	ReplicaNum    uint8
	Status        int8
	IsRecover     bool
	volID         uint64
	volName       string
	Hosts         []string
	Peers         []proto.Peer
	OfflinePeerID uint64
	MissNodes     map[string]int64
	LoadResponse  []*proto.MetaPartitionLoadResponse
	offlineMutex  sync.RWMutex
	sync.RWMutex
}

func newMetaReplica(start, end uint64, metaNode *MetaNode) (mr *MetaReplica) {
	mr = &MetaReplica{start: start, end: end, nodeID: metaNode.ID, Addr: metaNode.Addr}
	mr.metaNode = metaNode
	mr.ReportTime = time.Now().Unix()
	return
}

func newMetaPartition(partitionID, start, end uint64, replicaNum uint8, volName string, volID uint64) (mp *MetaPartition) {
	mp = &MetaPartition{PartitionID: partitionID, Start: start, End: end, volName: volName, volID: volID}
	mp.ReplicaNum = replicaNum
	mp.Replicas = make([]*MetaReplica, 0)
	mp.Status = proto.Unavailable
	mp.MissNodes = make(map[string]int64, 0)
	mp.Peers = make([]proto.Peer, 0)
	mp.Hosts = make([]string, 0)
	mp.LoadResponse = make([]*proto.MetaPartitionLoadResponse, 0)
	return
}

func (mp *MetaPartition) setPeers(peers []proto.Peer) {
	mp.Peers = peers
}

func (mp *MetaPartition) setHosts(hosts []string) {
	mp.Hosts = hosts
}

func (mp *MetaPartition) hostsToString() (hosts string) {
	return strings.Join(mp.Hosts, underlineSeparator)
}

func (mp *MetaPartition) addReplica(mr *MetaReplica) {
	for _, m := range mp.Replicas {
		if m.Addr == mr.Addr {
			return
		}
	}
	mp.Replicas = append(mp.Replicas, mr)
	return
}

func (mp *MetaPartition) removeReplica(mr *MetaReplica) {
	var newReplicas []*MetaReplica
	for _, m := range mp.Replicas {
		if m.Addr == mr.Addr {
			continue
		}
		newReplicas = append(newReplicas, m)
	}
	mp.Replicas = newReplicas
	return
}

func (mp *MetaPartition) removeReplicaByAddr(addr string) {
	var newReplicas []*MetaReplica
	for _, m := range mp.Replicas {
		if m.Addr == addr {
			continue
		}
		newReplicas = append(newReplicas, m)
	}
	mp.Replicas = newReplicas
	return
}

func (mp *MetaPartition) updateInodeIDRangeForAllReplicas() {
	for _, mr := range mp.Replicas {
		mr.end = mp.End
	}
}

//canSplit caller must be add lock
func (mp *MetaPartition) canSplit(end uint64) (err error) {
	if end < mp.Start {
		err = fmt.Errorf("end[%v] less than mp.start[%v]", end, mp.Start)
		return
	}
	// overflow
	if end > (defaultMaxMetaPartitionInodeID - defaultMetaPartitionInodeIDStep) {
		msg := fmt.Sprintf("action[updateInodeIDRange] vol[%v] partitionID[%v] nextStart[%v] "+
			"to prevent overflow ,not update end", mp.volName, mp.PartitionID, end)
		log.LogWarn(msg)
		err = fmt.Errorf(msg)
		return
	}
	if end <= mp.MaxInodeID {
		err = fmt.Errorf("next meta partition start must be larger than %v", mp.MaxInodeID)
		return
	}
	if _, err = mp.getMetaReplicaLeader(); err != nil {
		log.LogWarnf("action[updateInodeIDRange] vol[%v] id[%v] no leader", mp.volName, mp.PartitionID)
		return
	}
	return
}

func (mp *MetaPartition) addUpdateMetaReplicaTask(c *Cluster) (err error) {

	tasks := make([]*proto.AdminTask, 0)
	t := mp.createTaskToUpdateMetaReplica(c.Name, mp.PartitionID, mp.End)
	//if no leader,don't update end
	if t == nil {
		err = proto.ErrNoLeader
		return
	}
	tasks = append(tasks, t)
	c.addMetaNodeTasks(tasks)
	log.LogWarnf("action[addUpdateMetaReplicaTask] partitionID[%v] end[%v] success", mp.PartitionID, mp.End)
	return
}

func (mp *MetaPartition) checkEnd(c *Cluster, maxPartitionID uint64) {

	if mp.PartitionID < maxPartitionID {
		return
	}
	vol, err := c.getVol(mp.volName)
	if err != nil {
		log.LogWarnf("action[checkEnd] vol[%v] not exist", mp.volName)
		return
	}
	mp.Lock()
	defer mp.Unlock()
	curMaxPartitionID := vol.maxPartitionID()
	if mp.PartitionID != curMaxPartitionID {
		log.LogWarnf("action[checkEnd] partition[%v] not max partition[%v]", mp.PartitionID, curMaxPartitionID)
		return
	}
	if _, err = mp.getMetaReplicaLeader(); err != nil {
		log.LogWarnf("action[checkEnd] partition[%v] no leader", mp.PartitionID)
		return
	}
	if mp.End != defaultMaxMetaPartitionInodeID {
		oldEnd := mp.End
		mp.End = defaultMaxMetaPartitionInodeID
		if err := c.syncUpdateMetaPartition(mp); err != nil {
			mp.End = oldEnd
			log.LogErrorf("action[checkEnd] partitionID[%v] err[%v]", mp.PartitionID, err)
			return
		}
		if err = mp.addUpdateMetaReplicaTask(c); err != nil {
			mp.End = oldEnd
		}
	}
	log.LogDebugf("action[checkEnd] partitionID[%v] end[%v]", mp.PartitionID, mp.End)
}

func (mp *MetaPartition) getMetaReplica(addr string) (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.Addr == addr {
			return
		}
	}
	return nil, metaReplicaNotFound(addr)
}

func (mp *MetaPartition) removeMissingReplica(addr string) {
	if _, ok := mp.MissNodes[addr]; ok {
		delete(mp.MissNodes, addr)
	}
}

func (mp *MetaPartition) checkLeader() {
	mp.Lock()
	defer mp.Unlock()
	for _, mr := range mp.Replicas {
		if !mr.isActive() {
			mr.IsLeader = false
		}
	}
	return
}

func (mp *MetaPartition) checkStatus(clusterID string, writeLog bool, replicaNum int, maxPartitionID uint64) (doSplit bool) {
	mp.Lock()
	defer mp.Unlock()
	liveReplicas := mp.getLiveReplicas()
	if len(liveReplicas) <= replicaNum/2 {
		mp.Status = proto.Unavailable
	} else {
		mr, err := mp.getMetaReplicaLeader()
		if err != nil {
			mp.Status = proto.Unavailable
		}
		mp.Status = mr.Status
		for _, replica := range liveReplicas {
			if replica.Status == proto.ReadOnly {
				mp.Status = proto.ReadOnly
			}
			if mr.metaNode == nil {
				continue
			}
			if !mr.metaNode.reachesThreshold() && mp.InodeCount < defaultMetaPartitionInodeIDStep {
				continue
			}
			if mp.PartitionID == maxPartitionID {
				msg := fmt.Sprintf("split[checkStatus] need split,id:%v,status:%v,replicaNum:%v,InodeCount:%v",
					mp.PartitionID, mp.Status, mp.ReplicaNum, mp.InodeCount)
				log.LogInfo(msg)
				doSplit = true
			} else {
				if mr.metaNode.reachesThreshold() || mp.End - mp.MaxInodeID > 2 * defaultMetaPartitionInodeIDStep {
					msg := fmt.Sprintf("split[checkStatus],change state,id:%v,status:%v,replicaNum:%v,replicas:%v,persistenceHosts:%v, inodeCount:%v, MaxInodeID:%v, start:%v, end:%v",
						mp.PartitionID, mp.Status, mp.ReplicaNum, len(liveReplicas), mp.Hosts, mp.InodeCount, mp.MaxInodeID, mp.Start, mp.End)
						log.LogInfo(msg)
					mp.Status = proto.ReadOnly
				}
			}
		}
	}

	if mp.PartitionID >= maxPartitionID && mp.Status == proto.ReadOnly {
		mp.Status = proto.ReadWrite
	}
	if writeLog && len(liveReplicas) != int(mp.ReplicaNum) {
		msg := fmt.Sprintf("action[checkMPStatus],id:%v,status:%v,replicaNum:%v,replicas:%v,persistenceHosts:%v",
			mp.PartitionID, mp.Status, mp.ReplicaNum, len(liveReplicas), mp.Hosts)
		log.LogInfo(msg)
		Warn(clusterID, msg)
	}
	return
}

func (mp *MetaPartition) getMetaReplicaLeader() (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.IsLeader {
			return
		}
	}
	err = proto.ErrNoLeader
	return
}

func (mp *MetaPartition) checkReplicaNum(c *Cluster, volName string, replicaNum uint8) {
	mp.RLock()
	defer mp.RUnlock()
	if mp.ReplicaNum != replicaNum {
		msg := fmt.Sprintf("FIX MetaPartition replicaNum clusterID[%v] vol[%v] replica num[%v],current num[%v]",
			c.Name, volName, replicaNum, mp.ReplicaNum)
		Warn(c.Name, msg)
	}
}

func (mp *MetaPartition) removeIllegalReplica() (excessAddr string, t *proto.AdminTask, err error) {
	mp.RLock()
	defer mp.RUnlock()
	for _, mr := range mp.Replicas {
		if !contains(mp.Hosts, mr.Addr) {
			t = mr.createTaskToDeleteReplica(mp.PartitionID)
			err = proto.ErrIllegalMetaReplica
			break
		}
	}
	return
}

func (mp *MetaPartition) missingReplicaAddrs() (lackAddrs []string) {
	mp.RLock()
	defer mp.RUnlock()
	var liveReplicas []string
	for _, mr := range mp.Replicas {
		liveReplicas = append(liveReplicas, mr.Addr)
	}
	for _, host := range mp.Hosts {
		if !contains(liveReplicas, host) {
			lackAddrs = append(lackAddrs, host)
			break
		}
	}
	return
}

func (mp *MetaPartition) updateMetaPartition(mgr *proto.MetaPartitionReport, metaNode *MetaNode) {

	if !contains(mp.Hosts, metaNode.Addr) {
		return
	}
	mp.Lock()
	defer mp.Unlock()
	mr, err := mp.getMetaReplica(metaNode.Addr)
	if err != nil {
		mr = newMetaReplica(mp.Start, mp.End, metaNode)
		mp.addReplica(mr)
	}
	mr.updateMetric(mgr)
	mp.setMaxInodeID()
	mp.setInodeCount()
	mp.setDentryCount()
	mp.removeMissingReplica(metaNode.Addr)
}

func (mp *MetaPartition) canBeOffline(nodeAddr string, replicaNum int) (err error) {
	liveReplicas := mp.getLiveReplicas()
	if len(liveReplicas) < int(mp.ReplicaNum/2+1) {
		err = proto.ErrNoEnoughReplica
		return
	}
	liveAddrs := mp.getLiveReplicasAddr(liveReplicas)
	if len(liveReplicas) == (replicaNum/2+1) && contains(liveAddrs, nodeAddr) {
		err = fmt.Errorf("live replicas num will be less than majority after offline nodeAddr: %v", nodeAddr)
		return
	}
	return
}

// Check if there is a replica missing or not.
func (mp *MetaPartition) hasMissingOneReplica(replicaNum int) (err error) {
	hostNum := len(mp.Replicas)
	if hostNum <= replicaNum-1 {
		log.LogError(fmt.Sprintf("action[%v],partitionID:%v,err:%v",
			"hasMissingOneReplica", mp.PartitionID, proto.ErrHasOneMissingReplica))
		err = proto.ErrHasOneMissingReplica
	}
	return
}

func (mp *MetaPartition) getLiveReplicasAddr(liveReplicas []*MetaReplica) (addrs []string) {
	addrs = make([]string, 0)
	for _, mr := range liveReplicas {
		addrs = append(addrs, mr.Addr)
	}
	return
}
func (mp *MetaPartition) getLiveReplicas() (liveReplicas []*MetaReplica) {
	liveReplicas = make([]*MetaReplica, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive() {
			liveReplicas = append(liveReplicas, mr)
		}
	}
	return
}

func (mp *MetaPartition) persistToRocksDB(action, volName string, newHosts []string, newPeers []proto.Peer, c *Cluster) (err error) {
	oldHosts := make([]string, len(mp.Hosts))
	copy(oldHosts, mp.Hosts)
	oldPeers := make([]proto.Peer, len(mp.Peers))
	copy(oldPeers, mp.Peers)
	mp.Hosts = newHosts
	mp.Peers = newPeers
	if err = c.syncUpdateMetaPartition(mp); err != nil {
		mp.Hosts = oldHosts
		mp.Peers = oldPeers
		log.LogWarnf("action[%v_persist] failed,vol[%v] partitionID:%v  old hosts:%v new hosts:%v oldPeers:%v  newPeers:%v",
			action, volName, mp.PartitionID, mp.Hosts, newHosts, mp.Peers, newPeers)
		return
	}
	log.LogWarnf("action[%v_persist] success,vol[%v] partitionID:%v  old hosts:%v  new hosts:%v oldPeers:%v  newPeers:%v ",
		action, volName, mp.PartitionID, oldHosts, mp.Hosts, oldPeers, mp.Peers)
	return
}

func (mp *MetaPartition) getActiveAddrs() (liveAddrs []string) {
	liveAddrs = make([]string, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive() {
			liveAddrs = append(liveAddrs, mr.Addr)
		}
	}
	return liveAddrs
}

func (mp *MetaPartition) isMissingReplica(addr string) bool {
	return !contains(mp.getActiveAddrs(), addr)
}

func (mp *MetaPartition) shouldReportMissingReplica(addr string, interval int64) (isWarn bool) {
	lastWarningTime, ok := mp.MissNodes[addr]
	if !ok {
		isWarn = true
		mp.MissNodes[addr] = time.Now().Unix()
	} else if (time.Now().Unix() - lastWarningTime) > interval {
		isWarn = true
		mp.MissNodes[addr] = time.Now().Unix()
	}
	return false
}

func (mp *MetaPartition) reportMissingReplicas(clusterID, leaderAddr string, seconds, interval int64) {
	mp.Lock()
	defer mp.Unlock()
	for _, replica := range mp.Replicas {
		// reduce the alarm frequency
		if contains(mp.Hosts, replica.Addr) && replica.isMissing() && mp.shouldReportMissingReplica(replica.Addr, interval) {
			metaNode := replica.metaNode
			var (
				lastReportTime time.Time
			)
			isActive := true
			if metaNode != nil {
				lastReportTime = metaNode.ReportTime
				isActive = metaNode.IsActive
			}
			msg := fmt.Sprintf("action[reportMissingReplicas], clusterID[%v] volName[%v] partition:%v  on Node:%v  "+
				"miss time > :%v  vlocLastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v",
				clusterID, mp.volName, mp.PartitionID, replica.Addr, seconds, replica.ReportTime, lastReportTime, isActive)
			Warn(clusterID, msg)
			msg = fmt.Sprintf("decommissionMetaPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, mp.PartitionID, replica.Addr)
			Warn(clusterID, msg)
		}
	}

	for _, addr := range mp.Hosts {
		if mp.isMissingReplica(addr) && mp.shouldReportMissingReplica(addr, interval) {
			msg := fmt.Sprintf("action[reportMissingReplicas],clusterID[%v] volName[%v] partition:%v  on Node:%v  "+
				"miss time  > %v ",
				clusterID, mp.volName, mp.PartitionID, addr, defaultMetaPartitionTimeOutSec)
			Warn(clusterID, msg)
			msg = fmt.Sprintf("decommissionMetaPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, mp.PartitionID, addr)
			Warn(clusterID, msg)
		}
	}
}

func (mp *MetaPartition) replicaCreationTasks(clusterID, volName string) (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if addr, _, err := mp.removeIllegalReplica(); err != nil {
		msg = fmt.Sprintf("action[%v],clusterID[%v] metaPartition:%v  excess replication"+
			" on :%v  err:%v  persistenceHosts:%v",
			deleteIllegalReplicaErr, clusterID, mp.PartitionID, addr, err.Error(), mp.Hosts)
		log.LogWarn(msg)
	}
	if addrs := mp.missingReplicaAddrs(); addrs != nil {
		msg = fmt.Sprintf("action[missingReplicaAddrs],clusterID[%v] metaPartition:%v  lack replication"+
			" on :%v Hosts:%v",
			clusterID, mp.PartitionID, addrs, mp.Hosts)
		Warn(clusterID, msg)
	}

	return
}

func (mp *MetaPartition) buildNewMetaPartitionTasks(specifyAddrs []string, peers []proto.Peer, volName string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	hosts := make([]string, 0)
	req := &proto.CreateMetaPartitionRequest{
		Start:       mp.Start,
		End:         mp.End,
		PartitionID: mp.PartitionID,
		Members:     peers,
		VolName:     volName,
	}
	if specifyAddrs == nil {
		hosts = mp.Hosts
	} else {
		hosts = specifyAddrs
	}

	for _, addr := range hosts {
		t := proto.NewAdminTask(proto.OpCreateMetaPartition, addr, req)
		resetMetaPartitionTaskID(t, mp.PartitionID)
		tasks = append(tasks, t)
	}
	return
}

func (mp *MetaPartition) tryToChangeLeader(c *Cluster, metaNode *MetaNode) (err error) {
	task, err := mp.createTaskToTryToChangeLeader(metaNode.Addr)
	if err != nil {
		return
	}
	if _, err = metaNode.Sender.syncSendAdminTask(task); err != nil {
		return
	}
	return
}

func (mp *MetaPartition) createTaskToTryToChangeLeader(addr string) (task *proto.AdminTask, err error) {
	task = proto.NewAdminTask(proto.OpMetaPartitionTryToLeader, addr, nil)
	resetMetaPartitionTaskID(task, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToCreateReplica(host string) (t *proto.AdminTask, err error) {
	req := &proto.CreateMetaPartitionRequest{
		Start:       mp.Start,
		End:         mp.End,
		PartitionID: mp.PartitionID,
		Members:     mp.Peers,
		VolName:     mp.volName,
	}
	t = proto.NewAdminTask(proto.OpCreateMetaPartition, host, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToAddRaftMember(addPeer proto.Peer, leaderAddr string) (t *proto.AdminTask, err error) {
	req := &proto.AddMetaPartitionRaftMemberRequest{PartitionId: mp.PartitionID, AddPeer: addPeer}
	t = proto.NewAdminTask(proto.OpAddMetaPartitionRaftMember, leaderAddr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToRemoveRaftMember(removePeer proto.Peer) (t *proto.AdminTask, err error) {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		return nil, errors.NewError(err)
	}
	req := &proto.RemoveMetaPartitionRaftMemberRequest{PartitionId: mp.PartitionID, RemovePeer: removePeer}
	t = proto.NewAdminTask(proto.OpRemoveMetaPartitionRaftMember, mr.Addr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToDecommissionReplica(volName string, removePeer proto.Peer, addPeer proto.Peer) (t *proto.AdminTask, err error) {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		return nil, errors.NewError(err)
	}
	req := &proto.MetaPartitionDecommissionRequest{PartitionID: mp.PartitionID, VolName: volName, RemovePeer: removePeer, AddPeer: addPeer}
	t = proto.NewAdminTask(proto.OpDecommissionMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func resetMetaPartitionTaskID(t *proto.AdminTask, partitionID uint64) {
	t.ID = fmt.Sprintf("%v_pid[%v]", t.ID, partitionID)
	t.PartitionID = partitionID
}

func (mp *MetaPartition) createTaskToUpdateMetaReplica(clusterID string, partitionID uint64, end uint64) (t *proto.AdminTask) {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		msg := fmt.Sprintf("action[createTaskToUpdateMetaReplica] clusterID[%v] meta partition %v no leader",
			clusterID, mp.PartitionID)
		Warn(clusterID, msg)
		return
	}
	req := &proto.UpdateMetaPartitionRequest{PartitionID: partitionID, End: end, VolName: mp.volName}
	t = proto.NewAdminTask(proto.OpUpdateMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mr *MetaReplica) createTaskToDeleteReplica(partitionID uint64) (t *proto.AdminTask) {
	req := &proto.DeleteMetaPartitionRequest{PartitionID: partitionID}
	t = proto.NewAdminTask(proto.OpDeleteMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, partitionID)
	return
}

func (mr *MetaReplica) createTaskToLoadMetaPartition(partitionID uint64) (t *proto.AdminTask) {
	req := &proto.MetaPartitionLoadRequest{PartitionID: partitionID}
	t = proto.NewAdminTask(proto.OpLoadMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, partitionID)
	return
}
func (mr *MetaReplica) isMissing() (miss bool) {
	return time.Now().Unix()-mr.ReportTime > defaultMetaPartitionTimeOutSec
}

func (mr *MetaReplica) isActive() (active bool) {
	return mr.metaNode.IsActive && mr.Status != proto.Unavailable &&
		time.Now().Unix()-mr.ReportTime < defaultMetaPartitionTimeOutSec
}

func (mr *MetaReplica) setLastReportTime() {
	mr.ReportTime = time.Now().Unix()
}

func (mr *MetaReplica) updateMetric(mgr *proto.MetaPartitionReport) {
	mr.Status = (int8)(mgr.Status)
	mr.IsLeader = mgr.IsLeader
	mr.MaxInodeID = mgr.MaxInodeID
	mr.InodeCount = mgr.InodeCnt
	mr.DentryCount = mgr.DentryCnt
	mr.setLastReportTime()
}

func (mp *MetaPartition) afterCreation(nodeAddr string, c *Cluster) (err error) {
	metaNode, err := c.metaNode(nodeAddr)
	if err != nil {
		return err
	}
	mr := newMetaReplica(mp.Start, mp.End, metaNode)
	mr.Status = proto.ReadWrite
	mr.ReportTime = time.Now().Unix()
	mp.addReplica(mr)
	mp.removeMissingReplica(mr.Addr)
	return
}

func (mp *MetaPartition) addOrReplaceLoadResponse(response *proto.MetaPartitionLoadResponse) {
	mp.Lock()
	defer mp.Unlock()
	loadResponse := make([]*proto.MetaPartitionLoadResponse, 0)
	for _, lr := range mp.LoadResponse {
		if lr.Addr == response.Addr {
			continue
		}
		loadResponse = append(loadResponse, lr)
	}
	loadResponse = append(loadResponse, response)
	mp.LoadResponse = loadResponse
}

func (mp *MetaPartition) getMinusOfMaxInodeID() (minus float64) {
	mp.RLock()
	defer mp.RUnlock()
	var sentry float64
	for index, replica := range mp.Replicas {
		if index == 0 {
			sentry = float64(replica.MaxInodeID)
			continue
		}
		diff := math.Abs(float64(replica.MaxInodeID) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	return
}

func (mp *MetaPartition) setMaxInodeID() {
	var maxUsed uint64
	for _, r := range mp.Replicas {
		if r.MaxInodeID > maxUsed {
			maxUsed = r.MaxInodeID
		}
	}
	mp.MaxInodeID = maxUsed
}

func (mp *MetaPartition) setInodeCount() {
	var inodeCount uint64
	for _, r := range mp.Replicas {
		if r.InodeCount > inodeCount {
			inodeCount = r.InodeCount
		}
	}
	mp.InodeCount = inodeCount
}

func (mp *MetaPartition) setDentryCount() {
	var dentryCount uint64
	for _, r := range mp.Replicas {
		if r.DentryCount > dentryCount {
			dentryCount = r.DentryCount
		}
	}
	mp.DentryCount = dentryCount
}

func (mp *MetaPartition) getAllNodeSets() (nodeSets []uint64) {
	mp.RLock()
	defer mp.RUnlock()
	nodeSets = make([]uint64, 0)
	for _, mr := range mp.Replicas {
		if mr.metaNode == nil {
			continue
		}
		if !containsID(nodeSets, mr.metaNode.NodeSetID) {
			nodeSets = append(nodeSets, mr.metaNode.NodeSetID)
		}
	}
	return
}

func (mp *MetaPartition) getLiveZones(offlineAddr string) (zones []string) {
	mp.RLock()
	defer mp.RUnlock()
	for _, mr := range mp.Replicas {
		if mr.metaNode == nil {
			continue
		}
		if mr.Addr == offlineAddr {
			continue
		}
		zones = append(zones, mr.metaNode.ZoneName)
	}
	return
}
