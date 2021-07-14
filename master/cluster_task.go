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
	"runtime"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

func (c *Cluster) addDataNodeTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		c.addDataNodeTask(t)
	}
}

func (c *Cluster) addDataNodeTask(task *proto.AdminTask) {
	if task == nil {
		return
	}
	if node, err := c.dataNode(task.OperatorAddr); err != nil {
		log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", task.OperatorAddr, task.ID, err))
	} else {
		node.TaskManager.AddTask(task)
	}
}

func (c *Cluster) addMetaNodeTasks(tasks []*proto.AdminTask) {

	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.metaNode(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", t.OperatorAddr, t.ID, err.Error()))
		} else {
			node.Sender.AddTask(t)
		}
	}
}

func (c *Cluster) waitForResponseToLoadDataPartition(partitions []*DataPartition) {

	var wg sync.WaitGroup
	for _, dp := range partitions {
		wg.Add(1)
		go func(dp *DataPartition) {
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					const size = runtimeStackBufSize
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					log.LogError(fmt.Sprintf("doLoadDataPartition panic %v: %s\n", err, buf))
				}
			}()
			c.doLoadDataPartition(dp)
		}(dp)
	}
	wg.Wait()
}

func (c *Cluster) loadDataPartition(dp *DataPartition) {
	go func() {
		c.doLoadDataPartition(dp)
	}()
}

// taking the given mata partition offline.
// 1. checking if the meta partition can be offline.
// There are two cases where the partition is not allowed to be offline:
// (1) the replica is not in the latest host list
// (2) there are too few replicas
// 2. choosing a new available meta node
// 3. synchronized decommission meta partition
// 4. synchronized create a new meta partition
// 5. persistent the new host list
func (c *Cluster) decommissionMetaPartition(nodeAddr string, mp *MetaPartition) (err error) {
	var (
		newPeers        []proto.Peer
		metaNode        *MetaNode
		zone            *Zone
		ns              *nodeSet
		excludeNodeSets []uint64
		oldHosts        []string
		zones           []string
	)
	log.LogWarnf("action[decommissionMetaPartition],volName[%v],nodeAddr[%v],partitionID[%v] begin", mp.volName, nodeAddr, mp.PartitionID)
	mp.RLock()
	if !contains(mp.Hosts, nodeAddr) {
		mp.RUnlock()
		return
	}
	oldHosts = mp.Hosts
	mp.RUnlock()
	if err = c.validateDecommissionMetaPartition(mp, nodeAddr); err != nil {
		goto errHandler
	}
	if metaNode, err = c.metaNode(nodeAddr); err != nil {
		goto errHandler
	}
	if zone, err = c.t.getZone(metaNode.ZoneName); err != nil {
		goto errHandler
	}
	if ns, err = zone.getNodeSet(metaNode.NodeSetID); err != nil {
		goto errHandler
	}
	if _, newPeers, err = ns.getAvailMetaNodeHosts(oldHosts, 1); err != nil {
		if _, ok := c.vols[mp.volName]; !ok {
			log.LogWarnf("clusterID[%v] partitionID:%v  on Node:[%v]",
				c.Name, mp.PartitionID, mp.Hosts)
			return
		}
		if c.isFaultDomain(c.vols[mp.volName]) {
			log.LogWarnf("clusterID[%v] partitionID:%v  on Node:[%v]",
				c.Name, mp.PartitionID, mp.Hosts)
			return
		}
		// choose a meta node in other node set in the same zone
		excludeNodeSets = append(excludeNodeSets, ns.ID)
		if _, newPeers, err = zone.getAvailMetaNodeHosts(excludeNodeSets, oldHosts, 1); err != nil {
			zones = mp.getLiveZones(nodeAddr)
			var excludeZone []string
			if len(zones) == 0 {
				excludeZone = append(excludeZone, zone.name)
			} else {
				excludeZone = append(excludeZone, zones[0])
			}
			// choose a meta node in other zone
			if _, newPeers, err = c.chooseTargetMetaHosts(excludeZone, excludeNodeSets, oldHosts, 1, false, ""); err != nil {
				goto errHandler
			}
		}
	}
	if err = c.deleteMetaReplica(mp, nodeAddr, false); err != nil {
		goto errHandler
	}
	if err = c.addMetaReplica(mp, newPeers[0].Addr); err != nil {
		goto errHandler
	}
	mp.IsRecover = true
	c.putBadMetaPartitions(nodeAddr, mp.PartitionID)
	mp.RLock()
	c.syncUpdateMetaPartition(mp)
	mp.RUnlock()
	Warn(c.Name, fmt.Sprintf("action[decommissionMetaPartition] clusterID[%v] vol[%v] meta partition[%v] "+
		"offline addr[%v] success,new addr[%v]", c.Name, mp.volName, mp.PartitionID, nodeAddr, newPeers[0].Addr))
	return

errHandler:
	log.LogError(fmt.Sprintf("action[decommissionMetaPartition],volName: %v,partitionID: %v,err: %v",
		mp.volName, mp.PartitionID, errors.Stack(err)))
	Warn(c.Name, fmt.Sprintf("clusterID[%v] meta partition[%v] offline addr[%v] failed,err:%v",
		c.Name, mp.PartitionID, nodeAddr, err))
	if err != nil {
		err = fmt.Errorf("vol[%v],partition[%v],err[%v]", mp.volName, mp.PartitionID, err)
	}
	return
}

func (c *Cluster) validateDecommissionMetaPartition(mp *MetaPartition, nodeAddr string) (err error) {
	mp.RLock()
	defer mp.RUnlock()
	var vol *Vol
	if vol, err = c.getVol(mp.volName); err != nil {
		return
	}
	if err = mp.canBeOffline(nodeAddr, int(vol.mpReplicaNum)); err != nil {
		return
	}

	if err = mp.hasMissingOneReplica(int(vol.mpReplicaNum)); err != nil {
		return
	}

	if mp.IsRecover {
		err = fmt.Errorf("vol[%v],meta partition[%v] is recovering,[%v] can't be decommissioned", vol.Name, mp.PartitionID, nodeAddr)
		return
	}
	return
}

func (c *Cluster) checkCorruptMetaPartitions() (inactiveMetaNodes []string, corruptPartitions []*MetaPartition, err error) {
	partitionMap := make(map[uint64]uint8)
	inactiveMetaNodes = make([]string, 0)
	corruptPartitions = make([]*MetaPartition, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		if !metaNode.IsActive {
			inactiveMetaNodes = append(inactiveMetaNodes, metaNode.Addr)
		}
		return true
	})
	for _, addr := range inactiveMetaNodes {
		var metaNode *MetaNode
		if metaNode, err = c.metaNode(addr); err != nil {
			return
		}
		for _, partition := range metaNode.PersistenceMetaPartitions {
			partitionMap[partition] = partitionMap[partition] + 1
		}
	}

	for partitionID, badNum := range partitionMap {
		var partition *MetaPartition
		if partition, err = c.getMetaPartitionByID(partitionID); err != nil {
			return
		}
		if badNum > partition.ReplicaNum/2 {
			corruptPartitions = append(corruptPartitions, partition)
		}
	}
	log.LogInfof("clusterID[%v] inactiveMetaNodes:%v  corruptPartitions count:[%v]",
		c.Name, inactiveMetaNodes, len(corruptPartitions))
	return
}

// check corrupt partitions related to this meta node
func (c *Cluster) checkCorruptMetaNode(metaNode *MetaNode) (corruptPartitions []*MetaPartition, err error) {
	var (
		partition         *MetaPartition
		mn                *MetaNode
		corruptPids       []uint64
		corruptReplicaNum uint8
	)
	metaNode.RLock()
	defer metaNode.RUnlock()
	for _, pid := range metaNode.PersistenceMetaPartitions {
		corruptReplicaNum = 0
		if partition, err = c.getMetaPartitionByID(pid); err != nil {
			return
		}
		for _, host := range partition.Hosts {
			if mn, err = c.metaNode(host); err != nil {
				return
			}
			if !mn.IsActive {
				corruptReplicaNum = corruptReplicaNum + 1
			}
		}
		if corruptReplicaNum > partition.ReplicaNum/2 {
			corruptPartitions = append(corruptPartitions, partition)
			corruptPids = append(corruptPids, pid)
		}
	}
	log.LogInfof("action[checkCorruptMetaNode],clusterID[%v] metaNodeAddr:[%v], corrupt partitions%v",
		c.Name, metaNode.Addr, corruptPids)
	return
}

func (c *Cluster) checkLackReplicaMetaPartitions() (lackReplicaMetaPartitions []*MetaPartition, err error) {
	lackReplicaMetaPartitions = make([]*MetaPartition, 0)
	vols := c.copyVols()
	for _, vol := range vols {
		for _, mp := range vol.MetaPartitions {
			if mp.ReplicaNum > uint8(len(mp.Hosts)) {
				lackReplicaMetaPartitions = append(lackReplicaMetaPartitions, mp)
			}
		}
	}
	log.LogInfof("clusterID[%v] lackReplicaMetaPartitions count:[%v]", c.Name, len(lackReplicaMetaPartitions))
	return
}

func (c *Cluster) deleteMetaReplica(partition *MetaPartition, addr string, validate bool) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[deleteMetaReplica],vol[%v],data partition[%v],err[%v]", partition.volName, partition.PartitionID, err)
		}
	}()
	if validate {
		if err = c.validateDecommissionMetaPartition(partition, addr); err != nil {
			return
		}
	}
	metaNode, err := c.metaNode(addr)
	if err != nil {
		return
	}
	removePeer := proto.Peer{ID: metaNode.ID, Addr: addr}
	if err = c.removeMetaPartitionRaftMember(partition, removePeer); err != nil {
		return
	}
	if err = c.deleteMetaPartition(partition, metaNode); err != nil {
		return
	}
	return
}

func (c *Cluster) deleteMetaPartition(partition *MetaPartition, removeMetaNode *MetaNode) (err error) {
	partition.Lock()
	mr, err := partition.getMetaReplica(removeMetaNode.Addr)
	if err != nil {
		partition.Unlock()
		return
	}
	task := mr.createTaskToDeleteReplica(partition.PartitionID)
	partition.removeReplicaByAddr(removeMetaNode.Addr)
	partition.removeMissingReplica(removeMetaNode.Addr)
	partition.Unlock()
	_, err = removeMetaNode.Sender.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteMetaPartition] vol[%v],data partition[%v],err[%v]", partition.volName, partition.PartitionID, err)
	}
	return nil
}

func (c *Cluster) removeMetaPartitionRaftMember(partition *MetaPartition, removePeer proto.Peer) (err error) {
	partition.offlineMutex.Lock()
	defer partition.offlineMutex.Unlock()
	defer func(){
		if err1 := c.updateMetaPartitionOfflinePeerIDWithLock(partition, 0); err1 != nil {
			err = errors.Trace(err, "updateMetaPartitionOfflinePeerIDWithLock failed, err[%v]", err1)
		}
	}()
	if err = c.updateMetaPartitionOfflinePeerIDWithLock(partition, removePeer.ID); err != nil {
		return
	}
	mr, err := partition.getMetaReplicaLeader()
	if err != nil {
		return
	}
	t, err := partition.createTaskToRemoveRaftMember(removePeer)
	if err != nil {
		return
	}
	var leaderMetaNode *MetaNode
	leaderMetaNode = mr.metaNode
	if leaderMetaNode == nil {
		leaderMetaNode, err = c.metaNode(mr.Addr)
		if err != nil {
			return
		}
	}
	if _, err = leaderMetaNode.Sender.syncSendAdminTask(t); err != nil {
		return
	}
	newHosts := make([]string, 0, len(partition.Hosts)-1)
	newPeers := make([]proto.Peer, 0, len(partition.Hosts)-1)
	for _, host := range partition.Hosts {
		if host == removePeer.Addr {
			continue
		}
		newHosts = append(newHosts, host)
	}
	for _, peer := range partition.Peers {
		if peer.Addr == removePeer.Addr && peer.ID == removePeer.ID {
			continue
		}
		newPeers = append(newPeers, peer)
	}
	if err = partition.persistToRocksDB("removeMetaPartitionRaftMember", partition.volName, newHosts, newPeers, c); err != nil {
		return
	}
	if mr.Addr != removePeer.Addr {
		return
	}
	metaNode, err := c.metaNode(partition.Hosts[0])
	if err != nil {
		return
	}
	if err = partition.tryToChangeLeader(c, metaNode); err != nil {
		return
	}
	return
}

func (c *Cluster) updateMetaPartitionOfflinePeerIDWithLock(mp *MetaPartition, peerID uint64) (err error){
	mp.Lock()
	defer mp.Unlock()
	mp.OfflinePeerID = peerID
	if err = mp.persistToRocksDB("updateMetaPartitionOfflinePeerIDWithLock", mp.volName, mp.Hosts, mp.Peers, c); err != nil {
		return
	}
	return
}

func (c *Cluster) addMetaReplica(partition *MetaPartition, addr string) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addMetaReplica],vol[%v],data partition[%v],err[%v]", partition.volName, partition.PartitionID, err)
		}
	}()
	partition.Lock()
	defer partition.Unlock()
	if contains(partition.Hosts, addr) {
		err = fmt.Errorf("vol[%v],mp[%v] has contains host[%v]", partition.volName, partition.PartitionID, addr)
		return
	}
	metaNode, err := c.metaNode(addr)
	if err != nil {
		return
	}
	addPeer := proto.Peer{ID: metaNode.ID, Addr: addr}
	if err = c.addMetaPartitionRaftMember(partition, addPeer); err != nil {
		return
	}
	newHosts := make([]string, 0, len(partition.Hosts)+1)
	newPeers := make([]proto.Peer, 0, len(partition.Hosts)+1)
	newHosts = append(partition.Hosts, addPeer.Addr)
	newPeers = append(partition.Peers, addPeer)
	if err = partition.persistToRocksDB("addMetaReplica", partition.volName, newHosts, newPeers, c); err != nil {
		return
	}
	if err = c.createMetaReplica(partition, addPeer); err != nil {
		return
	}
	if err = partition.afterCreation(addPeer.Addr, c); err != nil {
		return
	}
	return
}

func (c *Cluster) createMetaReplica(partition *MetaPartition, addPeer proto.Peer) (err error) {
	task, err := partition.createTaskToCreateReplica(addPeer.Addr)
	if err != nil {
		return
	}
	metaNode, err := c.metaNode(addPeer.Addr)
	if err != nil {
		return
	}
	if _, err = metaNode.Sender.syncSendAdminTask(task); err != nil {
		return
	}
	return
}

func (c *Cluster) buildAddMetaPartitionRaftMemberTaskAndSyncSend(mp *MetaPartition, addPeer proto.Peer, leaderAddr string) (resp *proto.Packet, err error) {
	defer func() {
		var resultCode uint8
		if resp != nil {
			resultCode = resp.ResultCode
		}
		log.LogErrorf("action[addMetaRaftMemberAndSend],vol[%v],meta partition[%v],resultCode[%v],err[%v]", mp.volName, mp.PartitionID, resultCode, err)
	}()
	t, err := mp.createTaskToAddRaftMember(addPeer, leaderAddr)
	if err != nil {
		return
	}
	leaderMetaNode, err := c.metaNode(leaderAddr)
	if err != nil {
		return
	}
	if resp, err = leaderMetaNode.Sender.syncSendAdminTask(t); err != nil {
		return
	}
	return
}

func (c *Cluster) addMetaPartitionRaftMember(partition *MetaPartition, addPeer proto.Peer) (err error) {

	var (
		candidateAddrs []string
		leaderAddr     string
	)
	candidateAddrs = make([]string, 0, len(partition.Hosts))
	leaderMr, err := partition.getMetaReplicaLeader()
	if err == nil {
		leaderAddr = leaderMr.Addr
		if contains(partition.Hosts, leaderAddr) {
			candidateAddrs = append(candidateAddrs, leaderAddr)
		} else {
			leaderAddr = ""
		}
	}
	for _, host := range partition.Hosts {
		if host == leaderAddr {
			continue
		}
		candidateAddrs = append(candidateAddrs, host)
	}
	//send task to leader addr first,if need to retry,then send to other addr
	for index, host := range candidateAddrs {
		//wait for a new leader
		if leaderAddr == "" && len(candidateAddrs) < int(partition.ReplicaNum) {
			time.Sleep(retrySendSyncTaskInternal)
		}
		_, err = c.buildAddMetaPartitionRaftMemberTaskAndSyncSend(partition, addPeer, host)
		if err == nil {
			break
		}
		if index < len(candidateAddrs)-1 {
			time.Sleep(retrySendSyncTaskInternal)
		}
	}
	return
}

func (c *Cluster) loadMetaPartitionAndCheckResponse(mp *MetaPartition) {
	go func() {
		c.doLoadMetaPartition(mp)
	}()
}

func (c *Cluster) doLoadMetaPartition(mp *MetaPartition) {
	var wg sync.WaitGroup
	mp.Lock()
	hosts := make([]string, len(mp.Hosts))
	copy(hosts, mp.Hosts)
	mp.LoadResponse = make([]*proto.MetaPartitionLoadResponse, 0)
	mp.Unlock()
	errChannel := make(chan error, len(hosts))
	for _, host := range hosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			mr, err := mp.getMetaReplica(host)
			if err != nil {
				errChannel <- err
				return
			}
			task := mr.createTaskToLoadMetaPartition(mp.PartitionID)
			response, err := mr.metaNode.Sender.syncSendAdminTask(task)
			if err != nil {
				errChannel <- err
				return
			}
			loadResponse := &proto.MetaPartitionLoadResponse{}
			if err = json.Unmarshal(response.Data, loadResponse); err != nil {
				errChannel <- err
				return
			}
			loadResponse.Addr = host
			mp.addOrReplaceLoadResponse(loadResponse)
		}(host)
	}
	wg.Wait()
	select {
	case err := <-errChannel:
		msg := fmt.Sprintf("action[doLoadMetaPartition] vol[%v],mpID[%v],err[%v]", mp.volName, mp.PartitionID, err.Error())
		Warn(c.Name, msg)
		return
	default:
	}
	mp.checkSnapshot(c.Name)
}

func (c *Cluster) doLoadDataPartition(dp *DataPartition) {
	log.LogInfo(fmt.Sprintf("action[doLoadDataPartition],partitionID:%v", dp.PartitionID))
	if !dp.needsToCompareCRC() {
		log.LogInfo(fmt.Sprintf("action[doLoadDataPartition],partitionID:%v isRecover[%v] don't need compare", dp.PartitionID, dp.isRecover))
		return
	}
	dp.resetFilesWithMissingReplica()
	loadTasks := dp.createLoadTasks()
	c.addDataNodeTasks(loadTasks)
	for i := 0; i < timeToWaitForResponse; i++ {
		if dp.checkLoadResponse(c.cfg.DataPartitionTimeOutSec) {
			log.LogDebugf("action[checkLoadResponse]  all replica has responded,partitionID:%v ", dp.PartitionID)
			break
		}
		time.Sleep(time.Second)
	}

	if dp.checkLoadResponse(c.cfg.DataPartitionTimeOutSec) == false {
		return
	}

	dp.getFileCount()
	dp.validateCRC(c.Name)
	dp.checkReplicaSize(c.Name,c.cfg.diffSpaceUsage)
	dp.setToNormal()
}

func (c *Cluster) handleMetaNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf(fmt.Sprintf("action[handleMetaNodeTaskResponse] receive Task response:%v from %v", task.ID, nodeAddr))
	var (
		metaNode *MetaNode
	)

	if metaNode, err = c.metaNode(nodeAddr); err != nil {
		goto errHandler
	}
	metaNode.Sender.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpMetaNodeHeartbeat:
		response := task.Response.(*proto.MetaNodeHeartbeatResponse)
		err = c.dealMetaNodeHeartbeatResp(task.OperatorAddr, response)
	case proto.OpDeleteMetaPartition:
		response := task.Response.(*proto.DeleteMetaPartitionResponse)
		err = c.dealDeleteMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpUpdateMetaPartition:
		response := task.Response.(*proto.UpdateMetaPartitionResponse)
		err = c.dealUpdateMetaPartitionResp(task.OperatorAddr, response)
	default:
		err := fmt.Errorf("unknown operate code %v", task.OpCode)
		log.LogError(err)
	}

	if err != nil {
		log.LogError(fmt.Sprintf("process task[%v] failed", task.ToString()))
	} else {
		log.LogInfof("process task:%v status:%v success", task.ID, task.Status)
	}
	return
errHandler:
	log.LogError(fmt.Sprintf("action[handleMetaNodeTaskResponse],nodeAddr %v,taskId %v,err %v",
		nodeAddr, task.ID, err.Error()))
	return
}

func (c *Cluster) dealUpdateMetaPartitionResp(nodeAddr string, resp *proto.UpdateMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealUpdateMetaPartitionResp],clusterID[%v] nodeAddr %v update meta partition failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
	}
	return
}

func (c *Cluster) dealDeleteMetaPartitionResp(nodeAddr string, resp *proto.DeleteMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealDeleteMetaPartitionResp],clusterID[%v] nodeAddr %v "+
			"delete meta partition failed,err %v", c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	var mr *MetaReplica
	mp, err := c.getMetaPartitionByID(resp.PartitionID)
	if err != nil {
		goto errHandler
	}
	mp.Lock()
	defer mp.Unlock()
	if mr, err = mp.getMetaReplica(nodeAddr); err != nil {
		goto errHandler
	}
	mp.removeReplica(mr)
	return

errHandler:
	log.LogError(fmt.Sprintf("dealDeleteMetaPartitionResp %v", err))
	return
}

func (c *Cluster) dealMetaNodeHeartbeatResp(nodeAddr string, resp *proto.MetaNodeHeartbeatResponse) (err error) {
	var (
		metaNode *MetaNode
		logMsg   string
	)
	log.LogInfof("action[dealMetaNodeHeartbeatResp],clusterID[%v] receive nodeAddr[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealMetaNodeHeartbeatResp],clusterID[%v] nodeAddr %v heartbeat failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}

	if metaNode, err = c.metaNode(nodeAddr); err != nil {
		goto errHandler
	}

	if metaNode.ToBeOffline {
		return
	}
	if resp.ZoneName == "" {
		resp.ZoneName = DefaultZoneName
	}
	if metaNode.ZoneName != resp.ZoneName {
		c.t.deleteMetaNode(metaNode)
		oldZoneName := metaNode.ZoneName
		metaNode.ZoneName = resp.ZoneName
		c.adjustMetaNode(metaNode)
		log.LogWarnf("metaNode zone changed from [%v] to [%v]", oldZoneName, resp.ZoneName)
	}
	metaNode.updateMetric(resp, c.cfg.MetaNodeThreshold)
	metaNode.setNodeActive()

	if err = c.t.putMetaNode(metaNode); err != nil {
		log.LogErrorf("action[dealMetaNodeHeartbeatResp],metaNode[%v] error[%v]", metaNode.Addr, err)
	}
	c.updateMetaNode(metaNode, resp.MetaPartitionReports, metaNode.reachesThreshold())
	metaNode.metaPartitionInfos = nil
	logMsg = fmt.Sprintf("action[dealMetaNodeHeartbeatResp],metaNode:%v,zone[%v], ReportTime:%v  success", metaNode.Addr, metaNode.ZoneName, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, errors.Stack(err))
	log.LogError(logMsg)
	return
}

func (c *Cluster) adjustMetaNode(metaNode *MetaNode) {
	c.mnMutex.Lock()
	defer c.mnMutex.Unlock()
	oldNodeSetID := metaNode.NodeSetID
	var err error
	defer func() {
		if err != nil {
			err = fmt.Errorf("action[adjustMetaNode],clusterID[%v] addr:%v,zone[%v] err:%v ", c.Name, metaNode.Addr, metaNode.ZoneName, err.Error())
			log.LogError(errors.Stack(err))
			Warn(c.Name, err.Error())
		}
	}()
	var zone *Zone
	zone, err = c.t.getZone(metaNode.ZoneName)
	if err != nil {
		zone = newZone(metaNode.ZoneName)
		c.t.putZone(zone)
	}
	ns := zone.getAvailNodeSetForMetaNode()
	if ns == nil {
		if ns, err = zone.createNodeSet(c); err != nil {
			return
		}
	}

	metaNode.NodeSetID = ns.ID
	if err = c.syncUpdateMetaNode(metaNode); err != nil {
		metaNode.NodeSetID = oldNodeSetID
		return
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		return
	}
	err = c.t.putMetaNode(metaNode)
	return
}

func (c *Cluster) handleDataNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogInfof("action[handleDataNodeTaskResponse] receive addr[%v] task response,but task is nil", nodeAddr)
		return
	}
	log.LogDebugf("action[handleDataNodeTaskResponse] receive addr[%v] task response:%v", nodeAddr, task.ToString())
	var (
		err      error
		dataNode *DataNode
	)

	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		goto errHandler
	}
	dataNode.TaskManager.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpDeleteDataPartition:
		response := task.Response.(*proto.DeleteDataPartitionResponse)
		err = c.dealDeleteDataPartitionResponse(task.OperatorAddr, response)
	case proto.OpLoadDataPartition:
		response := task.Response.(*proto.LoadDataPartitionResponse)
		err = c.handleResponseToLoadDataPartition(task.OperatorAddr, response)
	case proto.OpDataNodeHeartbeat:
		response := task.Response.(*proto.DataNodeHeartbeatResponse)
		err = c.handleDataNodeHeartbeatResp(task.OperatorAddr, response)
	default:
		err = fmt.Errorf(fmt.Sprintf("unknown operate code %v", task.OpCode))
		goto errHandler
	}

	if err != nil {
		goto errHandler
	}
	return

errHandler:
	log.LogErrorf("process task[%v] failed,err:%v", task.ToString(), err)
	return
}

func (c *Cluster) dealDeleteDataPartitionResponse(nodeAddr string, resp *proto.DeleteDataPartitionResponse) (err error) {
	var (
		dp *DataPartition
	)
	if resp.Status == proto.TaskSucceeds {
		if dp, err = c.getDataPartitionByID(resp.PartitionId); err != nil {
			return
		}
		dp.Lock()
		defer dp.Unlock()
		dp.removeReplicaByAddr(nodeAddr)

	} else {
		Warn(c.Name, fmt.Sprintf("clusterID[%v] delete data partition[%v] failed,err[%v]", c.Name, nodeAddr, resp.Result))
	}

	return
}

func (c *Cluster) handleResponseToLoadDataPartition(nodeAddr string, resp *proto.LoadDataPartitionResponse) (err error) {
	if resp.Status == proto.TaskFailed || resp.PartitionSnapshot == nil {
		return
	}
	var (
		dataNode *DataNode
		dp       *DataPartition
		vol      *Vol
	)
	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		return
	}
	if resp.VolName != "" {
		vol, err = c.getVol(resp.VolName)
		if err != nil {
			return
		}
		dp, err = vol.getDataPartitionByID(resp.PartitionId)
	} else {
		dp, err = c.getDataPartitionByID(resp.PartitionId)
	}
	if err != nil {
		return
	}
	dp.loadFile(dataNode, resp)

	return
}

func (c *Cluster) handleDataNodeHeartbeatResp(nodeAddr string, resp *proto.DataNodeHeartbeatResponse) (err error) {

	var (
		dataNode *DataNode
		logMsg   string
	)
	log.LogInfof("action[handleDataNodeHeartbeatResp] clusterID[%v] receive dataNode[%v] heartbeat, ", c.Name, nodeAddr)
	if resp.Status != proto.TaskSucceeds {
		Warn(c.Name, fmt.Sprintf("action[handleDataNodeHeartbeatResp] clusterID[%v] dataNode[%v] heartbeat task failed",
			c.Name, nodeAddr))
		return
	}

	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		goto errHandler
	}
	if dataNode.ToBeOffline {
		return
	}
	if resp.ZoneName == "" {
		resp.ZoneName = DefaultZoneName
	}
	if dataNode.ZoneName != resp.ZoneName {
		c.t.deleteDataNode(dataNode)
		oldZoneName := dataNode.ZoneName
		dataNode.ZoneName = resp.ZoneName
		c.adjustDataNode(dataNode)
		log.LogWarnf("dataNode [%v] zone changed from [%v] to [%v]", dataNode.Addr, oldZoneName, resp.ZoneName)
	}

	dataNode.updateNodeMetric(resp)
	if err = c.t.putDataNode(dataNode); err != nil {
		log.LogErrorf("action[handleDataNodeHeartbeatResp] dataNode[%v],zone[%v],node set[%v], err[%v]", dataNode.Addr, dataNode.ZoneName, dataNode.NodeSetID, err)
	}
	c.updateDataNode(dataNode, resp.PartitionReports)
	logMsg = fmt.Sprintf("action[handleDataNodeHeartbeatResp],dataNode:%v,zone[%v], ReportTime:%v  success", dataNode.Addr, dataNode.ZoneName, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, err.Error())
	log.LogError(logMsg)
	return
}

func (c *Cluster) adjustDataNode(dataNode *DataNode) {
	c.dnMutex.Lock()
	defer c.dnMutex.Unlock()
	oldNodeSetID := dataNode.NodeSetID
	var err error
	defer func() {
		if err != nil {
			err = fmt.Errorf("action[adjustDataNode],clusterID[%v] dataNodeAddr:%v,zone[%v] err:%v ", c.Name, dataNode.Addr, dataNode.ZoneName, err.Error())
			log.LogError(errors.Stack(err))
			Warn(c.Name, err.Error())
		}
	}()
	var zone *Zone
	zone, err = c.t.getZone(dataNode.ZoneName)
	if err != nil {
		zone = newZone(dataNode.ZoneName)
		c.t.putZone(zone)
	}
	ns := zone.getAvailNodeSetForDataNode()
	if ns == nil {
		if ns, err = zone.createNodeSet(c); err != nil {
			return
		}
	}

	dataNode.NodeSetID = ns.ID
	if err = c.syncUpdateDataNode(dataNode); err != nil {
		dataNode.NodeSetID = oldNodeSetID
		return
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		return
	}
	err = c.t.putDataNode(dataNode)
	return
}

/*if node report data partition infos,so range data partition infos,then update data partition info*/
func (c *Cluster) updateDataNode(dataNode *DataNode, dps []*proto.PartitionReport) {
	for _, vr := range dps {
		if vr == nil {
			continue
		}
		if vr.VolName != "" {
			vol, err := c.getVol(vr.VolName)
			if err != nil {
				continue
			}
			if vol.Status == markDelete {
				continue
			}
			if dp, err := vol.getDataPartitionByID(vr.PartitionID); err == nil {
				dp.updateMetric(vr, dataNode, c)
			}
		} else {
			if dp, err := c.getDataPartitionByID(vr.PartitionID); err == nil {
				dp.updateMetric(vr, dataNode, c)
			}
		}
	}
}

func (c *Cluster) updateMetaNode(metaNode *MetaNode, metaPartitions []*proto.MetaPartitionReport, threshold bool) {
	var (
		vol *Vol
		err error
	)
	for _, mr := range metaPartitions {
		if mr == nil {
			continue
		}
		var mp *MetaPartition
		if mr.VolName != "" {
			vol, err = c.getVol(mr.VolName)
			if err != nil {
				continue
			}
			if vol.Status == markDelete {
				continue
			}
			mp, err = vol.metaPartition(mr.PartitionID)
			if err != nil {
				continue
			}
		} else {
			mp, err = c.getMetaPartitionByID(mr.PartitionID)
			if err != nil {
				continue
			}
		}

		//send latest end to replica
		if mr.End != mp.End {
			mp.addUpdateMetaReplicaTask(c)
		}
		mp.updateMetaPartition(mr, metaNode)
		c.updateInodeIDUpperBound(mp, mr, threshold, metaNode)
	}
}

func (c *Cluster) updateInodeIDUpperBound(mp *MetaPartition, mr *proto.MetaPartitionReport, hasArriveThreshold bool, metaNode *MetaNode) (err error) {
	if !hasArriveThreshold {
		return
	}
	var vol *Vol
	if vol, err = c.getVol(mp.volName); err != nil {
		log.LogWarnf("action[updateInodeIDRange] vol[%v] not found", mp.volName)
		return
	}
	maxPartitionID := vol.maxPartitionID()
	if mr.PartitionID < maxPartitionID {
		return
	}
	var end uint64
	if mr.MaxInodeID <= 0 {
		end = mr.Start + defaultMetaPartitionInodeIDStep
	} else {
		end = mr.MaxInodeID + defaultMetaPartitionInodeIDStep
	}
	log.LogWarnf("mpId[%v],start[%v],end[%v],addr[%v],used[%v]", mp.PartitionID, mp.Start, mp.End, metaNode.Addr, metaNode.Used)
	if err = vol.splitMetaPartition(c, mp, end); err != nil {
		log.LogError(err)
	}
	return
}
