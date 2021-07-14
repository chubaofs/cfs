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
	"container/list"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"sort"
	"sync"
)

type topology struct {
	dataNodes            *sync.Map
	metaNodes            *sync.Map
	zoneMap              *sync.Map
	zoneIndexForDataNode int
	zoneIndexForMetaNode int
	zones                []*Zone
	domainExcludeZones   []string
	zoneLock             sync.RWMutex
}

func newTopology() (t *topology) {
	t = new(topology)
	t.zoneMap = new(sync.Map)
	t.dataNodes = new(sync.Map)
	t.metaNodes = new(sync.Map)
	t.zones = make([]*Zone, 0)
	return
}

func (t *topology) zoneLen() int {
	t.zoneLock.RLock()
	defer t.zoneLock.RUnlock()
	return len(t.zones)
}

func (t *topology) clear() {
	t.dataNodes.Range(func(key, value interface{}) bool {
		t.dataNodes.Delete(key)
		return true
	})
	t.metaNodes.Range(func(key, value interface{}) bool {
		t.metaNodes.Delete(key)
		return true
	})
}

func (t *topology) putZone(zone *Zone) (err error) {
	t.zoneLock.Lock()
	defer t.zoneLock.Unlock()
	if _, ok := t.zoneMap.Load(zone.name); ok {
		return fmt.Errorf("zone[%v] has exist", zone.name)
	}
	t.zoneMap.Store(zone.name, zone)
	t.zones = append(t.zones, zone)
	return
}

func (t *topology) putZoneIfAbsent(zone *Zone) (beStoredZone *Zone) {
	t.zoneLock.Lock()
	defer t.zoneLock.Unlock()
	oldZone, ok := t.zoneMap.Load(zone.name)
	if ok {
		return oldZone.(*Zone)
	}
	t.zoneMap.Store(zone.name, zone)
	t.zones = append(t.zones, zone)
	beStoredZone = zone
	return
}

func (t *topology) getZone(name string) (zone *Zone, err error) {
	t.zoneMap.Range(func(zoneName, value interface{}) bool {
		if zoneName != name {
			return true
		}
		zone = value.(*Zone)
		return true
	})
	if zone == nil {
		return nil, fmt.Errorf("zone[%v] is not found", name)
	}
	return
}

func (t *topology) putDataNode(dataNode *DataNode) (err error) {

	if _, ok := t.dataNodes.Load(dataNode.Addr); ok {
		return
	}
	zone, err := t.getZone(dataNode.ZoneName)
	if err != nil {
		return
	}

	zone.putDataNode(dataNode)
	t.putDataNodeToCache(dataNode)
	return
}

func (t *topology) putDataNodeToCache(dataNode *DataNode) {
	t.dataNodes.Store(dataNode.Addr, dataNode)
}

func (t *topology) deleteDataNode(dataNode *DataNode) {
	zone, err := t.getZone(dataNode.ZoneName)
	if err != nil {
		return
	}
	zone.deleteDataNode(dataNode)
	t.dataNodes.Delete(dataNode.Addr)
}

func (t *topology) getZoneByDataNode(dataNode *DataNode) (zone *Zone, err error) {
	_, ok := t.dataNodes.Load(dataNode.Addr)
	if !ok {
		return nil, errors.Trace(dataNodeNotFound(dataNode.Addr), "%v not found", dataNode.Addr)
	}

	return t.getZone(dataNode.ZoneName)
}

func (t *topology) putMetaNode(metaNode *MetaNode) (err error) {
	if _, ok := t.metaNodes.Load(metaNode.Addr); ok {
		return
	}
	zone, err := t.getZone(metaNode.ZoneName)
	if err != nil {
		return
	}
	zone.putMetaNode (metaNode)
	t.putMetaNodeToCache(metaNode)
	return
}

func (t *topology) deleteMetaNode(metaNode *MetaNode) {
	t.metaNodes.Delete(metaNode.Addr)
	zone, err := t.getZone(metaNode.ZoneName)
	if err != nil {
		return
	}
	zone.deleteMetaNode(metaNode)
}

func (t *topology) putMetaNodeToCache(metaNode *MetaNode) {
	t.metaNodes.Store(metaNode.Addr, metaNode)
}

type nodeSetCollection []*nodeSet

func (nsc nodeSetCollection) Len() int {
	return len(nsc)
}

func (nsc nodeSetCollection) Less(i, j int) bool {
	return nsc[i].metaNodeLen() < nsc[j].metaNodeLen()
}

func (nsc nodeSetCollection) Swap(i, j int) {
	nsc[i], nsc[j] = nsc[j], nsc[i]
}

type nodeSetGroup struct {
	ID		uint64
	nsgInnerIndex	int //worked if alloc num of replica not equal with stardard set num of nsg
	nodeSets	[]*nodeSet
	nodeSetsIds	[]uint64
	status		uint8
	sync.RWMutex
}
func newNodeSetGrp(c *Cluster)	*nodeSetGroup  {
	var id uint64
	var err error
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		return nil
	}
	log.LogInfof("action[newNodeSetGrp] construct,id[%v]", id)
	nsg := &nodeSetGroup{
		ID: id,
		status: normal,
	}
	return nsg
}

type nodeSetGrpManager struct {
	c                     *Cluster
	nsgIndex              int					// alloc host from  avliable nodesetGrp with banlance policy
	init                  bool				// manager  cann't be used in some startup stage before load
	nodeSetGrpMap         []*nodeSetGroup
	zoneAvailableNodeSet  map[string]*list.List
	nsId2NsGrpMap         map[uint64]int // map nodeset id  to nodeset group index in nodeSetGrpMap
	excludeZoneListDomain map[string]int // upgrade old datastore old zones use old policy
	lastBuildIndex        int            // build index for 2 plus 1 policy,multi zones need banlance build
	status                uint8          // all nodesetGrp may be unavaliable or no nodesetGrp be exist on given policy
	nsIdMap               map[uint64]int // store all ns alreay be put into manager
	dataRatioLimit        float64
	excludeZoneUseRatio   float64
	sync.RWMutex
}

func newNodeSetGrpManager(cls *Cluster) *nodeSetGrpManager {
	log.LogInfof("action[newNodeSetGrpManager] construct")
	ns := &nodeSetGrpManager{
		c:                     cls,
		nsgIndex:              0,
		zoneAvailableNodeSet:  make(map[string]*list.List),
		nsId2NsGrpMap:         make(map[uint64]int),
		excludeZoneListDomain: make(map[string]int),
		nsIdMap:               make(map[uint64]int),
		dataRatioLimit:        defaultDomainUsageThreshold,
		excludeZoneUseRatio:   defaultZoneUsageThreshold,
	}
	return ns
}

func (nsgm *nodeSetGrpManager) start() {
	log.LogInfof("action[nodeSetGrpManager:start] start")
	nsgm.init = true
}

func (nsgm *nodeSetGrpManager) checkExcludeZoneState() {
	if len(nsgm.excludeZoneListDomain) == 0 {
		log.LogInfof("action[checkExcludeZoneState] no exclueZoneList for Domain,size zero")
		return
	}
	var (
		excludeNeedDomain = true
	)
	log.LogInfof("action[checkExcludeZoneState] exclueZoneList size[%v]", len(nsgm.excludeZoneListDomain))
	for zoneNm := range nsgm.excludeZoneListDomain {
		if value, ok := nsgm.c.t.zoneMap.Load(zoneNm); ok {
			zone := value.(*Zone)
			log.LogInfof("action[checkExcludeZoneState] zone name[%v],status[%v], index for datanode[%v],index for metanode[%v]",
				zone.name, zone.status, zone.setIndexForDataNode, zone.setIndexForMetaNode)
			if nsgm.excludeZoneUseRatio == 0 || nsgm.excludeZoneUseRatio > 1 {
				nsgm.excludeZoneUseRatio = defaultZoneUsageThreshold
			}
			if zone.isUsedRatio(nsgm.excludeZoneUseRatio) {
				if zone.status == normalZone {
					log.LogInfof("action[checkExcludeZoneState] zone[%v] be set unavailableZone", zone.name)
				}
				zone.status = unavailableZone
			} else {
				excludeNeedDomain = false
				if zone.status == unavailableZone {
					log.LogInfof("action[checkExcludeZoneState] zone[%v] be set normalZone", zone.name)
				}
				zone.status = normalZone
			}
		}
	}
	if excludeNeedDomain {
		log.LogInfof("action[checkExcludeZoneState] exclude zone cann't be used since now!excludeNeedDomain[%v]",
			excludeNeedDomain)
		nsgm.c.needFaultDomain = true
	} else {
		if nsgm.c.needFaultDomain == true {
			log.LogInfof("action[checkExcludeZoneState] needFaultDomain be set false")
		}
		nsgm.c.needFaultDomain = false
	}
}

func (nsgm *nodeSetGrpManager) checkGrpState() {
	nsgm.RLock()
	defer nsgm.RUnlock()
	if len(nsgm.nodeSetGrpMap) == 0 {
		log.LogInfof("action[checkGrpState] leave,size zero")
		return
	}
	log.LogInfof("action[checkGrpState] nodeSetGrpMap size [%v]", len(nsgm.nodeSetGrpMap))
	metaUnAvailableCnt := 0
	dataUnAvailableCnt := 0
	for i := 0; i < len(nsgm.nodeSetGrpMap); i++ {
		log.LogInfof("action[checkGrpState] nodesetgrp index[%v], id[%v], status[%v]",
					i, nsgm.nodeSetGrpMap[i].ID, nsgm.nodeSetGrpMap[i].status)
		grpStatus := normal
		grpMetaUnAvailableCnt := 0
		for j := 0; j < len(nsgm.nodeSetGrpMap[i].nodeSets); j++ {
			var (
				metaWorked bool
				dataWorked bool
				used 	uint64
				total 	uint64
			)
			nsgm.nodeSetGrpMap[i].nodeSets[j].dataNodes.Range(func(key, value interface{}) bool {
				node := value.(*DataNode)
				if node.isWriteAble() {
					used = used + node.Used
				} else {
					used = used + node.Total
				}
				total = total + node.Total

				log.LogInfof("action[checkGrpState] nodeid[%v] zonename[%v] used [%v] total [%v] UsageRatio [%v] got avaliable metanode",
					node.ID, node.ZoneName, node.Used, node.Total, node.UsageRatio)
				return true
			})

			if float64(used) / float64(total) <  nsgm.dataRatioLimit {
				log.LogInfof("action[checkGrpState] nodeset id [%v] zonename[%v] is fine. used [%v] total [%v] UsageRatio [%v] got avaliable datanode",
					nsgm.nodeSetGrpMap[i].nodeSets[j].ID, nsgm.nodeSetGrpMap[i].nodeSets[j].zoneName, used, total, float64(used) / float64(total))
				dataWorked = true
			}
			nsgm.nodeSetGrpMap[i].nodeSets[j].metaNodes.Range(func(key, value interface{}) bool {
				node := value.(*MetaNode)
				if node.isWritable() {
					metaWorked = true
					log.LogInfof("action[checkGrpState] nodeset[%v] zonename[%v] used [%v] total [%v] threshold [%v] got avaliable metanode",
								node.ID, node.ZoneName, node.Used, node.Total, node.Threshold)
					return false
				}
				log.LogInfof("action[checkGrpState] nodeset[%v] zonename[%v] used [%v] total [%v] threshold [%v] got avaliable metanode",
					node.ID, node.ZoneName, node.Used, node.Total, node.Threshold)
				return true
			})
			if !metaWorked || !dataWorked {
				log.LogInfof("action[checkGrpState] nodesetgrp index[%v], id[%v], status[%v] be set metaWorked[%v] dataWorked[%v]",
					i, nsgm.nodeSetGrpMap[i].ID, nsgm.nodeSetGrpMap[i].status, metaWorked, dataWorked)
				if !metaWorked {
					grpMetaUnAvailableCnt++
					if grpMetaUnAvailableCnt == 2 { // meta can be used if one node is not active
						if grpStatus == dataNodesUnavaliable {
							log.LogInfof("action[checkGrpState] nodesetgrp index[%v], id[%v], grp status change from dataNodesUnavaliable to unavaliable",
								i, nsgm.nodeSetGrpMap[i].ID)
							grpStatus = unavaliable
							break
						}
						log.LogInfof("action[checkGrpState] nodesetgrp index[%v], id[%v], grp status be set metaNodesUnavaliable",
								i, nsgm.nodeSetGrpMap[i].ID)
						grpStatus = metaNodesUnavaliable
						metaUnAvailableCnt++
					}
				}
				if !dataWorked && grpStatus != dataNodesUnavaliable{
					if grpStatus == metaNodesUnavaliable {
						log.LogInfof("action[checkGrpState] nodesetgrp index[%v], id[%v], grp status change from metaNodesUnavaliable to unavaliable",
							i, nsgm.nodeSetGrpMap[i].ID)
						grpStatus = unavaliable
						break
					}
					log.LogInfof("action[checkGrpState] nodesetgrp index[%v], id[%v], grp status be set dataNodesUnavaliable",
						i, nsgm.nodeSetGrpMap[i].ID)
					grpStatus = dataNodesUnavaliable
					dataUnAvailableCnt++
				}
			}
		}
		nsgm.nodeSetGrpMap[i].status = grpStatus
		log.LogInfof("action[checkGrpState] nodesetgrp index[%v], id[%v], status[%v] be set normal",
			i, nsgm.nodeSetGrpMap[i].ID, nsgm.nodeSetGrpMap[i].status)
	}

	nsgm.status = normal
	if dataUnAvailableCnt == len(nsgm.nodeSetGrpMap) {
		nsgm.status =  dataNodesUnavaliable
	}
	if metaUnAvailableCnt == len(nsgm.nodeSetGrpMap) {
		if nsgm.status ==  dataNodesUnavaliable {
			nsgm.status = unavaliable
		} else {
			nsgm.status = metaNodesUnavaliable
		}
	}
	log.LogInfof("action[checkGrpState] nodesetgrp size [%v] dataUnAvailableCnt [%v] metaUnAvailableCnt [%v] nsgm.status now[%v]",
		len(nsgm.nodeSetGrpMap), dataUnAvailableCnt, metaUnAvailableCnt, nsgm.status)
}
type buildNodeSetGrpMethod func(nsgm *nodeSetGrpManager) (err error)
func (nsgm *nodeSetGrpManager) buildNodeSetGrp() (err error) {
	log.LogInfof("action[buildNodeSetGrp] avaliable zone [%v]", len(nsgm.zoneAvailableNodeSet))
	if len(nsgm.zoneAvailableNodeSet) == 0 {
		err = fmt.Errorf("action[buildNodeSetGrp] failed zone avaliable zero")
		log.LogErrorf("[%v]", err)
		return
	}

	var method map[int]buildNodeSetGrpMethod
	method = make(map[int]buildNodeSetGrpMethod)
	method[3] = buildNodeSetGrp3Zone
	method[2] = buildNodeSetGrp2Plus1
	method[1] = buildNodeSetGrpOneZone
	step := defaultNodeSetGrpStep

	zoneCnt := nsgm.c.cfg.DomainNodeGrpBatchCnt
	log.LogInfof("action[buildNodeSetGrp] zoncnt [%v]", zoneCnt)
	if zoneCnt >= 3 {
		zoneCnt = 3
	}
	if zoneCnt > len(nsgm.zoneAvailableNodeSet) {
		if nsgm.c.cfg.DomainBuildAsPossible {
			log.LogInfof("action[buildNodeSetGrp] zoncnt [%v]", zoneCnt)
			zoneCnt = len(nsgm.zoneAvailableNodeSet)
		} else {
			err = fmt.Errorf("action[buildNodeSetGrp] failed zone avaliable [%v] need [%v]", zoneCnt, len(nsgm.zoneAvailableNodeSet))
			log.LogErrorf("[%v]", err)
			return
		}
	}
	for {
		log.LogInfof("action[buildNodeSetGrp] zoneCnt [%v] step [%v]", zoneCnt, step)
		err = method[zoneCnt](nsgm)
		if err != nil {
			log.LogInfof("action[buildNodeSetGrp] err [%v]", err)
			break
		}
		step--
		if step == 0 {
			break
		}
	}
	if nsgm.status != normal || len(nsgm.nodeSetGrpMap) == 0 {
		return fmt.Errorf("cann't build new group [%v]", err)
	}

	return nil
}

func (nsgm *nodeSetGrpManager) getHostFromNodeSetGrpSpecific(replicaNum uint8, createType uint32) (
			hosts []string,
			peers []proto.Peer,
			err error){
	log.LogErrorf("action[getHostFromNodeSetGrpSpecfic]  replicaNum[%v],type[%v], nsg cnt[%v], nsg status[%v]",
		replicaNum, createType, len(nsgm.nodeSetGrpMap), nsgm.status)
	if len(nsgm.nodeSetGrpMap) == 0 {
		return
	}

	nsgm.RLock()
	defer nsgm.RUnlock()

	var cnt int
	nsgIndex := nsgm.nsgIndex
	nsgm.nsgIndex = (nsgm.nsgIndex+1) % len(nsgm.nodeSetGrpMap)

	for {
		if cnt >= len(nsgm.nodeSetGrpMap) {
			log.LogInfof("action[getHostFromNodeSetGrpSpecfic] failed all nsGrp unavailable,cnt[%v]", cnt)
			err = fmt.Errorf("action[getHostFromNodeSetGrpSpecfic],err:no nsGrp status normal,cnt[%v]",cnt)
			break
		}
		cnt++
		nsgIndex = (nsgIndex+1) % len(nsgm.nodeSetGrpMap)
		nsg :=  nsgm.nodeSetGrpMap[nsgIndex]

		needReplicaNumArray := [3]int{1,2,3}
		for _, needReplicaNum := range needReplicaNumArray {
			var (
				host []string
				peer []proto.Peer
			)
			// every replica will look around every nodeset and break if get one
			for i := 0; i < defaultFaultDomainZoneCnt; i++ {
				ns := nsg.nodeSets[nsg.nsgInnerIndex]
				nsg.nsgInnerIndex = (nsg.nsgInnerIndex + 1) % defaultFaultDomainZoneCnt
				log.LogInfof("action[getHostFromNodeSetGrpSpecfic]  nodesetid[%v],zonename[%v], datanode len[%v],metanode len[%v],capcity[%v]",
					ns.ID, ns.zoneName, ns.dataNodeLen(), ns.metaNodeLen(), ns.Capacity)

				needNum := needReplicaNum
				if needReplicaNum > int(replicaNum)-len(hosts) {
					needNum = int(replicaNum)-len(hosts)
				}

				if createType == TypeDataPartion {
					if host, peer, err = ns.getAvailDataNodeHosts(nil, needNum); err != nil {
						log.LogErrorf("action[getHostFromNodeSetGrpSpecfic] ns[%v] zone[%v] TypeDataPartion err[%v]", ns.ID, ns.zoneName, err)
						//nsg.status = dataNodesUnavaliable
						continue
					}
				} else {
					if host, peer, err = ns.getAvailMetaNodeHosts(nil, needNum); err != nil {
						log.LogErrorf("action[getHostFromNodeSetGrpSpecfic]  ns[%v] zone[%v] TypeMetaPartion err[%v]", ns.ID, ns.zoneName, err)
						//nsg.status = metaNodesUnavaliable
						continue
					}
				}
				hosts = append(hosts, host...)
				peers = append(peers, peer...)
				if int(replicaNum) == len(hosts) {
					log.LogInfof("action[getHostFromNodeSetGrpSpecfic]  ngGrp[%v] unable support type[%v] replicaNum[%v]", nsg.ID, createType, replicaNum)
					return
				}
			}
			hosts = nil
			peers = nil
		}

	}

	return nil, nil, fmt.Errorf("action[getHostFromNodeSetGrpSpecfic] cann't alloc host")
}

func (nsgm *nodeSetGrpManager) getHostFromNodeSetGrp(replicaNum uint8, createType uint32) (
		hosts []string,
		peers []proto.Peer,
		err error){

	log.LogInfof("action[getHostFromNodeSetGrp]  replicaNum[%v],type[%v], nsg cnt[%v], nsg status[%v]",
				replicaNum, createType, len(nsgm.nodeSetGrpMap), nsgm.status)

	// this scenario is abnormal  may be caused by zone unavailable in high probability
	if nsgm.status != normal {
		return nsgm.getHostFromNodeSetGrpSpecific(replicaNum, createType)
	}
	// grp map be build with three zone on standard,no grp if zone less than three,here will build
	// nodesetGrp with zones less than three,because offer service is much more important than high available
	if len(nsgm.zoneAvailableNodeSet) != 0 {
		if nsgm.buildNodeSetGrp(); len(nsgm.nodeSetGrpMap) == 0 {
			err = fmt.Errorf("no usable group and build failed")
			log.LogInfof("action[getHostFromNodeSetGrp] err[%v]", err)
			return
		}
	} else if len(nsgm.nodeSetGrpMap) == 0 {
		err = fmt.Errorf("no usable group")
		log.LogInfof("action[getHostFromNodeSetGrp] err[%v]", err)
		return
	}

	nsgm.RLock()
	defer nsgm.RUnlock()

	var cnt int
	nsgIndex := nsgm.nsgIndex
	nsgm.nsgIndex = (nsgm.nsgIndex+1) % len(nsgm.nodeSetGrpMap)

	for {
		if cnt >= len(nsgm.nodeSetGrpMap) {
			log.LogInfof("action[getHostFromNodeSetGrp] failed all unavailable,cnt[%v]", cnt)
			err = fmt.Errorf("action[getHostFromNodeSetGrp],err:no grp status normal,cnt[%v]",cnt)
			//nsgm.status = unavaliable
			return
		}
		cnt++
		nsgIndex = (nsgIndex+1) % len(nsgm.nodeSetGrpMap)
		nsg :=  nsgm.nodeSetGrpMap[nsgIndex]

		var (
			host []string
			peer []proto.Peer
		)
		var i uint8
		for i = 0; i < replicaNum; i++ {
			ns := nsg.nodeSets[nsg.nsgInnerIndex]
			log.LogInfof("action[getHostFromNodeSetGrp]  nodesetid[%v],zonename[%v], datanode len[%v],metanode len[%v],capcity[%v]",
							ns.ID, ns.zoneName, ns.dataNodeLen(), ns.metaNodeLen(), ns.Capacity)
			nsg.nsgInnerIndex = (nsg.nsgInnerIndex+1) % defaultFaultDomainZoneCnt
			if createType == TypeDataPartion {
				if host, peer, err = ns.getAvailDataNodeHosts(nil, 1); err != nil {
					log.LogErrorf("action[getHostFromNodeSetGrp] ns[%v] zone[%v] TypeDataPartion err[%v]", ns.ID, ns.zoneName, err)
					//nsg.status = dataNodesUnavaliable
					break
				}
			} else {
				if host, peer, err = ns.getAvailMetaNodeHosts(nil, 1); err != nil {
					log.LogErrorf("action[getHostFromNodeSetGrp]  ns[%v] zone[%v] TypeMetaPartion err[%v]", ns.ID, ns.zoneName, err)
					//nsg.status = metaNodesUnavaliable
					break
				}
			}
			hosts = append(hosts, host[0])
			peers = append(peers, peer[0])
			log.LogInfof("action[getHostFromNodeSetGrp]  get host[%v] peer[%v], nsg id[%v] nsgInnerIndex[%v]", host[0], peer[0], nsg.ID, nsg.nsgInnerIndex)
		}
		if i == replicaNum {
			return hosts, peers, nil
		}
	}
}
// nodeset may not
type nsList struct {
	lst 	*list.List
	ele  	*list.Element
	zoneName string
}

func (nsgm *nodeSetGrpManager) buildNodeSetGrpPrepare() (buildIndex int, zoneAvaVec []nsList){
	sortedKeys := make([]string, 0)
	for k, _ := range nsgm.zoneAvailableNodeSet {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	for _, zoneName := range sortedKeys {
		var zoneInfo nsList
		zoneInfo.lst = nsgm.zoneAvailableNodeSet[zoneName]
		zoneInfo.zoneName = zoneName
		zoneAvaVec = append(zoneAvaVec, zoneInfo)
	}
	buildIndex = nsgm.lastBuildIndex % len(zoneAvaVec)
	nsgm.lastBuildIndex = (nsgm.lastBuildIndex + 1) % len(zoneAvaVec)
	return
}

func (nsgm *nodeSetGrpManager) buildNodeSetGrpDoWork(zoneName string, nodeList *list.List, needCnt int) (resList []nsList, err error) {
	log.LogInfof("action[buildNodeSetGrpDoWork] step in")
	var tmpList []nsList
	ele := nodeList.Front()
	for {
		if ele == nil {
			log.LogInfof("action[buildNodeSetGrpDoWork] zone [%v] can't create nodeset group nodeList not qualified", zoneName)
			err = fmt.Errorf("action[buildNodeSetGrpDoWork] zone [%v] can't create nodeset group nodeList not qualified", zoneName)
			return
		}
		nst := ele.Value.(*nodeSet)
		log.LogInfof("action[buildNodeSetGrpDoWork] nodeset [%v] zonename [%v] ,metacnt[%v],datacnt[%v]",
			nst.ID, nst.zoneName,nst.metaNodeLen(), nst.dataNodeLen())
		if nst.dataNodeLen() >0 && nst.metaNodeLen() > 0 {
			var nsl nsList
			nsl.lst = nodeList
			nsl.ele = ele
			nsl.zoneName = zoneName
			tmpList = append(tmpList,nsl)
			log.LogInfof("action[buildNodeSetGrpDoWork] nodeset [%v] zonename [%v] qualified be put in,metacnt[%v],datacnt[%v]",
				nst.ID, nst.zoneName,nst.metaNodeLen(), nst.dataNodeLen())
			needCnt = needCnt - 1
			if needCnt == 0 {
				break
			}
		}
		ele = ele.Next()
	}
	if needCnt == 0 {
		resList = append(resList, tmpList...)
	} else {
		err = fmt.Errorf("not quliaifed")
	}
	return
}

func (nsgm *nodeSetGrpManager) buildNodeSetGrpCommit(resList []nsList) {
	nodeSetGrp := newNodeSetGrp(nsgm.c)
	for i := 0; i < len(resList); i++ {
		nst := resList[i].ele.Value.(*nodeSet)
		nodeSetGrp.nodeSets = append(nodeSetGrp.nodeSets, nst)
		nodeSetGrp.nodeSetsIds = append(nodeSetGrp.nodeSetsIds, nst.ID)
		log.LogInfof("action[buildNodeSetGrpCommit] build nodesetGrp id[%v] with append nst id [%v] zoneName [%v]", nodeSetGrp.ID, nst.ID, nst.zoneName)
		resList[i].lst.Remove(resList[i].ele)
		nsgm.nsId2NsGrpMap[nst.ID] = len(nsgm.nodeSetGrpMap)
		if resList[i].lst.Len() == 0 {
			delete(nsgm.zoneAvailableNodeSet, resList[i].zoneName)
			log.LogInfof("action[buildNodeSetGrpCommit] after grp build no nodeset avaliable for zone[%v],nodesetid:[%v], zonelist size[%v]",
				nst.zoneName, nst.ID, len(nsgm.zoneAvailableNodeSet))
		}
	}

	log.LogInfof("action[buildNodeSetGrpCommit] success build nodesetgrp zonelist size[%v], nodesetids[%v]",
		len(nsgm.zoneAvailableNodeSet), nodeSetGrp.nodeSetsIds)
	nsgm.nodeSetGrpMap = append(nsgm.nodeSetGrpMap, nodeSetGrp)
	nsgm.c.putNodeSetGrpInfo(opSyncNodeSetGrp, nodeSetGrp)
	nsgm.status = normal
}

//policy of build zone if zone count large then three
func buildNodeSetGrp3Zone(nsgm *nodeSetGrpManager) (err error){
	nsgm.Lock()
	defer nsgm.Unlock()
	log.LogInfof("action[buildNodeSetGrp3Zone step in")
	if len(nsgm.zoneAvailableNodeSet) < defaultFaultDomainZoneCnt {
		log.LogInfof("action[nodeSetGrpManager::buildNodeSetGrp3Zone] size error,can't create group zone cnt[%v]",
						len(nsgm.zoneAvailableNodeSet))
		return fmt.Errorf("defaultFaultDomainZoneCnt not satisfied")
	}

	var resList []nsList
	buildIndex, zoneAvaVec := nsgm.buildNodeSetGrpPrepare()
	cnt := 0
	for {
		if cnt > 0 {
			buildIndex = (buildIndex + 1) % len(zoneAvaVec)
		}
		if cnt == len(zoneAvaVec) || len(resList) == defaultReplicaNum {
			log.LogInfof("step out inner loop in buildNodeSetGrp3Zone cnt [%v], inner index [%v]", cnt, buildIndex)
			break
		}
		cnt++
		nodeList := zoneAvaVec[buildIndex].lst
		zoneName := zoneAvaVec[buildIndex].zoneName
		var tmpList []nsList
		if tmpList, err = nsgm.buildNodeSetGrpDoWork(zoneName, nodeList, 1); err != nil {
			continue
		}
		resList = append(resList, tmpList...)
	}
	if len(resList) < defaultReplicaNum {
		log.LogInfof("action[nodeSetGrpManager::buildNodeSetGrp3Zone] can't create nodeset group nodeset qualified count [%v]", len(resList))
		return fmt.Errorf("defaultFaultDomainZoneCnt not satisfied")
	}
	nsgm.buildNodeSetGrpCommit(resList)
	return nil
}
func buildNodeSetGrpOneZone(nsgm *nodeSetGrpManager) (err error){
	nsgm.Lock()
	defer nsgm.Unlock()
	log.LogInfof("action[buildNodeSetGrpOneZone] step in")
	if len(nsgm.zoneAvailableNodeSet) != 1 {
		err = fmt.Errorf("avaliable zone cnt[%v]", len(nsgm.zoneAvailableNodeSet))
		return
	}
	buildIndex, zoneAvaVec := nsgm.buildNodeSetGrpPrepare()

	if zoneAvaVec[buildIndex].lst.Len() < defaultReplicaNum {
		return fmt.Errorf("not enough nodeset in avaliable list")
	}
	var resList []nsList
	if resList, err = nsgm.buildNodeSetGrpDoWork(zoneAvaVec[buildIndex].zoneName,
			zoneAvaVec[buildIndex].lst, defaultReplicaNum); err != nil {
		return err
	}
	nsgm.buildNodeSetGrpCommit(resList)

	return nil
}
//build 2 puls 1 nodesetGrp with 2zone or larger
func buildNodeSetGrp2Plus1(nsgm *nodeSetGrpManager) (err error){
	nsgm.Lock()
	defer nsgm.Unlock()
	log.LogInfof("step in buildNodeSetGrp2Plus1")

	cnt := 0
	var resList []nsList
	_, zoneAvaVec := nsgm.buildNodeSetGrpPrepare()
	var np1, np2 int

	if zoneAvaVec[0].lst.Len() <  zoneAvaVec[1].lst.Len() {
		np1 = 0
		np2 = 1
	} else {
		np1 = 1
		np2 = 0
	}
	for i:=2; i < len(zoneAvaVec); i++{
		if zoneAvaVec[i].lst.Len() > zoneAvaVec[np1].lst.Len() {
			if zoneAvaVec[i].lst.Len() > zoneAvaVec[np2].lst.Len() {
				np2 = i
			} else {
				np1 = i
			}
		}
	}
	if zoneAvaVec[np1].lst.Len() < 1 ||  zoneAvaVec[np2].lst.Len() < 2 {
		log.LogInfof("step out buildNodeSetGrp2Plus1 np1 [%v] np2 [%v] cnt [%v], inner index [%v]",
					np1, np2, cnt, nsgm.lastBuildIndex)
		return fmt.Errorf("action[buildNodeSetGrp2Plus1] failed")
	}

	var tmpList []nsList
	if tmpList, err = nsgm.buildNodeSetGrpDoWork(zoneAvaVec[np1].zoneName, zoneAvaVec[np1].lst, 1); err != nil {
		return
	}
	resList = append(resList, tmpList...)
	if tmpList, err = nsgm.buildNodeSetGrpDoWork(zoneAvaVec[np2].zoneName, zoneAvaVec[np2].lst, 2); err != nil {
		return
	}
	resList = append(resList, tmpList...)
	nsgm.buildNodeSetGrpCommit(resList)
	return
}

func (nsgm *nodeSetGrpManager) putNodeSet(ns *nodeSet, load bool) (err error){
	nsgm.Lock()
	defer nsgm.Unlock()
	log.LogInfof("action[nodeSetGrpManager::putNodeSet]  zone[%v],nodesetid:[%v], zonelist size[%v], load[%v]",
		ns.zoneName, ns.ID, len(nsgm.zoneAvailableNodeSet), load)

	if _, ok := nsgm.excludeZoneListDomain[ns.zoneName]; ok {
		log.LogInfof("action[nodeSetGrpManager::putNodeSet] zone[%v],nodesetid:[%v], zonelist size[%v]",
			ns.zoneName, ns.ID, len(nsgm.zoneAvailableNodeSet))
		return
	}
	if _, ok := nsgm.nsIdMap[ns.ID]; ok {
		log.LogInfof("action[nodeSetGrpManager::putNodeSet]  zone[%v],nodesetid:[%v] already be put before load[%v]",
			ns.zoneName, ns.ID, load)
		return
	}
	nsgm.nsIdMap[ns.ID] = 0
	// nodeset alreay be put into grp,this should be happened at condition of load == true
	// here hosts in ns should be nullptr and wait node register
	if grpidx, ok := nsgm.nsId2NsGrpMap[ns.ID]; ok {
		nsgm.nodeSetGrpMap[grpidx].nodeSets = append(nsgm.nodeSetGrpMap[grpidx].nodeSets, ns)
		log.LogInfof("action[nodeSetGrpManager::putNodeSet]  zone[%v],nodesetid:[%v] already be put before grp index[%v], grp id[%v] load[%v]",
			ns.zoneName, ns.ID, grpidx, nsgm.nodeSetGrpMap[grpidx].ID, load)
		return
	}
	if _, ok := nsgm.zoneAvailableNodeSet[ns.zoneName]; !ok {
		nsgm.zoneAvailableNodeSet[ns.zoneName] = list.New()
		log.LogInfof("action[nodeSetGrpManager::putNodeSet] init list for zone[%v],zonelist size[%v]",ns.zoneName, len(nsgm.zoneAvailableNodeSet))
	}
	log.LogInfof("action[nodeSetGrpManager::putNodeSet] ns id[%v] be put in zone[%v]", ns.ID, ns.zoneName)
	nsgm.zoneAvailableNodeSet[ns.zoneName].PushBack(ns)
	return
}

type nodeSet struct {
	ID        uint64
	Capacity  int
	zoneName  string
	metaNodes *sync.Map
	dataNodes *sync.Map
	sync.RWMutex
}

func newNodeSet(id uint64, cap int, zoneName string) *nodeSet {
	log.LogInfof("action[newNodeSet] id[%v]",id)
	ns := &nodeSet{
		ID:        id,
		Capacity:  cap,
		zoneName:  zoneName,
		metaNodes: new(sync.Map),
		dataNodes: new(sync.Map),
	}
	return ns
}

func (ns *nodeSet) metaNodeLen() (count int) {
	ns.RLock()
	defer ns.RUnlock()
	ns.metaNodes.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return
}

func (ns *nodeSet) dataNodeLen() (count int) {
	ns.RLock()
	defer ns.RUnlock()
	ns.dataNodes.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return
}

func (ns *nodeSet) putMetaNode(metaNode *MetaNode) {
	ns.metaNodes.Store(metaNode.Addr, metaNode)
}

func (ns *nodeSet) deleteMetaNode(metaNode *MetaNode) {
	ns.metaNodes.Delete(metaNode.Addr)
}

func (ns *nodeSet) canWriteForDataNode(replicaNum int) bool {
	var count int
	ns.dataNodes.Range(func(key, value interface{}) bool {
		node := value.(*DataNode)
		if node.isWriteAble() {
			count++
		}
		if count >= replicaNum {
			return false
		}
		return true
	})
	log.LogInfof("canWriteForDataNode zone[%v], ns[%v],count[%v], replicaNum[%v]",
		ns.zoneName, ns.ID, count, replicaNum)
	return count >= replicaNum
}

func (ns *nodeSet) canWriteForMetaNode(replicaNum int) bool {
	var count int
	ns.metaNodes.Range(func(key, value interface{}) bool {
		node := value.(*MetaNode)
		if node.isWritable() {
			count++
		}
		if count >= replicaNum {
			return false
		}
		return true
	})
	log.LogInfof("canWriteForMetaNode zone[%v], ns[%v],count[%v] replicaNum[%v]",
		ns.zoneName, ns.ID, count, replicaNum)
	return count >= replicaNum
}

func (ns *nodeSet) putDataNode(dataNode *DataNode) {
	ns.dataNodes.Store(dataNode.Addr, dataNode)
}

func (ns *nodeSet) deleteDataNode(dataNode *DataNode) {
	ns.dataNodes.Delete(dataNode.Addr)
}

func (t *topology) isSingleZone() bool {
	t.zoneLock.RLock()
	defer t.zoneLock.RUnlock()
	var zoneLen int
	t.zoneMap.Range(func(zoneName, value interface{}) bool {
		zoneLen++
		return true
	})
	return zoneLen == 1
}

func (t *topology) getDomainExcludeZones() (zones []*Zone) {
	t.zoneLock.RLock()
	defer t.zoneLock.RUnlock()
	zones = make([]*Zone, 0)
	for i:= 0; i < len(t.domainExcludeZones); i++ {
		if value, ok := t.zoneMap.Load(t.domainExcludeZones[i]); ok {
			zones = append(zones, value.(*Zone))
			log.LogInfof("action[getDomainExcludeZones] append zone name:[%v]_[%v]", t.domainExcludeZones[i], value.(*Zone).name)
		}
	}
	return
}

func (t *topology) getAllZones() (zones []*Zone) {
	t.zoneLock.RLock()
	defer t.zoneLock.RUnlock()
	zones = make([]*Zone, 0)
	t.zoneMap.Range(func(zoneName, value interface{}) bool {
		zone := value.(*Zone)
		zones = append(zones, zone)
		return true
	})
	return
}

func (t *topology) getZoneByIndex(index int) (zone *Zone) {
	t.zoneLock.RLock()
	defer t.zoneLock.RUnlock()
	return t.zones[index]
}

func calculateDemandWriteNodes(zoneNum int, replicaNum int) (demandWriteNodes int) {
	if zoneNum == 1 {
		demandWriteNodes = replicaNum
	} else {
		if replicaNum == 1 {
			demandWriteNodes = 1
		} else {
			demandWriteNodes = 2
		}
	}
	return
}

func (t *topology) allocZonesForMetaNode(zoneNum, replicaNum int, excludeZone []string) (zones []*Zone, err error) {
	if len(t.domainExcludeZones) > 0 {
		zones = t.getDomainExcludeZones()
		log.LogInfof("action[allocZonesForMetaNode] getDomainExcludeZones zones [%v]", t.domainExcludeZones)
	} else {
		// if domain enable, will not enter here
		zones = t.getAllZones()
	}
	if t.isSingleZone() {
		return zones, nil
	}
	if excludeZone == nil {
		excludeZone = make([]string, 0)
	}
	candidateZones := make([]*Zone, 0)
	demandWriteNodes := calculateDemandWriteNodes(zoneNum, replicaNum)
	for i := 0; i < len(zones); i++ {
		if t.zoneIndexForMetaNode >= len(zones) {
			t.zoneIndexForMetaNode = 0
		}
		zone := zones[t.zoneIndexForMetaNode]
		t.zoneIndexForMetaNode++
		if zone.status == unavailableZone {
			continue
		}
		if contains(excludeZone, zone.name) {
			continue
		}
		if zone.canWriteForMetaNode(uint8(demandWriteNodes)) {
			candidateZones = append(candidateZones, zone)
		}
		if len(candidateZones) >= zoneNum {
			break
		}
	}

	//if across zone,candidateZones must be larger than or equal with 2,otherwise,must have a candidate zone
	if (zoneNum >= 2 && len(candidateZones) < 2) || len(candidateZones) < 1 {
		log.LogError(fmt.Sprintf("action[allocZonesForMetaNode],reqZoneNum[%v],candidateZones[%v],demandWriteNodes[%v],err:%v",
			zoneNum, len(candidateZones), demandWriteNodes, proto.ErrNoZoneToCreateMetaPartition))
		return nil, proto.ErrNoZoneToCreateMetaPartition
	}
	zones = candidateZones
	err = nil
	return
}

func (t *topology) allocZonesForDataNode(zoneNum, replicaNum int, excludeZone []string) (zones []*Zone, err error) {
	// domain enabled and have old zones to be used
	if len(t.domainExcludeZones) > 0 {
		zones = t.getDomainExcludeZones()
	} else {
		// if domain enable, will not enter here
		zones = t.getAllZones()
	}
	log.LogInfof("len(zones) = %v \n", len(zones))
	if t.isSingleZone() {
		return zones, nil
	}
	if excludeZone == nil {
		excludeZone = make([]string, 0)
	}
	demandWriteNodes := calculateDemandWriteNodes(zoneNum, replicaNum)
	candidateZones := make([]*Zone, 0)
	for i := 0; i < len(zones); i++ {
		if t.zoneIndexForDataNode >= len(zones) {
			t.zoneIndexForDataNode = 0
		}
		zone := zones[t.zoneIndexForDataNode]
		t.zoneIndexForDataNode++
		if zone.status == unavailableZone {
			continue
		}
		if contains(excludeZone, zone.name) {
			continue
		}
		if zone.canWriteForDataNode(uint8(demandWriteNodes)) {
			candidateZones = append(candidateZones, zone)
		}
		if len(candidateZones) >= zoneNum {
			break
		}
	}
	//if across zone,candidateZones must be larger than or equal with 2,otherwise,must have one candidate zone
	if (zoneNum >= 2 && len(candidateZones) < 2) || len(candidateZones) < 1 {
		log.LogError(fmt.Sprintf("action[allocZonesForDataNode],reqZoneNum[%v],candidateZones[%v],demandWriteNodes[%v],err:%v",
			zoneNum, len(candidateZones), demandWriteNodes, proto.ErrNoZoneToCreateDataPartition))
		return nil, errors.NewError(proto.ErrNoZoneToCreateDataPartition)
	}
	zones = candidateZones
	err = nil
	return
}

func (ns *nodeSet) dataNodeCount() int {
	var count int
	ns.dataNodes.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

func (ns *nodeSet) getAvailDataNodeHosts(excludeHosts []string, replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	return getAvailHosts(ns.dataNodes, excludeHosts, replicaNum, selectDataNode)
}

// Zone stores all the zone related information
type Zone struct {
	name                string
	setIndexForDataNode int
	setIndexForMetaNode int
	status              int
	dataNodes           *sync.Map
	metaNodes           *sync.Map
	nodeSetMap          map[uint64]*nodeSet
	nsLock              sync.RWMutex
	sync.RWMutex
}

func newZone(name string) (zone *Zone) {
	zone = &Zone{name: name}
	zone.status = normalZone
	zone.dataNodes = new(sync.Map)
	zone.metaNodes = new(sync.Map)
	zone.nodeSetMap = make(map[uint64]*nodeSet)
	return
}

func (zone *Zone) setStatus(status int) {
	zone.status = status
}

func (zone *Zone) getStatus() int {
	return zone.status
}

func (zone *Zone) getStatusToString() string {
	if zone.status == normalZone {
		return "available"
	} else {
		return "unavailable"
	}
}

func (zone *Zone) isSingleNodeSet() bool {
	zone.RLock()
	defer zone.RUnlock()
	return len(zone.nodeSetMap) == 1
}

func (zone *Zone) getNodeSet(setID uint64) (ns *nodeSet, err error) {
	zone.nsLock.RLock()
	defer zone.nsLock.RUnlock()
	ns, ok := zone.nodeSetMap[setID]
	if !ok {
		return nil, errors.NewErrorf("set %v not found", setID)
	}
	return
}

func (zone *Zone) putNodeSet(ns *nodeSet) (err error) {
	zone.nsLock.Lock()
	defer zone.nsLock.Unlock()
	if _, ok := zone.nodeSetMap[ns.ID]; ok {
		return fmt.Errorf("nodeSet [%v] has exist", ns.ID)
	}
	zone.nodeSetMap[ns.ID] = ns
	return
}

func (zone *Zone) createNodeSet(c *Cluster) (ns *nodeSet, err error) {
	cnt := 1
	allNodeSet := zone.getAllNodeSet()
	if c.FaultDomain && c.nodeSetGrpManager.init && c.cfg.DomainNodeGrpBatchCnt < defaultReplicaNum {
		if _, ok := c.nodeSetGrpManager.excludeZoneListDomain[zone.name]; !ok {
			if len(allNodeSet) < c.cfg.DomainNodeGrpBatchCnt {
				log.LogInfof("action[createNodeSet] zone[%v] nodeset len:[%v] less then 3,create to 3 one time",
					zone.name, len(allNodeSet))
				cnt = c.cfg.DomainNodeGrpBatchCnt - len(allNodeSet)
			}
		}
	}
	for {
		if cnt == 0 {
			break
		}
		cnt--
		id, err := c.idAlloc.allocateCommonID()
		if err != nil {
			return nil, err
		}
		ns = newNodeSet(id, c.cfg.nodeSetCapacity, zone.name)
		log.LogInfof("action[createNodeSet] syncAddNodeSet[%v] zonename[%v]", ns.ID, zone.name)
		if err = c.syncAddNodeSet(ns); err != nil {
			return nil, err
		}
		if err = zone.putNodeSet(ns); err != nil {
			return nil, err
		}
		log.LogInfof("action[createNodeSet] nodeSet[%v]", ns.ID)
	}
	return
}

func (zone *Zone) getAllNodeSet() (nsc nodeSetCollection) {
	zone.nsLock.RLock()
	defer zone.nsLock.RUnlock()
	nsc = make(nodeSetCollection, 0)
	for _, ns := range zone.nodeSetMap {
		nsc = append(nsc, ns)
	}
	return
}

func (zone *Zone) getAvailNodeSetForMetaNode() (nset *nodeSet) {
	allNodeSet := zone.getAllNodeSet()
	sort.Sort(sort.Reverse(allNodeSet))
	for _, ns := range allNodeSet {
		if ns.metaNodeLen() < ns.Capacity {
			if nset == nil {
				nset = ns
			} else {
				if nset.Capacity - nset.metaNodeLen() < ns.Capacity - ns.metaNodeLen() {
					nset = ns
				}
			}
			continue
		}

	}
	return
}

func (zone *Zone) getAvailNodeSetForDataNode() (nset *nodeSet) {
	allNodeSet := zone.getAllNodeSet()
	for _, ns := range allNodeSet {
		if ns.dataNodeLen() < ns.Capacity {
			if nset == nil {
				nset = ns
			} else {
				if nset.Capacity - nset.dataNodeLen() < ns.Capacity - ns.dataNodeLen() {
					nset = ns
				}
			}
			continue
		}
	}
	return
}

func (zone *Zone) putDataNode(dataNode *DataNode) (err error) {
	var ns *nodeSet
	if ns, err = zone.getNodeSet(dataNode.NodeSetID); err != nil {
		log.LogErrorf("action[putDataNode] nodeSet[%v] not found", dataNode.NodeSetID)
		return
	}
	ns.putDataNode(dataNode)
	zone.dataNodes.Store(dataNode.Addr, dataNode)
	return
}

func (zone *Zone) getDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := zone.dataNodes.Load(addr)
	if !ok {
		return nil, errors.Trace(dataNodeNotFound(addr), "%v not found", addr)
	}
	dataNode = value.(*DataNode)
	return
}
func (zone *Zone) deleteDataNode(dataNode *DataNode) {
	ns, err := zone.getNodeSet(dataNode.NodeSetID)
	if err != nil {
		log.LogErrorf("action[zoneDeleteDataNode] nodeSet[%v] not found", dataNode.NodeSetID)
		return
	}
	ns.deleteDataNode(dataNode)
	zone.dataNodes.Delete(dataNode.Addr)
}

func (zone *Zone) putMetaNode(metaNode *MetaNode) (err error) {
	var ns *nodeSet
	if ns, err = zone.getNodeSet(metaNode.NodeSetID); err != nil {
		log.LogErrorf("action[zonePutMetaNode] nodeSet[%v] not found", metaNode.NodeSetID)
		return
	}
	ns.putMetaNode(metaNode)
	zone.metaNodes.Store(metaNode.Addr, metaNode)
	return
}

func (zone *Zone) deleteMetaNode(metaNode *MetaNode) (err error) {
	ns, err := zone.getNodeSet(metaNode.NodeSetID)
	if err != nil {
		log.LogErrorf("action[zoneDeleteMetaNode] nodeSet[%v] not found", metaNode.NodeSetID)
		return
	}
	ns.deleteMetaNode(metaNode)
	zone.metaNodes.Delete(metaNode.Addr)
	return
}

func (zone *Zone) allocNodeSetForDataNode(excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error) {
	nset := zone.getAllNodeSet()
	if nset == nil {
		return nil, errors.NewError(proto.ErrNoNodeSetToCreateDataPartition)
	}
	zone.nsLock.Lock()
	defer zone.nsLock.Unlock()
	for i := 0; i < len(nset); i++ {
		if zone.setIndexForDataNode >= len(nset) {
			zone.setIndexForDataNode = 0
		}
		ns = nset[zone.setIndexForDataNode]
		zone.setIndexForDataNode++
		if containsID(excludeNodeSets, ns.ID) {
			continue
		}
		if ns.canWriteForDataNode(int(replicaNum)) {
			return
		}
	}
	log.LogErrorf("action[allocNodeSetForDataNode],nset len[%v],excludeNodeSets[%v],rNum[%v] err:%v",
		nset.Len(), excludeNodeSets, replicaNum, proto.ErrNoNodeSetToCreateDataPartition)
	return nil, errors.NewError(proto.ErrNoNodeSetToCreateDataPartition)
}

func (zone *Zone) allocNodeSetForMetaNode(excludeNodeSets []uint64, replicaNum uint8) (ns *nodeSet, err error) {
	nset := zone.getAllNodeSet()
	if nset == nil {
		return nil, proto.ErrNoNodeSetToCreateMetaPartition
	}
	zone.nsLock.Lock()
	defer zone.nsLock.Unlock()
	for i := 0; i < len(nset); i++ {
		if zone.setIndexForMetaNode >= len(nset) {
			zone.setIndexForMetaNode = 0
		}
		ns = nset[zone.setIndexForMetaNode]
		zone.setIndexForMetaNode++
		if containsID(excludeNodeSets, ns.ID) {
			continue
		}
		if ns.canWriteForMetaNode(int(replicaNum)) {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[allocNodeSetForMetaNode],zone[%v],excludeNodeSets[%v],rNum[%v],err:%v",
		zone.name, excludeNodeSets, replicaNum, proto.ErrNoNodeSetToCreateMetaPartition))
	return nil, proto.ErrNoNodeSetToCreateMetaPartition
}

func (zone *Zone) canWriteForDataNode(replicaNum uint8) (can bool) {
	zone.RLock()
	defer zone.RUnlock()
	var leastAlive uint8
	zone.dataNodes.Range(func(addr, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.isActive == true && dataNode.isWriteAbleWithSize(30*util.GB) == true {
			leastAlive++
		}
		if leastAlive >= replicaNum {
			can = true
			return false
		}
		return true
	})
	log.LogInfof("canWriteForDataNode leastAlive[%v],replicaNum[%v],count[%v]\n", leastAlive, replicaNum, zone.dataNodeCount())
	return
}
func (zone *Zone) isUsedRatio(ratio float64) (can bool) {
	zone.RLock()
	defer zone.RUnlock()
	var (
		dataNodeUsed  uint64
		dataNodeTotal uint64
		metaNodeUsed  uint64
		metaNodeTotal uint64
	)
	zone.dataNodes.Range(func(addr, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.isActive == true{
			dataNodeUsed  += dataNode.Used
		} else {
			dataNodeUsed  += dataNode.Total
		}
		dataNodeTotal += dataNode.Total
		return true
	})

	if float64(dataNodeUsed)/float64(dataNodeTotal) > ratio {
		log.LogInfof("action[isUsedRatio] zone[%v] dataNodeUsed [%v] total [%v], ratio[%v]", zone.name, dataNodeUsed, dataNodeTotal, ratio)
		return true
	}

	zone.metaNodes.Range(func(addr, value interface{}) bool {
		metaNode := value.(*MetaNode)
		if metaNode.IsActive == true && metaNode.isWritable() == true {
			metaNodeUsed += metaNode.Used
		} else {
			metaNodeUsed += metaNode.Total
		}
		metaNodeTotal += metaNode.Total
		return true
	})

	if float64(metaNodeUsed)/float64(metaNodeTotal) > ratio {
		log.LogInfof("action[isUsedRatio] zone[%v] metaNodeUsed [%v] total [%v], ratio[%v]", zone.name, metaNodeUsed, metaNodeTotal, ratio)
		return true
	}

	return false
}

func (zone *Zone) canWriteForMetaNode(replicaNum uint8) (can bool) {
	zone.RLock()
	defer zone.RUnlock()
	var leastAlive uint8
	zone.metaNodes.Range(func(addr, value interface{}) bool {
		metaNode := value.(*MetaNode)
		if metaNode.IsActive == true && metaNode.isWritable() == true {
			leastAlive++
		}
		if leastAlive >= replicaNum {
			can = true
			return false
		}
		return true
	})
	return
}

func (zone *Zone) getDataNodeMaxTotal() (maxTotal uint64) {
	zone.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.Total > maxTotal {
			maxTotal = dataNode.Total
		}
		return true
	})
	return
}

func (zone *Zone) getAvailDataNodeHosts(excludeNodeSets []uint64, excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	if replicaNum == 0 {
		return
	}
	ns, err := zone.allocNodeSetForDataNode(excludeNodeSets, uint8(replicaNum))
	if err != nil {
		return nil, nil, errors.Trace(err, "zone[%v] alloc node set,replicaNum[%v]", zone.name, replicaNum)
	}
	return ns.getAvailDataNodeHosts(excludeHosts, replicaNum)
}

func (zone *Zone) getAvailMetaNodeHosts(excludeNodeSets []uint64, excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	if replicaNum == 0 {
		return
	}
	ns, err := zone.allocNodeSetForMetaNode(excludeNodeSets, uint8(replicaNum))
	if err != nil {
		return nil, nil, errors.NewErrorf("zone[%v],err[%v]", zone.name, err)
	}
	return ns.getAvailMetaNodeHosts(excludeHosts, replicaNum)

}

func (zone *Zone) dataNodeCount() (len int) {

	zone.dataNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}
