package master

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type ClusterService struct {
	cluster    *Cluster
	user       *User
	conf       *clusterConfig
	leaderInfo *LeaderInfo
}

func (s *ClusterService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()

	s.registerObject(schema)
	s.registerQuery(schema)
	s.registerMutation(schema)

	return schema.MustBuild()
}

func (s *ClusterService) registerObject(schema *schemabuilder.Schema) {
	object := schema.Object("ClusterView", proto.ClusterView{})

	object.FieldFunc("serverCount", func(ctx context.Context, args struct{}) (int32, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return 0, err
		}
		return int32(s.cluster.dataNodeCount() + s.cluster.metaNodeCount()), nil
	})

	object.FieldFunc("dataPartitionCount", func(ctx context.Context, args struct{}) (int32, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return 0, err
		}
		return int32(s.cluster.getDataPartitionCount()), nil
	})

	object.FieldFunc("metaPartitionCount", func(ctx context.Context, args struct{}) (int32, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return 0, err
		}
		return int32(s.cluster.getMetaPartitionCount()), nil
	})

	object.FieldFunc("volumeCount", func(ctx context.Context, args struct{}) (int32, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return 0, err
		}
		return int32(len(s.cluster.vols)), nil
	})

	object.FieldFunc("masterCount", func(ctx context.Context, args struct{}) (int32, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return 0, err
		}
		return int32(len(s.conf.peerAddrs)), nil
	})

	object.FieldFunc("metaNodeCount", func(ctx context.Context, args struct{}) (int32, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return 0, err
		}
		return int32(s.cluster.metaNodeCount()), nil
	})

	object.FieldFunc("dataNodeCount", func(ctx context.Context, args struct{}) (int32, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return 0, err
		}
		return int32(s.cluster.dataNodeCount()), nil
	})

	nv := schema.Object("NodeView", proto.NodeView{})

	nv.FieldFunc("toMetaNode", func(ctx context.Context, n *proto.NodeView) (*MetaNode, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return nil, err
		}
		return s.cluster.metaNode(n.Addr)
	})

	nv.FieldFunc("toDataNode", func(ctx context.Context, n *proto.NodeView) (*DataNode, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return nil, err
		}
		return s.cluster.dataNode(n.Addr)
	})

	nv.FieldFunc("reportDisks", func(ctx context.Context, n *proto.NodeView) ([]string, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return nil, err
		}
		node, err := s.cluster.dataNode(n.Addr)
		if err != nil {
			return nil, err
		}

		diskmap := make(map[string]bool)
		for _, p := range node.DataPartitionReports {
			diskmap[p.DiskPath] = true
		}

		keys := make([]string, 0, len(diskmap))

		for key := range diskmap {
			keys = append(keys, key)
		}

		sort.Slice(keys, func(i, j int) bool {
			return strings.Compare(keys[i], keys[j]) > 0
		})

		return keys, nil
	})

	vs := schema.Object("VolStatInfo", proto.VolStatInfo{})
	vs.FieldFunc("toVolume", func(ctx context.Context, n *proto.VolStatInfo) (*Vol, error) {
		if _, _, err := permissions(ctx, ADMIN); err != nil {
			return nil, err
		}
		return s.cluster.getVol(n.Name)
	})

	object = schema.Object("DataNode", DataNode{})
	object.FieldFunc("isActive", func(ctx context.Context, n *DataNode) bool {
		return n.isActive
	})

	object = schema.Object("metaNode", MetaNode{})
	object.FieldFunc("metaPartitionInfos", func(ctx context.Context, n *MetaNode) []*proto.MetaPartitionReport {
		return n.metaPartitionInfos
	})

}

func (s *ClusterService) registerQuery(schema *schemabuilder.Schema) {
	query := schema.Query()
	query.FieldFunc("clusterView", s.clusterView)
	query.FieldFunc("dataNodeList", s.dataNodeList)
	query.FieldFunc("dataNodeListTest", s.dataNodeListTest)
	query.FieldFunc("dataNodeGet", s.dataNodeGet)
	query.FieldFunc("metaNodeList", s.metaNodeList)
	query.FieldFunc("metaNodeGet", s.metaNodeGet)
	query.FieldFunc("masterList", s.masterList)
	query.FieldFunc("getTopology", s.getTopology)
	query.FieldFunc("alarmList", s.alarmList)
}

func (s *ClusterService) registerMutation(schema *schemabuilder.Schema) {
	mutation := schema.Mutation()

	mutation.FieldFunc("clusterFreeze", s.clusterFreeze)
	mutation.FieldFunc("addRaftNode", s.addRaftNode)
	mutation.FieldFunc("removeRaftNode", s.removeRaftNode)
	mutation.FieldFunc("addMetaNode", s.removeRaftNode)
	mutation.FieldFunc("loadMetaPartition", s.loadMetaPartition)
	mutation.FieldFunc("decommissionMetaPartition", s.decommissionMetaPartition)
	mutation.FieldFunc("decommissionMetaNode", s.decommissionMetaNode)
	mutation.FieldFunc("decommissionDisk", s.decommissionDisk)
	mutation.FieldFunc("decommissionDataNode", s.decommissionDataNode)
}

// Decommission a disk. This will decommission all the data partitions on this disk.
func (m *ClusterService) decommissionDisk(ctx context.Context, args struct {
	OffLineAddr string
	DiskPath    string
}) (*proto.GeneralResp, error) {

	node, err := m.cluster.dataNode(args.OffLineAddr)
	if err != nil {
		return nil, err
	}

	badPartitions := node.badPartitions(args.DiskPath, m.cluster)
	if len(badPartitions) == 0 {
		err = fmt.Errorf("node[%v] disk[%v] does not have any data partition", node.Addr, args.DiskPath)
		return nil, err
	}

	var badPartitionIds []uint64
	for _, bdp := range badPartitions {
		badPartitionIds = append(badPartitionIds, bdp.PartitionID)
	}
	rstMsg := fmt.Sprintf("receive decommissionDisk node[%v] disk[%v], badPartitionIds[%v] has offline successfully",
		node.Addr, args.DiskPath, badPartitionIds)
	if err = m.cluster.decommissionDisk(node, args.DiskPath, badPartitions); err != nil {
		return nil, err
	}
	Warn(m.cluster.Name, rstMsg)

	return proto.Success("success"), nil

}

// Decommission a data node. This will decommission all the data partition on that node.
func (m *ClusterService) decommissionDataNode(ctx context.Context, args struct {
	OffLineAddr string
}) (*proto.GeneralResp, error) {

	node, err := m.cluster.dataNode(args.OffLineAddr)
	if err != nil {
		return nil, err
	}
	if err := m.cluster.decommissionDataNode(node); err != nil {
		return nil, err
	}
	rstMsg := fmt.Sprintf("decommission data node [%v] successfully", args.OffLineAddr)

	return proto.Success(rstMsg), nil
}

func (m *ClusterService) decommissionMetaNode(ctx context.Context, args struct {
	OffLineAddr string
}) (*proto.GeneralResp, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	metaNode, err := m.cluster.metaNode(args.OffLineAddr)
	if err != nil {
		return nil, err
	}
	if err = m.cluster.decommissionMetaNode(metaNode); err != nil {
		return nil, err
	}
	log.LogInfof("decommissionMetaNode metaNode [%v] has offline successfully", args.OffLineAddr)
	return proto.Success("success"), nil
}

func (m *ClusterService) loadMetaPartition(ctx context.Context, args struct {
	PartitionID uint64
}) (*proto.GeneralResp, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	mp, err := m.cluster.getMetaPartitionByID(args.PartitionID)
	if err != nil {
		return nil, err
	}

	m.cluster.loadMetaPartitionAndCheckResponse(mp)
	log.LogInfof(proto.AdminLoadMetaPartition+" partitionID :%v Load successfully", args.PartitionID)
	return proto.Success("success"), nil
}

func (m *ClusterService) decommissionMetaPartition(ctx context.Context, args struct {
	PartitionID uint64
	NodeAddr    string
}) (*proto.GeneralResp, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	mp, err := m.cluster.getMetaPartitionByID(args.PartitionID)
	if err != nil {
		return nil, err
	}
	if err := m.cluster.decommissionMetaPartition(args.NodeAddr, mp); err != nil {
		return nil, err
	}
	log.LogInfof(proto.AdminDecommissionMetaPartition+" partitionID :%v  decommissionMetaPartition successfully", args.PartitionID)
	return proto.Success("success"), nil
}

func (m *ClusterService) getMetaNode(ctx context.Context, args struct {
	NodeAddr string
}) (*MetaNode, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	metaNode, err := m.cluster.metaNode(args.NodeAddr)
	if err != nil {
		return nil, err
	}
	return metaNode, nil
}

// View the topology of the cluster.
func (m *ClusterService) getTopology(ctx context.Context, args struct{}) (*proto.GeneralResp, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
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

	bs, e := json.Marshal(tv)
	if e != nil {
		return nil, e
	}
	return proto.Success(string(bs)), e
}

func (s *ClusterService) clusterView(ctx context.Context, args struct{}) (*proto.ClusterView, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	return s.makeClusterView(), nil
}

type MasterInfo struct {
	Index    string
	Addr     string
	IsLeader bool
}

func (s *ClusterService) masterList(ctx context.Context, args struct{}) ([]*MasterInfo, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}

	list := make([]*MasterInfo, 0)
	leader := strings.Split(s.leaderInfo.addr, ":")
	for _, addr := range s.conf.peerAddrs {
		split := strings.Split(addr, ":")
		list = append(list, &MasterInfo{
			Index:    split[0],
			Addr:     split[1],
			IsLeader: leader[0] == split[1],
		})
	}
	return list, nil
}

func (s *ClusterService) dataNodeGet(ctx context.Context, args struct {
	Addr string
}) (*DataNode, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	return s.cluster.dataNode(args.Addr)
}

func (s *ClusterService) dataNodeList(ctx context.Context, args struct{}) ([]*DataNode, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	var all []*DataNode
	s.cluster.dataNodes.Range(func(_, value interface{}) bool {
		all = append(all, value.(*DataNode))
		return true
	})
	return all, nil
}

func (s *ClusterService) dataNodeListTest(ctx context.Context, args struct {
	Num int64
}) ([]*DataNode, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	var all []*DataNode

	for i := 0; i < int(args.Num); i++ {
		all = append(all, &DataNode{
			Total:          uint64(i),
			Used:           1,
			AvailableSpace: 1,
			ID:             1,
			ZoneName:       "123",
			Addr:           "123123121231",
			ReportTime:     time.Time{},
			isActive:       false,
			RWMutex:        sync.RWMutex{},
			UsageRatio:     1,
			SelectedTimes:  2,
			Carry:          3,
		})
	}

	return all, nil
}

func (s *ClusterService) metaNodeGet(ctx context.Context, args struct {
	Addr string
}) (*MetaNode, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	mn, found := s.cluster.metaNodes.Load(args.Addr)
	if found {
		return mn.(*MetaNode), nil
	}
	return nil, fmt.Errorf("not found meta_node by add:[%s]", args.Addr)
}

func (s *ClusterService) metaNodeList(ctx context.Context, args struct{}) ([]*MetaNode, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	var all []*MetaNode
	s.cluster.metaNodes.Range(func(_, value interface{}) bool {
		all = append(all, value.(*MetaNode))
		return true
	})
	return all, nil
}

func (m *ClusterService) addMetaNode(ctx context.Context, args struct {
	NodeAddr string
	ZoneName string
}) (uint64, error) {
	if id, err := m.cluster.addMetaNode(args.NodeAddr, args.ZoneName, 0); err != nil {
		return 0, err
	} else {
		return id, nil
	}
}

// Dynamically remove a master node. Similar to addRaftNode, this operation is performed online.
func (m *ClusterService) removeRaftNode(ctx context.Context, args struct {
	Id   uint64
	Addr string
}) (*proto.GeneralResp, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}
	if err := m.cluster.removeRaftNode(args.Id, args.Addr); err != nil {
		return nil, err
	}
	log.LogInfof("remove  raft node id :%v,adr:%v successfully\n", args.Id, args.Addr)
	return proto.Success("success"), nil
}

// Dynamically add a raft node (replica) for the master.
// By using this function, there is no need to stop all the master services. Adding a new raft node is performed online.
func (m *ClusterService) addRaftNode(ctx context.Context, args struct {
	Id   uint64
	Addr string
}) (*proto.GeneralResp, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}

	if err := m.cluster.addRaftNode(args.Id, args.Addr); err != nil {
		return nil, err
	}

	log.LogInfof("add  raft node id :%v, addr:%v successfully \n", args.Id, args.Addr)
	return proto.Success("success"), nil
}

// Turn on or off the automatic allocation of the data partitions.
// If DisableAutoAllocate == off, then we WILL NOT automatically allocate new data partitions for the volume when:
// 	1. the used space is below the max capacity,
//	2. and the number of r&w data partition is less than 20.
//
// If DisableAutoAllocate == on, then we WILL automatically allocate new data partitions for the volume when:
// 	1. the used space is below the max capacity,
//	2. and the number of r&w data partition is less than 20.
func (m *ClusterService) clusterFreeze(ctx context.Context, args struct {
	Status bool
}) (*proto.GeneralResp, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}

	if err := m.cluster.setDisableAutoAllocate(args.Status); err != nil {
		return nil, err
	}
	return proto.Success("success"), nil
}

type WarnMessage struct {
	Time     string `json:"time"`
	Key      string `json:"key"`
	Hostname string `json:"hostname"`
	Type     string `json:"type"`
	Value    string `json:"value"`
	Detail   string `json:"detail"`
}

func (m *ClusterService) alarmList(ctx context.Context, args struct {
	Size int32
}) ([]*WarnMessage, error) {
	if _, _, err := permissions(ctx, ADMIN); err != nil {
		return nil, err
	}

	size := int64(args.Size * 1000)

	list := make([]*WarnMessage, 0, 100)

	path := filepath.Join(log.LogDir, "master"+log.CriticalLogFileName)

	stat, err := os.Stat(path)
	if err != nil {
		list = append(list, &WarnMessage{
			Time:     time.Now().Format("2006-01-02 15:04:05"),
			Key:      "not found",
			Hostname: m.leaderInfo.addr,
			Type:     "not found",
			Value:    "not found",
			Detail:   path + " read has err:" + err.Error(),
		})
		return list, nil
	}

	f, err := os.Open(path)

	if err != nil {
		return nil, fmt.Errorf("open file has err:[%s]", err.Error())
	}

	if stat.Size() > size {
		if _, err := f.Seek(stat.Size()-size, 0); err != nil {
			return nil, fmt.Errorf("seek file has err:[%s]", err.Error())
		}
	}

	defer func() {
		if err := f.Close(); err != nil {
			log.LogErrorf("close alarm file has err:[%s]", err.Error())
		}
	}()

	buf := bufio.NewReader(f)

	all, err := ioutil.ReadAll(buf)
	if err != nil {
		return nil, fmt.Errorf("read file:[%s] size:[%d] has err:[%s]", path, stat.Size(), err.Error())
	}

	for _, line := range strings.Split(string(all), "\n") {

		if len(line) == 0 {
			break
		}

		split := strings.Split(string(line), " ")

		var msg *WarnMessage

		if len(split) < 7 {
			value := string(line)
			msg = &WarnMessage{
				Time:     "unknow",
				Key:      "parse msg has err",
				Hostname: "parse msg has err",
				Type:     "parse msg has err",
				Value:    value,
				Detail:   value,
			}
		} else {
			value := strings.Join(split[6:], " ")
			msg = &WarnMessage{
				Time:     split[0] + " " + split[1],
				Key:      split[4],
				Hostname: split[5],
				Type:     split[2],
				Value:    value,
				Detail:   value,
			}
		}

		list = append(list, msg)
	}

	//reverse slice
	l := len(list)
	for i := 0; i < l/2; i++ {
		list[i], list[l-i-1] = list[l-i-1], list[i]
	}

	if len(list) > int(args.Size) {
		list = list[:args.Size]
	}

	return list, nil
}

func (m *ClusterService) makeClusterView() *proto.ClusterView {
	cv := &proto.ClusterView{
		Name:                m.cluster.Name,
		LeaderAddr:          m.cluster.leaderInfo.addr,
		DisableAutoAlloc:    m.cluster.DisableAutoAllocate,
		MetaNodeThreshold:   m.cluster.cfg.MetaNodeThreshold,
		Applied:             m.cluster.fsm.applied,
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
	m.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{Path: path, PartitionIDs: badDataPartitionIds}
		cv.BadPartitionIDs = append(cv.BadPartitionIDs, bpv)
		return true
	})
	m.cluster.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badPartitionIds := value.([]uint64)
		path := key.(string)
		bpv := badPartitionView{Path: path, PartitionIDs: badPartitionIds}
		cv.BadMetaPartitionIDs = append(cv.BadMetaPartitionIDs, bpv)
		return true
	})
	return cv
}
