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
	"net/http"
	"strconv"

	"github.com/chubaofs/chubaofs/proto"
)

type AdminAPI struct {
	mc *MasterClient
}

func (api *AdminAPI) GetCluster() (cv *proto.ClusterView, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.AdminGetCluster)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	cv = &proto.ClusterView{}
	if err = json.Unmarshal(buf, &cv); err != nil {
		return
	}
	return
}
func (api *AdminAPI) GetClusterStat() (cs *proto.ClusterStatInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminClusterStat)
	request.addHeader("isTimeOut", "false")
	var buf []byte
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	cs = &proto.ClusterStatInfo{}
	if err = json.Unmarshal(buf, &cs); err != nil {
		return
	}
	return
}
func (api *AdminAPI) ListZones() (zoneViews []*proto.ZoneView, err error) {
	var request = newAPIRequest(http.MethodGet, proto.GetAllZones)
	var buf []byte
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	zoneViews = make([]*proto.ZoneView, 0)
	if err = json.Unmarshal(buf, &zoneViews); err != nil {
		return
	}
	return
}
func (api *AdminAPI) Topo() (topo *proto.TopologyView, err error ){
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.GetTopologyView)
	if buf, err = api.mc.serveRequest(request); err != nil{
		return
	}
	topo = &proto.TopologyView{}
	if err = json.Unmarshal(buf, &topo); err != nil {
		return
	}
	return
}

func (api *AdminAPI) GetDataPartition(volName string, partitionID uint64) (partition *proto.DataPartitionInfo, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.AdminGetDataPartition)
	request.addParam("id", strconv.Itoa(int(partitionID)))
	request.addParam("name", volName)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	partition = &proto.DataPartitionInfo{}
	if err = json.Unmarshal(buf, &partition); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DiagnoseDataPartition() (diagnosis *proto.DataPartitionDiagnosis, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.AdminDiagnoseDataPartition)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	diagnosis = &proto.DataPartitionDiagnosis{}
	if err = json.Unmarshal(buf, &diagnosis); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DiagnoseMetaPartition() (diagnosis *proto.MetaPartitionDiagnosis, err error) {
	var buf []byte
	var request = newAPIRequest(http.MethodGet, proto.AdminDiagnoseMetaPartition)
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	diagnosis = &proto.MetaPartitionDiagnosis{}
	if err = json.Unmarshal(buf, &diagnosis); err != nil {
		return
	}
	return
}

func (api *AdminAPI) LoadDataPartition(volName string, partitionID uint64) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminLoadDataPartition)
	request.addParam("id", strconv.Itoa(int(partitionID)))
	request.addParam("name", volName)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) CreateDataPartition(volName string, count int) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminCreateDataPartition)
	request.addParam("name", volName)
	request.addParam("count", strconv.Itoa(count))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DecommissionDataPartition(dataPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminDecommissionDataPartition)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DecommissionMetaPartition(metaPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminDecommissionMetaPartition)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DeleteDataReplica(dataPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminDeleteDataReplica)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) AddDataReplica(dataPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminAddDataReplica)
	request.addParam("id", strconv.FormatUint(dataPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DeleteMetaReplica(metaPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminDeleteMetaReplica)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) AddMetaReplica(metaPartitionID uint64, nodeAddr string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminAddMetaReplica)
	request.addParam("id", strconv.FormatUint(metaPartitionID, 10))
	request.addParam("addr", nodeAddr)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) DeleteVolume(volName, authKey string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminDeleteVol)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) UpdateVolume(volName string, capacity uint64, replicas int, followerRead, authenticate, enableToken bool, authKey, zoneName string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminUpdateVol)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	request.addParam("replicaNum", strconv.Itoa(replicas))
	request.addParam("followerRead", strconv.FormatBool(followerRead))
	request.addParam("enableToken", strconv.FormatBool(enableToken))
	request.addParam("authenticate", strconv.FormatBool(authenticate))
	request.addParam("zoneName", zoneName)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) VolShrink(volName string, capacity uint64, authKey string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminVolShrink)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) VolExpand(volName string, capacity uint64, authKey string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminVolExpand)
	request.addParam("name", volName)
	request.addParam("authKey", authKey)
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) CreateVolume(volName, owner string, mpCount int,
	dpSize uint64, capacity uint64, replicas int, followerRead bool, zoneName string, crossZone bool) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminCreateVol)
	request.addParam("name", volName)
	request.addParam("owner", owner)
	request.addParam("mpCount", strconv.Itoa(mpCount))
	request.addParam("size", strconv.FormatUint(dpSize, 10))
	request.addParam("capacity", strconv.FormatUint(capacity, 10))
	request.addParam("followerRead", strconv.FormatBool(followerRead))
	request.addParam("zoneName", zoneName)
	request.addParam("crossZone", strconv.FormatBool(crossZone))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) CreateDefaultVolume(volName, owner string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminCreateVol)
	request.addParam("name", volName)
	request.addParam("owner", owner)
	request.addParam("capacity", "10")
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) GetVolumeSimpleInfo(volName string) (vv *proto.SimpleVolView, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminGetVol)
	request.addParam("name", volName)
	var buf []byte
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	vv = &proto.SimpleVolView{}
	if err = json.Unmarshal(buf, &vv); err != nil {
		return
	}
	return
}

func (api *AdminAPI) GetClusterInfo() (ci *proto.ClusterInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminGetIP)
	var buf []byte
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	ci = &proto.ClusterInfo{}
	if err = json.Unmarshal(buf, &ci); err != nil {
		return
	}
	return
}

func (api *AdminAPI) CreateMetaPartition(volName string, inodeStart uint64) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminCreateMetaPartition)
	request.addParam("name", volName)
	request.addParam("start", strconv.FormatUint(inodeStart, 10))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) ListVols(keywords string) (volsInfo []*proto.VolInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminListVols)
	request.addParam("keywords", keywords)
	var buf []byte
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	volsInfo = make([]*proto.VolInfo, 0)
	if err = json.Unmarshal(buf, &volsInfo); err != nil {
		return
	}
	return
}

func (api *AdminAPI) IsFreezeCluster(isFreeze bool) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminClusterFreeze)
	request.addParam("enable", strconv.FormatBool(isFreeze))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) SetMetaNodeThreshold(threshold float64) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminSetMetaNodeThreshold)
	request.addParam("threshold", strconv.FormatFloat(threshold, 'f', 6, 64))
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) SetDeleteParas(batchCount, markDeleteRate, deleteWorkerSleepMs, autoRepairRate string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminSetNodeInfo)
	request.addParam("batchCount", batchCount)
	request.addParam("markDeleteRate", markDeleteRate)
	request.addParam("deleteWorkerSleepMs", deleteWorkerSleepMs)
	request.addParam("autoRepairRate", autoRepairRate)

	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *AdminAPI) GetDeleteParas() (delParas map[string]string, err error) {
	var request = newAPIRequest(http.MethodGet, proto.AdminGetNodeInfo)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	var buf []byte
	if buf, err = api.mc.serveRequest(request); err != nil {
		return
	}
	delParas = make(map[string]string)
	if err = json.Unmarshal(buf, &delParas); err != nil {
		return
	}
	return
}
