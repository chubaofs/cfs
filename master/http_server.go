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
	"context"
	"encoding/json"
	"fmt"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/introspection"
	"net/http"
	"net/http/httputil"

	"github.com/gorilla/mux"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

func (m *Server) startHTTPService(modulename string, cfg *config.Config) {
	router := mux.NewRouter().SkipClean(true)
	m.registerAPIRoutes(router)
	m.registerAPIMiddleware(router)
	exporter.InitWithRouter(modulename, cfg, router, m.port)
	var server = &http.Server{
		Addr:    colonSplit + m.port,
		Handler: router,
	}
	var serveAPI = func() {
		if err := server.ListenAndServe(); err != nil {
			log.LogErrorf("serveAPI: serve http server failed: err(%v)", err)
			return
		}
	}
	go serveAPI()
	m.apiServer = server
	return
}

func (m *Server) registerAPIMiddleware(route *mux.Router) {
	var interceptor mux.MiddlewareFunc = func(next http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				log.LogDebugf("action[interceptor] request, method[%v] path[%v] query[%v]", r.Method, r.URL.Path, r.URL.Query())
				if mux.CurrentRoute(r).GetName() == proto.AdminGetIP {
					next.ServeHTTP(w, r)
					return
				}
				if m.partition.IsRaftLeader() {
					if m.metaReady {
						next.ServeHTTP(w, r)
						return
					}
					log.LogWarnf("action[interceptor] leader meta has not ready")
					http.Error(w, m.leaderInfo.addr, http.StatusBadRequest)
					return
				}
				if m.leaderInfo.addr == "" {
					log.LogErrorf("action[interceptor] no leader,request[%v]", r.URL)
					http.Error(w, "no leader", http.StatusBadRequest)
					return
				}
				m.proxy(w, r)
			})
	}
	route.Use(interceptor)
}

func (m *Server) registerAPIRoutes(router *mux.Router) {
	//graphql api for cluster
	cs := &ClusterService{user: m.user, cluster: m.cluster, conf: m.config, leaderInfo: m.leaderInfo}
	m.registerHandler(router, proto.AdminClusterAPI, cs.Schema())

	us := &UserService{user: m.user, cluster: m.cluster}
	m.registerHandler(router, proto.AdminUserAPI, us.Schema())

	vs := &VolumeService{user: m.user, cluster: m.cluster}
	m.registerHandler(router, proto.AdminVolumeAPI, vs.Schema())

	// cluster management APIs
	router.NewRoute().Name(proto.AdminGetIP).
		Methods(http.MethodGet).
		Path(proto.AdminGetIP).
		HandlerFunc(m.getIPAddr)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetCluster).
		HandlerFunc(m.getCluster)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminClusterFreeze).
		HandlerFunc(m.setupAutoAllocation)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddRaftNode).
		HandlerFunc(m.addRaftNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.RemoveRaftNode).
		HandlerFunc(m.removeRaftNode)
	router.NewRoute().Methods(http.MethodGet).Path(proto.AdminClusterStat).HandlerFunc(m.clusterStat)

	// volume management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminCreateVol).
		HandlerFunc(m.createVol)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetVol).
		HandlerFunc(m.getVolSimpleInfo)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDeleteVol).
		HandlerFunc(m.markDeleteVol)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateVol).
		HandlerFunc(m.updateVol)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminVolShrink).
		HandlerFunc(m.volShrink)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminVolExpand).
		HandlerFunc(m.volExpand)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.ClientVol).
		HandlerFunc(m.getVol)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientVolStat).
		HandlerFunc(m.getVolStatInfo)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetTopologyView).
		HandlerFunc(m.getTopology)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminListVols).
		HandlerFunc(m.listVols)

	// node task response APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetDataNodeTaskResponse).
		HandlerFunc(m.handleDataNodeTaskResponse)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.GetMetaNodeTaskResponse).
		HandlerFunc(m.handleMetaNodeTaskResponse)

	// meta partition management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminLoadMetaPartition).
		HandlerFunc(m.loadMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDecommissionMetaPartition).
		HandlerFunc(m.decommissionMetaPartition)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientMetaPartitions).
		HandlerFunc(m.getMetaPartitions)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientMetaPartition).
		HandlerFunc(m.getMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminCreateMetaPartition).
		HandlerFunc(m.createMetaPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminAddMetaReplica).
		HandlerFunc(m.addMetaReplica)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDeleteMetaReplica).
		HandlerFunc(m.deleteMetaReplica)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDiagnoseMetaPartition).
		HandlerFunc(m.diagnoseMetaPartition)

	// data partition management APIs
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.AdminGetDataPartition).
		HandlerFunc(m.getDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminCreateDataPartition).
		HandlerFunc(m.createDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminLoadDataPartition).
		HandlerFunc(m.loadDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDecommissionDataPartition).
		HandlerFunc(m.decommissionDataPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDiagnoseDataPartition).
		HandlerFunc(m.diagnoseDataPartition)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.ClientDataPartitions).
		HandlerFunc(m.getDataPartitions)

	// meta node management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddMetaNode).
		HandlerFunc(m.addMetaNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionMetaNode).
		HandlerFunc(m.decommissionMetaNode)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetMetaNode).
		HandlerFunc(m.getMetaNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetMetaNodeThreshold).
		HandlerFunc(m.setMetaNodeThreshold)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminAddDataReplica).
		HandlerFunc(m.addDataReplica)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminDeleteDataReplica).
		HandlerFunc(m.deleteDataReplica)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateMetaNode).
		HandlerFunc(m.updateMetaNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateDataNode).
		HandlerFunc(m.updateDataNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetInvalidNodes).
		HandlerFunc(m.checkInvalidIDNodes)

	// data node management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AddDataNode).
		HandlerFunc(m.addDataNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionDataNode).
		HandlerFunc(m.decommissionDataNode)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetDataNode).
		HandlerFunc(m.getDataNode)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.DecommissionDisk).
		HandlerFunc(m.decommissionDisk)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminSetNodeInfo).
		HandlerFunc(m.setNodeInfoHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetNodeInfo).
		HandlerFunc(m.getNodeInfoHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetIsDomainOn).
		HandlerFunc(m.getIsDomainOn)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetAllNodeSetGrpInfo).
		HandlerFunc(m.getAllNodeSetGrpInfoHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminGetNodeSetGrpInfo).
        HandlerFunc(m.getNodeSetGrpInfoHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateNodeSetCapcity).
		HandlerFunc(m.updateNodeSetCapacityHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateNodeSetId).
		HandlerFunc(m.updateNodeSetIdHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateDomainDataUseRatio).
		HandlerFunc(m.updateDataUseRatioHandler)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.AdminUpdateZoneExcludeRatio).
		HandlerFunc(m.updateZoneExcludeRatioHandler)

	// user management APIs
	router.NewRoute().Methods(http.MethodPost).
		Path(proto.UserCreate).
		HandlerFunc(m.createUser)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserDelete).
		HandlerFunc(m.deleteUser)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserUpdate).
		HandlerFunc(m.updateUser)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserUpdatePolicy).
		HandlerFunc(m.updateUserPolicy)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserRemovePolicy).
		HandlerFunc(m.removeUserPolicy)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserDeleteVolPolicy).
		HandlerFunc(m.deleteUserVolPolicy)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.UserGetAKInfo).
		HandlerFunc(m.getUserAKInfo)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.UserGetInfo).
		HandlerFunc(m.getUserInfo)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.UserList).
		HandlerFunc(m.getAllUsers)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UserTransferVol).
		HandlerFunc(m.transferUserVol)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.UsersOfVol).
		HandlerFunc(m.getUsersOfVol)

	// zone management APIs
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(proto.UpdateZone).
		HandlerFunc(m.updateZone)
	router.NewRoute().Methods(http.MethodGet).
		Path(proto.GetAllZones).
		HandlerFunc(m.listZone)
}

func (m *Server) registerHandler(router *mux.Router, model string, schema *graphql.Schema) {
	introspection.AddIntrospectionToSchema(schema)

	gHandler := graphql.HTTPHandler(schema)
	router.NewRoute().Name(model).Methods(http.MethodGet, http.MethodPost).Path(model).HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		userID := request.Header.Get(proto.UserKey)
		if userID == "" {
			ErrResponse(writer, fmt.Errorf("not found [%s] in header", proto.UserKey))
			return
		}

		if ui, err := m.user.getUserInfo(userID); err != nil {
			ErrResponse(writer, fmt.Errorf("user:[%s] not found ", userID))
			return
		} else {
			request = request.WithContext(context.WithValue(request.Context(), proto.UserInfoKey, ui))
		}

		gHandler.ServeHTTP(writer, request)
	})
}
func ErrResponse(w http.ResponseWriter, err error) {
	response := struct {
		Errors []string `json:"errors"`
	}{
		Errors: []string{err.Error()},
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	if _, e := w.Write(responseJSON); e != nil {
		log.LogErrorf("send response has err:[%s]", e)
	}
}

func (m *Server) newReverseProxy() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{Director: func(request *http.Request) {
		request.URL.Scheme = "http"
		request.URL.Host = m.leaderInfo.addr
	}}
}

func (m *Server) proxy(w http.ResponseWriter, r *http.Request) {
	m.reverseProxy.ServeHTTP(w, r)
}
