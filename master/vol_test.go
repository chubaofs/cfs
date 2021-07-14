package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"testing"
	"time"
)

func TestAutoCreateDataPartitions(t *testing.T) {
	commonVol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	commonVol.Capacity = 300 * util.TB
	dpCount := len(commonVol.dataPartitions.partitions)
	commonVol.dataPartitions.readableAndWritableCnt = 0
	server.cluster.DisableAutoAllocate = false
	t.Logf("status[%v],disableAutoAlloc[%v],cap[%v]\n",
		commonVol.Status, server.cluster.DisableAutoAllocate, commonVol.Capacity)
	commonVol.checkAutoDataPartitionCreation(server.cluster)
	newDpCount := len(commonVol.dataPartitions.partitions)
	if dpCount == newDpCount {
		t.Errorf("autoCreateDataPartitions failed,expand 0 data partitions,oldCount[%v],curCount[%v]", dpCount, newDpCount)
		return
	}
}

func TestCheckVol(t *testing.T) {
	commonVol.checkStatus(server.cluster)
	commonVol.checkMetaPartitions(server.cluster)
	commonVol.checkDataPartitions(server.cluster)
	log.LogFlush()
	fmt.Printf("writable data partitions[%v]\n", commonVol.dataPartitions.readableAndWritableCnt)
}

func TestVol(t *testing.T) {
	capacity := 300
	name := "test1"
	createVol(name, t)
	//report mp/dp info to master
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	//check status
	server.cluster.checkMetaPartitions()
	server.cluster.checkDataPartitions()
	server.cluster.checkLoadMetaPartitions()
	server.cluster.doLoadDataPartitions()
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	vol.checkStatus(server.cluster)
	getVol(name, t)
	updateVol(name, capacity, t)
	statVol(name, t)
	markDeleteVol(name, t)
	getSimpleVol(name, t)
	vol.checkStatus(server.cluster)
	vol.deleteVolFromStore(server.cluster)
}

func createVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&replicas=3&type=extent&capacity=100&owner=cfs&mpCount=2&zoneName=%v", hostAddr, proto.AdminCreateVol, name, testZone2)
	fmt.Println(reqURL)
	process(reqURL, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	checkDataPartitionsWritableTest(vol, t)
	checkMetaPartitionsWritableTest(vol, t)
}

func checkDataPartitionsWritableTest(vol *Vol, t *testing.T) {
	if len(vol.dataPartitions.partitions) == 0 {
		return
	}
	partition := vol.dataPartitions.partitions[0]
	if partition.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, partition.Status)
		return
	}

	//after check data partitions ,the status must be writable
	vol.checkDataPartitions(server.cluster)
	partition = vol.dataPartitions.partitions[0]
	if partition.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, partition.Status)
		return
	}
}

func checkMetaPartitionsWritableTest(vol *Vol, t *testing.T) {
	if len(vol.MetaPartitions) == 0 {
		t.Error("no meta partition")
		return
	}

	for _, mp := range vol.MetaPartitions {
		if mp.Status != proto.ReadWrite {
			t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, mp.Status)
			return
		}
	}
	maxPartitionID := vol.maxPartitionID()
	maxMp := vol.MetaPartitions[maxPartitionID]
	//after check meta partitions ,the status must be writable
	maxMp.checkStatus(server.cluster.Name, false, int(vol.mpReplicaNum), maxPartitionID)
	if maxMp.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, maxMp.Status)
		return
	}
}

func getSimpleVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.AdminGetVol, name)
	fmt.Println(reqURL)
	process(reqURL, t)
}

func getVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.ClientVol, name, buildAuthKey("cfs"))
	fmt.Println(reqURL)
	process(reqURL, t)
}

func updateVol(name string, capacity int, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v",
		hostAddr, proto.AdminUpdateVol, name, capacity, buildAuthKey("cfs"))
	fmt.Println(reqURL)
	process(reqURL, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	if vol.Capacity != uint64(capacity) {
		t.Errorf("update vol failed,expect[%v],real[%v]", capacity, vol.Capacity)
		return
	}
}

func statVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v",
		hostAddr, proto.ClientVolStat, name)
	fmt.Println(reqURL)
	process(reqURL, t)
}

func markDeleteVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v",
		hostAddr, proto.AdminDeleteVol, name, buildAuthKey("cfs"))
	fmt.Println(reqURL)
	process(reqURL, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	if vol.Status != markDelete {
		t.Errorf("markDeleteVol failed,expect[%v],real[%v]", markDelete, vol.Status)
		return
	}
}

//func TestVolReduceReplicaNum(t *testing.T) {
//	volName := "reduce-replica-num"
//	vol, err := server.cluster.createVol(volName, volName, testZone2, 3, 3, util.DefaultDataPartitionSize,
//		100, false, false, false, false)
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	server.cluster.checkDataNodeHeartbeat()
//	time.Sleep(2 * time.Second)
//	for _, dp := range vol.dataPartitions.partitionMap {
//		t.Logf("dp[%v] replicaNum[%v],hostLen[%v]\n", dp.PartitionID, dp.ReplicaNum, len(dp.Hosts))
//	}
//	oldReplicaNum := vol.dpReplicaNum
//	reqURL := fmt.Sprintf("%v%v?name=%v&capacity=%v&replicaNum=%v&authKey=%v",
//		hostAddr, proto.AdminUpdateVol, volName, 100, 2, buildAuthKey(volName))
//	fmt.Println(reqURL)
//	process(reqURL, t)
//	if vol.dpReplicaNum != 2 {
//		t.Error("update vol replica Num to [2] failed")
//		return
//	}
//	for i := 0; i < int(oldReplicaNum); i++ {
//		t.Logf("before check,needToLowerReplica[%v] \n", vol.NeedToLowerReplica)
//		vol.NeedToLowerReplica = true
//		t.Logf(" after check,needToLowerReplica[%v]\n", vol.NeedToLowerReplica)
//		vol.checkReplicaNum(server.cluster)
//	}
//	vol.NeedToLowerReplica = true
//	//check more once,the replica num of data partition must be equal with vol.dpReplicaNun
//	vol.checkReplicaNum(server.cluster)
//	for _, dp := range vol.dataPartitions.partitionMap {
//		if dp.ReplicaNum != vol.dpReplicaNum || len(dp.Hosts) != int(vol.dpReplicaNum) {
//			t.Errorf("dp.replicaNum[%v],hosts[%v],vol.dpReplicaNum[%v]\n", dp.ReplicaNum, len(dp.Hosts), vol.dpReplicaNum)
//			return
//		}
//	}
//}

func TestConcurrentReadWriteDataPartitionMap(t *testing.T) {
	name := "TestConcurrentReadWriteDataPartitionMap"
	var volID uint64 = 1
	var createTime = time.Now().Unix()
	vol := newVol(volID, name, name, "", util.DefaultDataPartitionSize, 100, defaultReplicaNum,
		defaultReplicaNum, false, false, false,false, createTime, "")
	// unavailable mp
	mp1 := newMetaPartition(1, 1, defaultMaxMetaPartitionInodeID, 3, name, volID)
	vol.addMetaPartition(mp1)
	//readonly mp
	mp2 := newMetaPartition(2, 1, defaultMaxMetaPartitionInodeID, 3, name, volID)
	mp2.Status = proto.ReadOnly
	vol.addMetaPartition(mp2)
	vol.updateViewCache(server.cluster)
	for id := 0; id < 30000; id++ {
		dp := newDataPartition(uint64(id), 3, name, volID)
		vol.dataPartitions.put(dp)
	}
	go func() {
		var id uint64 = 30000
		for {
			id++
			dp := newDataPartition(id, 3, name, volID)
			vol.dataPartitions.put(dp)
			time.Sleep(time.Second)
		}
	}()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		vol.updateViewCache(server.cluster)
	}
}
