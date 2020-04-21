// Copyright 2020 The Chubao Authors.
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

package ecnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	EcPartitionPrefix           = "ecpartition"
	EcPartitionMetaDataFileName = "META"
	TempMetaDataFileName        = ".meta"
	TimeLayout                  = "2006-01-02 15:04:05"

	IntervalToUpdatePartitionSize = 60 // interval to update the partition size
)

type EcPartition struct {
	clusterID string

	partitionID   uint64
	partitionSize int
	volumeID      string

	dataNodeNum     uint32
	parityNodeNum   uint32
	nodeIndex       uint32
	dataNodes       []string
	parityNodes     []string
	stripeSize      uint32
	stripeBlockSize uint32

	partitionStatus int

	disk        *Disk
	path        string
	used        int
	extentStore *storage.ExtentStore
	storeC      chan uint64
	stopC       chan bool

	intervalToUpdatePartitionSize int64
	loadExtentHeaderStatus        int

	config *EcPartitionCfg
}

type EcPartitionCfg struct {
	VolName         string `json:"vol_name"`
	ClusterID       string `json:"cluster_id"`
	PartitionID     uint64 `json:"partition_id"`
	PartitionSize   int    `json:"partition_size"`
	StripeBlockSize int    `json:"stripe_block_size"`

	DataNodeNum   uint32   `json:"data_node_num"`
	ParityNodeNum uint32   `json:"parity_node_num"`
	NodeIndex     uint32   `json:"node_index"`
	DataNodes     []string `json:"data_nodes"`
	ParityNodes   []string `json:"parity_nodes"`
}

type EcPartitionMetaData struct {
	PartitionID     uint64
	PartitionSize   int
	VolumeID        string
	StripeBlockSize int
	DataNodeNum     uint32
	ParityNodeNum   uint32
	NodeIndex       uint32
	DataNodes       []string
	ParityNodes     []string

	CreateTime string
}

// Disk returns the disk instance.
func (ep *EcPartition) Disk() *Disk {
	return ep.disk
}

func (ep *EcPartition) IsRejectWrite() bool {
	return ep.Disk().RejectWrite
}

// Status returns the partition status.
func (ep *EcPartition) Status() int {
	return ep.partitionStatus
}

// Size returns the partition size.
func (ep *EcPartition) Size() int {
	return ep.partitionSize
}

// Used returns the used space.
func (ep *EcPartition) Used() int {
	return ep.used
}

// Available returns the available space.
func (ep *EcPartition) Available() int {
	return ep.partitionSize - ep.used
}

func (ep *EcPartition) GetExtentCount() int {
	return ep.extentStore.GetExtentCount()
}

func (ep *EcPartition) Path() string {
	return ep.path
}

func (ep *EcPartition) DataNodeNum() uint32 {
	return ep.dataNodeNum
}

func (ep *EcPartition) ParityNodeNum() uint32 {
	return ep.parityNodeNum
}

func (ep *EcPartition) NodeIndex() uint32 {
	return ep.nodeIndex
}

func (ep *EcPartition) DataNodes() []string {
	return ep.dataNodes
}

func (ep *EcPartition) ParityNodes() []string {
	return ep.parityNodes
}

func (ep *EcPartition) StripeSize() uint32 {
	return ep.stripeSize
}

func (ep *EcPartition) StripeBlockSize() uint32 {
	return ep.stripeBlockSize
}

func (ep *EcPartition) ExtentStore() *storage.ExtentStore {
	return ep.extentStore
}

func (ep *EcPartition) checkIsDiskError(err error) (diskError bool) {
	if err == nil {
		return
	}

	if IsDiskErr(err.Error()) {
		mesg := fmt.Sprintf("disk path %v error on %v", ep.Path(), localIP)
		log.LogErrorf(mesg)
		ep.disk.incReadErrCnt()
		ep.disk.incWriteErrCnt()
		ep.disk.Status = proto.Unavailable
		ep.statusUpdate()
		diskError = true
	}
	return
}

func (ep *EcPartition) computeUsage() {
	var (
		used  int64
		files []os.FileInfo
		err   error
	)
	if time.Now().Unix()-ep.intervalToUpdatePartitionSize < IntervalToUpdatePartitionSize {
		return
	}
	if files, err = ioutil.ReadDir(ep.path); err != nil {
		return
	}
	for _, file := range files {
		isExtent := storage.RegexpExtentFile.MatchString(file.Name())
		if !isExtent {
			continue
		}
		used += file.Size()
	}
	ep.used = int(used)
	ep.intervalToUpdatePartitionSize = time.Now().Unix()
}

func (ep *EcPartition) statusUpdate() {
	status := proto.ReadWrite
	ep.computeUsage()

	if ep.used >= ep.partitionSize {
		status = proto.ReadOnly
	}
	if ep.extentStore.GetExtentCount() >= storage.MaxExtentCount {
		status = proto.ReadOnly
	}
	if ep.Status() == proto.Unavailable {
		status = proto.Unavailable
	}

	ep.partitionStatus = int(math.Min(float64(status), float64(ep.disk.Status)))
}

func (ep *EcPartition) statusUpdateScheduler() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			ep.statusUpdate()
		case <-ep.stopC:
			ticker.Stop()
			return
		}
	}
}

// PersistMetaData persists the file metadata on the disk
func (ep EcPartition) PersistMetaData() (err error) {
	fileName := path.Join(ep.Path(), TempMetaDataFileName)
	metadataFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return
	}
	defer func() {
		metadataFile.Sync()
		metadataFile.Close()
		os.Remove(fileName)
	}()

	md := &EcPartitionMetaData{
		PartitionID:     ep.config.PartitionID,
		PartitionSize:   ep.config.PartitionSize,
		VolumeID:        ep.config.VolName,
		StripeBlockSize: ep.config.StripeBlockSize,
		DataNodeNum:     ep.config.DataNodeNum,
		ParityNodeNum:   ep.config.ParityNodeNum,
		NodeIndex:       ep.config.NodeIndex,
		DataNodes:       ep.config.DataNodes,
		ParityNodes:     ep.config.ParityNodes,

		CreateTime: time.Now().Format(TimeLayout),
	}
	metadata, err := json.Marshal(md)
	if err != nil {
		return
	}

	_, err = metadataFile.Write(metadata)
	if err != nil {
		return
	}
	log.LogInfof("PersistMetaData EcPartition(%v) data(%v)", ep.partitionID, string(metadata))
	err = os.Rename(fileName, path.Join(ep.Path(), EcPartitionMetaDataFileName))
	return
}

// newEcPartition
func newEcPartition(epCfg *EcPartitionCfg, disk *Disk) (ep *EcPartition, err error) {
	partitionID := epCfg.PartitionID
	dataPath := path.Join(disk.Path, fmt.Sprintf(EcPartitionPrefix+"_%v_%v", partitionID, epCfg.PartitionSize))
	partition := &EcPartition{
		clusterID: epCfg.ClusterID,

		partitionID:     epCfg.PartitionID,
		partitionSize:   epCfg.PartitionSize,
		volumeID:        epCfg.VolName,
		stripeBlockSize: uint32(epCfg.StripeBlockSize),
		dataNodeNum:     epCfg.DataNodeNum,
		parityNodeNum:   epCfg.ParityNodeNum,
		nodeIndex:       epCfg.NodeIndex,
		dataNodes:       epCfg.DataNodes,
		parityNodes:     epCfg.ParityNodes,

		disk:            disk,
		path:            dataPath,
		stopC:           make(chan bool, 0),
		storeC:          make(chan uint64, 128),
		partitionStatus: proto.ReadWrite,
		config:          epCfg,
	}

	partition.stripeSize = partition.stripeBlockSize * partition.dataNodeNum

	partition.extentStore, err = storage.NewExtentStore(partition.path, epCfg.PartitionID, epCfg.PartitionSize)
	if err != nil {
		return
	}

	disk.AttachEcPartition(partition)
	ep = partition
	go partition.statusUpdateScheduler()
	return
}

// LoadEcPartition load partition from the specified directory when ecnode start
func LoadEcPartition(partitionDir string, disk *Disk) (ep *EcPartition, err error) {
	metaDataRaw, err := ioutil.ReadFile(path.Join(partitionDir, EcPartitionMetaDataFileName))
	if err != nil {
		return
	}

	metaData := &EcPartitionMetaData{}
	err = json.Unmarshal(metaDataRaw, metaData)
	if err != nil {
		return
	}

	volumeID := strings.TrimSpace(metaData.VolumeID)
	if len(volumeID) == 0 || metaData.PartitionID == 0 || metaData.PartitionSize == 0 {
		return
	}

	epCfg := &EcPartitionCfg{
		VolName:         metaData.VolumeID,
		ClusterID:       disk.space.GetClusterID(),
		PartitionID:     metaData.PartitionID,
		PartitionSize:   metaData.PartitionSize,
		StripeBlockSize: metaData.StripeBlockSize,

		DataNodeNum:   metaData.DataNodeNum,
		ParityNodeNum: metaData.ParityNodeNum,
		NodeIndex:     metaData.NodeIndex,
		DataNodes:     metaData.DataNodes,
		ParityNodes:   metaData.ParityNodes,
	}

	ep, err = newEcPartition(epCfg, disk)
	if err != nil {
		return
	}

	disk.space.AttachPartition(ep)
	disk.AddSize(uint64(ep.Size()))
	return
}

// CreateEcPartition create ec partition and return its instance
func CreateEcPartition(epCfg *EcPartitionCfg, disk *Disk, request *proto.CreateEcPartitionRequest) (ep *EcPartition, err error) {
	ep, err = newEcPartition(epCfg, disk)
	if err != nil {
		return
	}

	err = ep.PersistMetaData()
	if err != nil {
		return
	}

	disk.AddSize(uint64(ep.Size()))
	return
}

// IsStripeRead return whether nead read from other node in one stripe
func (ep *EcPartition) IsStripeRead(offset int64, size uint32) (isStripeRead bool) {
	if size > ep.stripeBlockSize {
		return true
	}

	firstOffsetIndex := uint32(offset) % ep.stripeSize / ep.stripeBlockSize
	lastOffsetIndex := (uint32(offset) + size - 1) % ep.stripeSize / ep.stripeBlockSize

	if firstOffsetIndex != ep.nodeIndex || lastOffsetIndex != ep.nodeIndex {
		return true
	}

	return false
}

func (ep *EcPartition) localRead(extentID uint64, offset int64, size uint32) ([]byte, error) {
	store := ep.ExtentStore()

	data := make([]byte, size)

	_, err := store.Read(extentID, offset, int64(size), data, false)
	ep.checkIsDiskError(err)
	return data, err
}

func (ep *EcPartition) remoteRead(nodeIndex uint32, extentID uint64, offset int64, size uint32) (data []byte, err error) {
	request := proto.NewPacket()
	request.ReqID = proto.GenerateRequestID()
	request.Opcode = proto.OpStreamRead
	request.Size = size
	request.ExtentOffset = offset
	request.PartitionID = ep.partitionID
	request.ExtentID = extentID
	request.Magic = proto.ProtoMagic

	conn, err := net.Dial("tcp", ep.dataNodes[nodeIndex])
	if err != nil {
		return
	}
	defer conn.Close()

	err = request.WriteToConn(conn)
	if err != nil {
		err = errors.New(fmt.Sprintf("ExtentStripeRead to host(%v) error(%v)", ep.dataNodes[nodeIndex], err))
		log.LogWarnf("action[streamRepairExtent] err(%v).", err)
		return
	}

	err = request.ReadFromConn(conn, proto.ReadDeadlineTime) // read the response
	if err != nil {
		err = errors.New(fmt.Sprintf("Stripe RemoteRead EcPartition(%v) from host(%v) error(%v)", ep.partitionID,
			ep.dataNodes[nodeIndex], err))
		return
	}
	if request.ResultCode != proto.OpOk {
		err = errors.New(fmt.Sprintf("Stripe RemoteRead EcPartition(%v) from host(%v) error(%v) resultCode(%v)",
			ep.partitionID, ep.dataNodes[nodeIndex], err, request.ResultCode))
		return
	}

	data = request.Data

	return
}

// StripeRead get the data from different node in one stripe
func (ep *EcPartition) StripeRead(extentID uint64, offset int64, size uint32) (data []byte, err error) {
	var wg sync.WaitGroup
	var nodeDatasLock sync.RWMutex

	nodeDatas := make(map[int][]byte)
	errs := make(map[int]error)

	i := int(0)

	for {
		if size == 0 {
			break
		}

		wg.Add(1)
		nodeIndex := uint32(offset) % ep.stripeSize / ep.stripeBlockSize
		currSize := size
		if currSize > ep.stripeBlockSize {
			currSize = ep.stripeBlockSize
		}

		// if offset don't start with a stripeBlock, we need to address read op to stripeBlockSize
		currSize -= uint32(offset) % ep.stripeBlockSize

		if nodeIndex == ep.nodeIndex {
			go func(num int, readOffset int64, readSize uint32) {
				defer wg.Done()

				nodeData, nodeErr := ep.localRead(extentID, readOffset, readSize)

				nodeDatasLock.Lock()
				defer nodeDatasLock.Unlock()
				nodeDatas[num] = nodeData
				errs[num] = nodeErr
				if errs[num] != nil {
					return
				}
			}(i, offset, currSize)
		} else {
			go func(num int, node uint32, readOffset int64, readSize uint32) {
				defer wg.Done()

				nodeData, nodeErr := ep.remoteRead(node, extentID, readOffset, readSize)

				nodeDatasLock.Lock()
				defer nodeDatasLock.Unlock()
				nodeDatas[num] = nodeData
				errs[num] = nodeErr
				if errs[num] != nil {
					return
				}

			}(i, nodeIndex, offset, currSize)
		}

		offset = offset + int64(currSize)
		size = size - currSize
		i++
	}
	wg.Wait()
	var resData []byte

	for j := 0; j < i; j++ {
		if errs[i] != nil {
			err = errs[i]
			return
		}
		resData = append(resData, nodeDatas[j]...)
	}

	return resData, nil
}
