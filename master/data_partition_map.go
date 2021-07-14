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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"runtime"
	"sync"
	"time"
)

// DataPartitionMap stores all the data partitionMap
type DataPartitionMap struct {
	sync.RWMutex
	partitionMap           map[uint64]*DataPartition
	readableAndWritableCnt int    // number of readable and writable partitionMap
	lastLoadedIndex        uint64 // last loaded partition index
	lastReleasedIndex      uint64 // last released partition index
	partitions             []*DataPartition
	responseCache          []byte
	lastAutoCreateTime     time.Time
	volName                string
}

func newDataPartitionMap(volName string) (dpMap *DataPartitionMap) {
	dpMap = new(DataPartitionMap)
	dpMap.partitionMap = make(map[uint64]*DataPartition, 0)
	dpMap.partitions = make([]*DataPartition, 0)
	dpMap.responseCache = make([]byte, 0)
	dpMap.volName = volName
	return
}

func (dpMap *DataPartitionMap) get(ID uint64) (*DataPartition, error) {
	dpMap.RLock()
	defer dpMap.RUnlock()
	if v, ok := dpMap.partitionMap[ID]; ok {
		return v, nil
	}
	return nil, proto.ErrDataPartitionNotExists
}

func (dpMap *DataPartitionMap) put(dp *DataPartition) {
	dpMap.Lock()
	defer dpMap.Unlock()
	_, ok := dpMap.partitionMap[dp.PartitionID]
	if !ok {
		dpMap.partitions = append(dpMap.partitions, dp)
		dpMap.partitionMap[dp.PartitionID] = dp
		return
	}

	// replace the old partition with dp in the map and array
	dpMap.partitionMap[dp.PartitionID] = dp
	dataPartitions := make([]*DataPartition, 0)
	for index, partition := range dpMap.partitions {
		if partition.PartitionID == dp.PartitionID {
			dataPartitions = append(dataPartitions, dpMap.partitions[:index]...)
			dataPartitions = append(dataPartitions, dp)
			dataPartitions = append(dataPartitions, dpMap.partitions[index+1:]...)
			dpMap.partitions = dataPartitions
			break
		}
	}
}

func (dpMap *DataPartitionMap) setReadWriteDataPartitions(readWrites int, clusterName string) {
	dpMap.Lock()
	defer dpMap.Unlock()
	dpMap.readableAndWritableCnt = readWrites
}

func (dpMap *DataPartitionMap) getDataPartitionResponseCache() []byte {
	dpMap.RLock()
	defer dpMap.RUnlock()
	return dpMap.responseCache
}

func (dpMap *DataPartitionMap) setDataPartitionResponseCache(responseCache []byte) {
	dpMap.Lock()
	defer dpMap.Unlock()
	if responseCache != nil {
		dpMap.responseCache = responseCache
	}
}

func (dpMap *DataPartitionMap) updateResponseCache(needsUpdate bool, minPartitionID uint64) (body []byte, err error) {
	responseCache := dpMap.getDataPartitionResponseCache()
	if responseCache == nil || needsUpdate || len(responseCache) == 0 {
		dpResps := dpMap.getDataPartitionsView(minPartitionID)
		if len(dpResps) == 0 {
			log.LogError(fmt.Sprintf("action[updateDpResponseCache],volName[%v] minPartitionID:%v,err:%v",
				dpMap.volName, minPartitionID, proto.ErrNoAvailDataPartition))
			return nil, proto.ErrNoAvailDataPartition
		}
		cv := proto.NewDataPartitionsView()
		cv.DataPartitions = dpResps
		reply := newSuccessHTTPReply(cv)
		if body, err = json.Marshal(reply); err != nil {
			log.LogError(fmt.Sprintf("action[updateDpResponseCache],minPartitionID:%v,err:%v",
				minPartitionID, err.Error()))
			return nil, proto.ErrMarshalData
		}
		dpMap.setDataPartitionResponseCache(body)
		return
	}
	body = make([]byte, len(responseCache))
	copy(body, responseCache)

	return
}

func (dpMap *DataPartitionMap) getDataPartitionsView(minPartitionID uint64) (dpResps []*proto.DataPartitionResponse) {
	dpResps = make([]*proto.DataPartitionResponse, 0)
	log.LogDebugf("volName[%v] DataPartitionMapLen[%v],DataPartitionsLen[%v],minPartitionID[%v]",
		dpMap.volName, len(dpMap.partitionMap), len(dpMap.partitions), minPartitionID)
	dpMap.RLock()
	defer dpMap.RUnlock()
	for _, dp := range dpMap.partitionMap {
		if dp.PartitionID <= minPartitionID {
			continue
		}
		dpResp := dp.convertToDataPartitionResponse()
		dpResps = append(dpResps, dpResp)
	}

	return
}

func (dpMap *DataPartitionMap) getDataPartitionsToBeReleased(numberOfDataPartitionsToFree int, secondsToFreeDataPartitionAfterLoad int64) (partitions []*DataPartition, startIndex uint64) {
	partitions = make([]*DataPartition, 0)
	dpMap.RLock()
	defer dpMap.RUnlock()
	dpLen := len(dpMap.partitions)
	if dpLen == 0 {
		return
	}
	startIndex = dpMap.lastReleasedIndex
	count := numberOfDataPartitionsToFree
	if dpLen < numberOfDataPartitionsToFree {
		count = dpLen
	}
	for i := 0; i < count; i++ {
		if dpMap.lastReleasedIndex >= uint64(dpLen) {
			dpMap.lastReleasedIndex = 0
		}
		dp := dpMap.partitions[dpMap.lastReleasedIndex]
		dpMap.lastReleasedIndex++
		if time.Now().Unix()-dp.LastLoadedTime >= secondsToFreeDataPartitionAfterLoad {
			partitions = append(partitions, dp)
		}
	}

	return
}

func (dpMap *DataPartitionMap) freeMemOccupiedByDataPartitions(partitions []*DataPartition) {
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
					log.LogError(fmt.Sprintf("[%v] freeMemOccupiedByDataPartitions panic %v: %s\n", dpMap.volName, err, buf))
				}
			}()
			dp.releaseDataPartition()
		}(dp)
	}
	wg.Wait()

}

func (dpMap *DataPartitionMap) getDataPartitionsToBeChecked(loadFrequencyTime int64) (partitions []*DataPartition, startIndex uint64) {
	partitions = make([]*DataPartition, 0)
	dpMap.RLock()
	defer dpMap.RUnlock()
	dpLen := len(dpMap.partitions)
	if dpLen == 0 {
		return
	}
	startIndex = dpMap.lastLoadedIndex

	// determine the number of data partitions to load
	count := dpLen / intervalToLoadDataPartition
	if count == 0 {
		count = 1
	}

	for i := 0; i < count; i++ {
		if dpMap.lastLoadedIndex >= (uint64)(len(dpMap.partitions)) {
			dpMap.lastLoadedIndex = 0
		}
		dp := dpMap.partitions[dpMap.lastLoadedIndex]
		dpMap.lastLoadedIndex++

		if time.Now().Unix()-dp.LastLoadedTime >= loadFrequencyTime {
			partitions = append(partitions, dp)
		}
	}

	return
}

func (dpMap *DataPartitionMap) totalUsedSpace() (totalUsed uint64) {
	dpMap.RLock()
	defer dpMap.RUnlock()
	for _, dp := range dpMap.partitions {
		totalUsed = totalUsed + dp.getMaxUsedSpace()
	}
	return
}

func (dpMap *DataPartitionMap) setAllDataPartitionsToReadOnly() {
	dpMap.Lock()
	defer dpMap.Unlock()
	for _, dp := range dpMap.partitions {
		dp.Status = proto.ReadOnly
	}
}

func (dpMap *DataPartitionMap) checkBadDiskDataPartitions(diskPath, nodeAddr string) (partitions []*DataPartition) {
	dpMap.RLock()
	defer dpMap.RUnlock()
	partitions = make([]*DataPartition, 0)
	for _, dp := range dpMap.partitionMap {
		if dp.containsBadDisk(diskPath, nodeAddr) {
			partitions = append(partitions, dp)
		}
	}
	return
}
