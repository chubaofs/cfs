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

package proto

import "github.com/chubaofs/chubaofs/util/errors"

//err
var (
	ErrSuc                    = errors.New("success")
	ErrInternalError          = errors.New("internal error")
	ErrParamError             = errors.New("parameter error")
	ErrInvalidCfg             = errors.New("bad configuration file")
	ErrPersistenceByRaft      = errors.New("persistence by raft occurred error")
	ErrMarshalData            = errors.New("marshal data error")
	ErrUnmarshalData          = errors.New("unmarshal data error")
	ErrVolNotExists           = errors.New("vol not exists")
	ErrMetaPartitionNotExists = errors.New("meta partition not exists")
	ErrDataPartitionNotExists = errors.New("data partition not exists")
	ErrDataNodeNotExists      = errors.New("data node not exists")
	ErrMetaNodeNotExists      = errors.New("meta node not exists")
	ErrDuplicateVol           = errors.New("duplicate vol")
	ErrActiveDataNodesTooLess = errors.New("no enough active data node")
	ErrActiveMetaNodesTooLess = errors.New("no enough active meta node")
	ErrInvalidMpStart         = errors.New("invalid meta partition start value")
	ErrNoAvailDataPartition   = errors.New("no available data partition")
	ErrReshuffleArray         = errors.New("the array to be reshuffled is nil")

	ErrIllegalDataReplica = errors.New("data replica is illegal")

	ErrMissingReplica       = errors.New("a missing data replica is found")
	ErrHasOneMissingReplica = errors.New("there is a missing replica")

	ErrNoDataNodeToWrite = errors.New("No data node available for creating a data partition")
	ErrNoMetaNodeToWrite = errors.New("No meta node available for creating a meta partition")

	ErrCannotBeOffLine                 = errors.New("cannot take the data replica offline")
	ErrNoDataNodeToCreateDataPartition = errors.New("no enough data nodes for creating a data partition")
	ErrNoZoneToCreateDataPartition     = errors.New("no zone available for creating a data partition")
	ErrNoZoneToCreateMetaPartition     = errors.New("no zone available for creating a meta partition")
	ErrNoNodeSetToCreateDataPartition  = errors.New("no node set available for creating a data partition")
	ErrNoNodeSetToCreateMetaPartition  = errors.New("no node set available for creating a meta partition")
	ErrNoMetaNodeToCreateMetaPartition = errors.New("no enough meta nodes for creating a meta partition")
	ErrIllegalMetaReplica              = errors.New("illegal meta replica")
	ErrNoEnoughReplica                 = errors.New("no enough replicas")
	ErrNoLeader                        = errors.New("no leader")
	ErrVolAuthKeyNotMatch              = errors.New("client and server auth key do not match")
	ErrAuthKeyStoreError               = errors.New("auth keystore error")
	ErrAuthAPIAccessGenRespError       = errors.New("auth API access response error")
	ErrAuthOSCapsOpGenRespError        = errors.New("auth Object Storage Node API response error")
	ErrKeyNotExists                    = errors.New("key not exists")
	ErrDuplicateKey                    = errors.New("duplicate key")
	ErrAccessKeyNotExists              = errors.New("access key not exists")
	ErrInvalidTicket                   = errors.New("invalid ticket")
	ErrExpiredTicket                   = errors.New("expired ticket")
	ErrMasterAPIGenRespError           = errors.New("master API generate response error")
	ErrDuplicateUserID                 = errors.New("duplicate user id")
	ErrUserNotExists                   = errors.New("user not exists")
	ErrReadBodyError                   = errors.New("read request body failed")
	ErrVolPolicyNotExists              = errors.New("vol policy not exists")
	ErrDuplicateAccessKey              = errors.New("duplicate access key")
	ErrHaveNoPolicy                    = errors.New("no vol policy")
	ErrZoneNotExists                   = errors.New("zone not exists")
	ErrOwnVolExists                    = errors.New("own vols not empty")
	ErrSuperAdminExists                = errors.New("super administrator exists ")
	ErrInvalidUserID                   = errors.New("invalid user ID")
	ErrInvalidUserType                 = errors.New("invalid user type")
	ErrNoPermission                    = errors.New("no permission")
	ErrTokenNotFound                   = errors.New("token not found")
	ErrInvalidAccessKey                = errors.New("invalid access key")
	ErrInvalidSecretKey                = errors.New("invalid secret key")
	ErrIsOwner                         = errors.New("user owns the volume")
	ErrZoneNum                         = errors.New("zone num not qualified")
)

// http response error code and error message definitions
const (
	ErrCodeSuccess = iota
	ErrCodeInternalError
	ErrCodeParamError
	ErrCodeInvalidCfg
	ErrCodePersistenceByRaft
	ErrCodeMarshalData
	ErrCodeUnmarshalData
	ErrCodeVolNotExists
	ErrCodeMetaPartitionNotExists
	ErrCodeDataPartitionNotExists
	ErrCodeDataNodeNotExists
	ErrCodeMetaNodeNotExists
	ErrCodeDuplicateVol
	ErrCodeActiveDataNodesTooLess
	ErrCodeActiveMetaNodesTooLess
	ErrCodeInvalidMpStart
	ErrCodeNoAvailDataPartition
	ErrCodeReshuffleArray
	ErrCodeIllegalDataReplica
	ErrCodeMissingReplica
	ErrCodeHasOneMissingReplica
	ErrCodeNoDataNodeToWrite
	ErrCodeNoMetaNodeToWrite
	ErrCodeCannotBeOffLine
	ErrCodeNoDataNodeToCreateDataPartition
	ErrCodeNoZoneToCreateDataPartition
	ErrCodeNoNodeSetToCreateDataPartition
	ErrCodeNoNodeSetToCreateMetaPartition
	ErrCodeNoMetaNodeToCreateMetaPartition
	ErrCodeIllegalMetaReplica
	ErrCodeNoEnoughReplica
	ErrCodeNoLeader
	ErrCodeVolAuthKeyNotMatch
	ErrCodeAuthKeyStoreError
	ErrCodeAuthAPIAccessGenRespError
	ErrCodeAuthRaftNodeGenRespError
	ErrCodeAuthOSCapsOpGenRespError
	ErrCodeAuthReqRedirectError
	ErrCodeAccessKeyNotExists
	ErrCodeInvalidTicket
	ErrCodeExpiredTicket
	ErrCodeMasterAPIGenRespError
	ErrCodeDuplicateUserID
	ErrCodeUserNotExists
	ErrCodeReadBodyError
	ErrCodeVolPolicyNotExists
	ErrCodeDuplicateAccessKey
	ErrCodeHaveNoPolicy
	ErrCodeNoZoneToCreateMetaPartition
	ErrCodeZoneNotExists
	ErrCodeOwnVolExists
	ErrCodeSuperAdminExists
	ErrCodeInvalidUserID
	ErrCodeInvalidUserType
	ErrCodeNoPermission
	ErrCodeTokenNotExist
	ErrCodeInvalidAccessKey
	ErrCodeInvalidSecretKey
	ErrCodeIsOwner
	ErrCodeZoneNumError
)

// Err2CodeMap error map to code
var Err2CodeMap = map[error]int32{
	ErrSuc:                             ErrCodeSuccess,
	ErrInternalError:                   ErrCodeInternalError,
	ErrParamError:                      ErrCodeParamError,
	ErrInvalidCfg:                      ErrCodeInvalidCfg,
	ErrPersistenceByRaft:               ErrCodePersistenceByRaft,
	ErrMarshalData:                     ErrCodeMarshalData,
	ErrUnmarshalData:                   ErrCodeUnmarshalData,
	ErrVolNotExists:                    ErrCodeVolNotExists,
	ErrMetaPartitionNotExists:          ErrCodeMetaPartitionNotExists,
	ErrDataPartitionNotExists:          ErrCodeDataPartitionNotExists,
	ErrDataNodeNotExists:               ErrCodeDataNodeNotExists,
	ErrMetaNodeNotExists:               ErrCodeMetaNodeNotExists,
	ErrDuplicateVol:                    ErrCodeDuplicateVol,
	ErrActiveDataNodesTooLess:          ErrCodeActiveDataNodesTooLess,
	ErrActiveMetaNodesTooLess:          ErrCodeActiveMetaNodesTooLess,
	ErrInvalidMpStart:                  ErrCodeInvalidMpStart,
	ErrNoAvailDataPartition:            ErrCodeNoAvailDataPartition,
	ErrReshuffleArray:                  ErrCodeReshuffleArray,
	ErrIllegalDataReplica:              ErrCodeIllegalDataReplica,
	ErrMissingReplica:                  ErrCodeMissingReplica,
	ErrHasOneMissingReplica:            ErrCodeHasOneMissingReplica,
	ErrNoDataNodeToWrite:               ErrCodeNoDataNodeToWrite,
	ErrNoMetaNodeToWrite:               ErrCodeNoMetaNodeToWrite,
	ErrCannotBeOffLine:                 ErrCodeCannotBeOffLine,
	ErrNoDataNodeToCreateDataPartition: ErrCodeNoDataNodeToCreateDataPartition,
	ErrNoZoneToCreateDataPartition:     ErrCodeNoZoneToCreateDataPartition,
	ErrNoZoneToCreateMetaPartition:     ErrCodeNoZoneToCreateMetaPartition,
	ErrNoNodeSetToCreateDataPartition:  ErrCodeNoNodeSetToCreateDataPartition,
	ErrNoNodeSetToCreateMetaPartition:  ErrCodeNoNodeSetToCreateMetaPartition,
	ErrNoMetaNodeToCreateMetaPartition: ErrCodeNoMetaNodeToCreateMetaPartition,
	ErrIllegalMetaReplica:              ErrCodeIllegalMetaReplica,
	ErrNoEnoughReplica:                 ErrCodeNoEnoughReplica,
	ErrNoLeader:                        ErrCodeNoLeader,
	ErrVolAuthKeyNotMatch:              ErrCodeVolAuthKeyNotMatch,
	ErrAuthKeyStoreError:               ErrCodeAuthKeyStoreError,
	ErrAuthAPIAccessGenRespError:       ErrCodeAuthAPIAccessGenRespError,
	ErrAuthOSCapsOpGenRespError:        ErrCodeAuthOSCapsOpGenRespError,
	ErrAccessKeyNotExists:              ErrCodeAccessKeyNotExists,
	ErrInvalidTicket:                   ErrCodeInvalidTicket,
	ErrExpiredTicket:                   ErrCodeExpiredTicket,
	ErrMasterAPIGenRespError:           ErrCodeMasterAPIGenRespError,
	ErrDuplicateUserID:                 ErrCodeDuplicateUserID,
	ErrUserNotExists:                   ErrCodeUserNotExists,
	ErrReadBodyError:                   ErrCodeReadBodyError,
	ErrVolPolicyNotExists:              ErrCodeVolPolicyNotExists,
	ErrDuplicateAccessKey:              ErrCodeDuplicateAccessKey,
	ErrHaveNoPolicy:                    ErrCodeHaveNoPolicy,
	ErrZoneNotExists:                   ErrCodeZoneNotExists,
	ErrOwnVolExists:                    ErrCodeOwnVolExists,
	ErrSuperAdminExists:                ErrCodeSuperAdminExists,
	ErrInvalidUserID:                   ErrCodeInvalidUserID,
	ErrInvalidUserType:                 ErrCodeInvalidUserType,
	ErrNoPermission:                    ErrCodeNoPermission,
	ErrTokenNotFound:                   ErrCodeTokenNotExist,
	ErrInvalidAccessKey:                ErrCodeInvalidAccessKey,
	ErrInvalidSecretKey:                ErrCodeInvalidSecretKey,
	ErrIsOwner:                         ErrCodeIsOwner,
	ErrZoneNum:							ErrCodeZoneNumError,
}

func ParseErrorCode(code int32) error {
	if err, exist := code2ErrMap[code]; exist {
		return err
	}
	return ErrInternalError
}

// Code2ErrMap error map to code
var code2ErrMap = map[int32]error{
	ErrCodeSuccess:                         ErrSuc,
	ErrCodeInternalError:                   ErrInternalError,
	ErrCodeParamError:                      ErrParamError,
	ErrCodeInvalidCfg:                      ErrInvalidCfg,
	ErrCodePersistenceByRaft:               ErrPersistenceByRaft,
	ErrCodeMarshalData:                     ErrMarshalData,
	ErrCodeUnmarshalData:                   ErrUnmarshalData,
	ErrCodeVolNotExists:                    ErrVolNotExists,
	ErrCodeMetaPartitionNotExists:          ErrMetaPartitionNotExists,
	ErrCodeDataPartitionNotExists:          ErrDataPartitionNotExists,
	ErrCodeDataNodeNotExists:               ErrDataNodeNotExists,
	ErrCodeMetaNodeNotExists:               ErrMetaNodeNotExists,
	ErrCodeDuplicateVol:                    ErrDuplicateVol,
	ErrCodeActiveDataNodesTooLess:          ErrActiveDataNodesTooLess,
	ErrCodeActiveMetaNodesTooLess:          ErrActiveMetaNodesTooLess,
	ErrCodeInvalidMpStart:                  ErrInvalidMpStart,
	ErrCodeNoAvailDataPartition:            ErrNoAvailDataPartition,
	ErrCodeReshuffleArray:                  ErrReshuffleArray,
	ErrCodeIllegalDataReplica:              ErrIllegalDataReplica,
	ErrCodeMissingReplica:                  ErrMissingReplica,
	ErrCodeHasOneMissingReplica:            ErrHasOneMissingReplica,
	ErrCodeNoDataNodeToWrite:               ErrNoDataNodeToWrite,
	ErrCodeNoMetaNodeToWrite:               ErrNoMetaNodeToWrite,
	ErrCodeCannotBeOffLine:                 ErrCannotBeOffLine,
	ErrCodeNoDataNodeToCreateDataPartition: ErrNoDataNodeToCreateDataPartition,
	ErrCodeNoZoneToCreateDataPartition:     ErrNoZoneToCreateDataPartition,
	ErrCodeNoZoneToCreateMetaPartition:     ErrNoZoneToCreateMetaPartition,
	ErrCodeNoNodeSetToCreateDataPartition:  ErrNoNodeSetToCreateDataPartition,
	ErrCodeNoNodeSetToCreateMetaPartition:  ErrNoNodeSetToCreateMetaPartition,
	ErrCodeNoMetaNodeToCreateMetaPartition: ErrNoMetaNodeToCreateMetaPartition,
	ErrCodeIllegalMetaReplica:              ErrIllegalMetaReplica,
	ErrCodeNoEnoughReplica:                 ErrNoEnoughReplica,
	ErrCodeNoLeader:                        ErrNoLeader,
	ErrCodeVolAuthKeyNotMatch:              ErrVolAuthKeyNotMatch,
	ErrCodeAuthKeyStoreError:               ErrAuthKeyStoreError,
	ErrCodeAuthAPIAccessGenRespError:       ErrAuthAPIAccessGenRespError,
	ErrCodeAuthOSCapsOpGenRespError:        ErrAuthOSCapsOpGenRespError,
	ErrCodeAccessKeyNotExists:              ErrAccessKeyNotExists,
	ErrCodeInvalidTicket:                   ErrInvalidTicket,
	ErrCodeExpiredTicket:                   ErrExpiredTicket,
	ErrCodeMasterAPIGenRespError:           ErrMasterAPIGenRespError,
	ErrCodeDuplicateUserID:                 ErrDuplicateUserID,
	ErrCodeUserNotExists:                   ErrUserNotExists,
	ErrCodeReadBodyError:                   ErrReadBodyError,
	ErrCodeVolPolicyNotExists:              ErrVolPolicyNotExists,
	ErrCodeDuplicateAccessKey:              ErrDuplicateAccessKey,
	ErrCodeHaveNoPolicy:                    ErrHaveNoPolicy,
	ErrCodeZoneNotExists:                   ErrZoneNotExists,
	ErrCodeOwnVolExists:                    ErrOwnVolExists,
	ErrCodeSuperAdminExists:                ErrSuperAdminExists,
	ErrCodeInvalidUserType:                 ErrInvalidUserType,
	ErrCodeInvalidUserID:                   ErrInvalidUserID,
	ErrCodeNoPermission:                    ErrNoPermission,
	ErrCodeTokenNotExist:                   ErrTokenNotFound,
	ErrCodeInvalidAccessKey:                ErrInvalidAccessKey,
	ErrCodeInvalidSecretKey:                ErrInvalidSecretKey,
	ErrCodeIsOwner:                         ErrIsOwner,
	ErrCodeZoneNumError:					ErrZoneNum,
}

type GeneralResp struct {
	Message string
	Code    int32
}

func Success(msg string) *GeneralResp {
	return &GeneralResp{Message: msg, Code: ErrCodeSuccess}
}
