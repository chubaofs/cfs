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

package cmd

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdVolUse   = "volume [COMMAND]"
	cmdVolShort = "Manage cluster volumes"
)

func newVolCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdVolUse,
		Short:   cmdVolShort,
		Args:    cobra.MinimumNArgs(0),
		Aliases: []string{"vol"},
	}
	cmd.AddCommand(
		newVolListCmd(client),
		newVolCreateCmd(client),
		newVolExpandCmd(client),
		newVolShrinkCmd(client),
		newVolSetCmd(client),
		newVolInfoCmd(client),
		newVolDeleteCmd(client),
		newVolTransferCmd(client),
		newVolAddDPCmd(client),
	)
	return cmd
}

const (
	cmdVolListShort = "List cluster volumes"
)

func newVolListCmd(client *master.MasterClient) *cobra.Command {
	var optKeyword string
	var cmd = &cobra.Command{
		Use:     CliOpList,
		Short:   cmdVolListShort,
		Aliases: []string{"ls"},
		Run: func(cmd *cobra.Command, args []string) {
			var vols []*proto.VolInfo
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if vols, err = client.AdminAPI().ListVols(optKeyword); err != nil {
				return
			}
			stdout("%v\n", volumeInfoTableHeader)
			for _, vol := range vols {
				stdout("%v\n", formatVolInfoTableRow(vol))
			}
		},
	}
	cmd.Flags().StringVar(&optKeyword, "keyword", "", "Specify keyword of volume name to filter")
	return cmd
}

const (
	cmdVolCreateUse             = "create [VOLUME NAME] [USER ID]"
	cmdVolCreateShort           = "Create a new volume"
	cmdVolDefaultMPCount        = 3
	cmdVolDefaultDPSize         = 120
	cmdVolDefaultCapacity       = 10 // 100GB
	cmdVolDefaultReplicas       = 3
	cmdVolDefaultFollowerReader = true
	cmdVolDefaultZoneName = ""
	cmdVolDefaultCrossZone = false
)

func newVolCreateCmd(client *master.MasterClient) *cobra.Command {
	var optMPCount int
	var optDPSize uint64
	var optCapacity uint64
	var optReplicas int
	var optFollowerRead bool
	var optYes bool
	var optCrossZone bool
	var optZoneName string
	var cmd = &cobra.Command{
		Use:   cmdVolCreateUse,
		Short: cmdVolCreateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var userID = args[1]
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			// ask user for confirm
			if !optYes {
				stdout("Create a new volume:\n")
				stdout("  Name                : %v\n", volumeName)
				stdout("  Owner               : %v\n", userID)
				stdout("  Dara partition size : %v GB\n", optDPSize)
				stdout("  Meta partition count: %v\n", optMPCount)
				stdout("  Capacity            : %v GB\n", optCapacity)
				stdout("  Replicas            : %v\n", optReplicas)
				stdout("  Allow follower read : %v\n", formatEnabledDisabled(optFollowerRead))
				stdout("  ZoneName            : %v\n", optZoneName)
				stdout("  CrossZone            : %v\n", optCrossZone)
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}

			err = client.AdminAPI().CreateVolume(
				volumeName, userID, optMPCount, optDPSize,
				optCapacity, optReplicas, optFollowerRead, optZoneName, optCrossZone)
			if err != nil {
				err = fmt.Errorf("Create volume failed case:\n%v\n", err)
				return
			}
			stdout("Create volume success.\n")
			return
		},
	}
	cmd.Flags().IntVar(&optMPCount, CliFlagMetaPartitionCount, cmdVolDefaultMPCount, "Specify init meta partition count")
	cmd.Flags().Uint64Var(&optDPSize, CliFlagDataPartitionSize, cmdVolDefaultDPSize, "Specify size of data partition size [Unit: GB]")
	cmd.Flags().Uint64Var(&optCapacity, CliFlagCapacity, cmdVolDefaultCapacity, "Specify volume capacity [Unit: GB]")
	cmd.Flags().IntVar(&optReplicas, CliFlagReplicas, cmdVolDefaultReplicas, "Specify data partition replicas number")
	cmd.Flags().BoolVar(&optFollowerRead, CliFlagEnableFollowerRead, cmdVolDefaultFollowerReader, "Enable read form replica follower")
	cmd.Flags().StringVar(&optZoneName, CliFlagZoneName, cmdVolDefaultZoneName, "Specify volume zone name")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().BoolVar(&optCrossZone, CliFlagCrossZone, cmdVolDefaultCrossZone, "Disable cross zone")

	return cmd
}

const (
	cmdVolSetShort = "Set configuration of the volume"
)

func newVolSetCmd(client *master.MasterClient) *cobra.Command {
	var optCapacity uint64
	var optReplicas int
	var optFollowerRead string
	var optAuthenticate string
	var optEnableToken string
	var optZoneName string
	var optYes bool
	var confirmString = strings.Builder{}
	var vv *proto.SimpleVolView
	var cmd = &cobra.Command{
		Use:   CliOpSet + " [VOLUME NAME]",
		Short: cmdVolSetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var isChange = false
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if vv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				return
			}
			confirmString.WriteString("Volume configuration changes:\n")
			confirmString.WriteString(fmt.Sprintf("  Name                : %v\n", vv.Name))
			if optCapacity > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  Capacity            : %v GB -> %v GB\n", vv.Capacity, optCapacity))
				vv.Capacity = optCapacity
			} else {
				confirmString.WriteString(fmt.Sprintf("  Capacity            : %v GB\n", vv.Capacity))
			}
			if optReplicas > 0 {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  Replicas            : %v -> %v\n", vv.DpReplicaNum, optReplicas))
				vv.DpReplicaNum = uint8(optReplicas)
			} else {
				confirmString.WriteString(fmt.Sprintf("  Replicas            : %v\n", vv.DpReplicaNum))
			}
			if optFollowerRead != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optFollowerRead); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  Allow follower read : %v -> %v\n", formatEnabledDisabled(vv.FollowerRead), formatEnabledDisabled(enable)))
				vv.FollowerRead = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  Allow follower read : %v\n", formatEnabledDisabled(vv.FollowerRead)))
			}

			if optAuthenticate != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optAuthenticate); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  Authenticate        : %v -> %v\n", formatEnabledDisabled(vv.Authenticate), formatEnabledDisabled(enable)))
				vv.Authenticate = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  Authenticate        : %v\n", formatEnabledDisabled(vv.Authenticate)))
			}
			if optEnableToken != "" {
				isChange = true
				var enable bool
				if enable, err = strconv.ParseBool(optEnableToken); err != nil {
					return
				}
				confirmString.WriteString(fmt.Sprintf("  EnableToken         : %v -> %v\n", formatEnabledDisabled(vv.EnableToken), formatEnabledDisabled(enable)))
				vv.EnableToken = enable
			} else {
				confirmString.WriteString(fmt.Sprintf("  EnableToken         : %v\n", formatEnabledDisabled(vv.EnableToken)))
			}
			if vv.CrossZone == false && "" != optZoneName {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  ZoneName            : %v -> %v\n", vv.ZoneName, optZoneName))
				vv.ZoneName = optZoneName
			} else {
				confirmString.WriteString(fmt.Sprintf("  ZoneName            : %v\n", vv.ZoneName))
			}
			if vv.CrossZone == true && "" != optZoneName {
				err = fmt.Errorf("Can not set zone name of the volume that cross zone\n")
			}
			if err != nil {
				return
			}
			if !isChange {
				stdout("No changes has been set.\n")
				return
			}
			// ask user for confirm
			if !optYes {
				stdout(confirmString.String())
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}
			err = client.AdminAPI().UpdateVolume(vv.Name, vv.Capacity, int(vv.DpReplicaNum),
				vv.FollowerRead, vv.Authenticate, vv.EnableToken, calcAuthKey(vv.Owner), vv.ZoneName)
			if err != nil {
				return
			}
			stdout("Volume configuration has been set successfully.\n")
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().Uint64Var(&optCapacity, CliFlagCapacity, 0, "Specify volume capacity [Unit: GB]")
	cmd.Flags().IntVar(&optReplicas, CliFlagReplicas, 0, "Specify data partition replicas number")
	cmd.Flags().StringVar(&optFollowerRead, CliFlagEnableFollowerRead, "", "Enable read form replica follower")
	cmd.Flags().StringVar(&optAuthenticate, CliFlagAuthenticate, "", "Enable authenticate")
	cmd.Flags().StringVar(&optZoneName, CliFlagZoneName, "", "Specify volume zone name")
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

const (
	cmdVolInfoUse   = "info [VOLUME NAME]"
	cmdVolInfoShort = "Show volume information"
)

func newVolInfoCmd(client *master.MasterClient) *cobra.Command {
	var (
		optMetaDetail bool
		optDataDetail bool
	)

	var cmd = &cobra.Command{
		Use:   cmdVolInfoUse,
		Short: cmdVolInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			var svv *proto.SimpleVolView
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			if svv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				err = fmt.Errorf("Get volume info failed:\n%v\n", err)
				return
			}
			// print summary info
			stdout("Summary:\n%s\n", formatSimpleVolView(svv))

			// print metadata detail
			if optMetaDetail {
				var views []*proto.MetaPartitionView
				if views, err = client.ClientAPI().GetMetaPartitions(volumeName); err != nil {
					err = fmt.Errorf("Get volume metadata detail information failed:\n%v\n", err)
					return
				}
				stdout("Meta partitions:\n")
				stdout("%v\n", metaPartitionTableHeader)
				sort.SliceStable(views, func(i, j int) bool {
					return views[i].PartitionID < views[j].PartitionID
				})
				for _, view := range views {
					stdout("%v\n", formatMetaPartitionTableRow(view))
				}
			}

			// print data detail
			if optDataDetail {
				var view *proto.DataPartitionsView
				if view, err = client.ClientAPI().GetDataPartitions(volumeName); err != nil {
					err = fmt.Errorf("Get volume data detail information failed:\n%v\n", err)
					return
				}
				stdout("Data partitions:\n")
				stdout("%v\n", dataPartitionTableHeader)
				sort.SliceStable(view.DataPartitions, func(i, j int) bool {
					return view.DataPartitions[i].PartitionID < view.DataPartitions[j].PartitionID
				})
				for _, dp := range view.DataPartitions {
					stdout("%v\n", formatDataPartitionTableRow(dp))
				}
			}
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVarP(&optMetaDetail, "meta-partition", "m", false, "Display meta partition detail information")
	cmd.Flags().BoolVarP(&optDataDetail, "data-partition", "d", false, "Display data partition detail information")
	return cmd
}

const (
	cmdVolDeleteUse   = "delete [VOLUME NAME]"
	cmdVolDeleteShort = "Delete a volume from cluster"
)

func newVolDeleteCmd(client *master.MasterClient) *cobra.Command {
	var (
		optYes bool
	)
	var cmd = &cobra.Command{
		Use:   cmdVolDeleteUse,
		Short: cmdVolDeleteShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volumeName = args[0]
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			// ask user for confirm
			if !optYes {
				stdout("Delete volume [%v] (yes/no)[no]:", volumeName)
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}

			var svv *proto.SimpleVolView
			if svv, err = client.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
				err = fmt.Errorf("Delete volume failed:\n%v\n", err)
				return
			}

			if err = client.AdminAPI().DeleteVolume(volumeName, calcAuthKey(svv.Owner)); err != nil {
				err = fmt.Errorf("Delete volume failed:\n%v\n", err)
				return
			}
			stdout("Delete volume success.\n")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}

const (
	cmdVolTransferUse   = "transfer [VOLUME NAME] [USER ID]"
	cmdVolTransferShort = "Transfer volume to another user. (Change owner of volume)"
)

func newVolTransferCmd(client *master.MasterClient) *cobra.Command {
	var optYes bool
	var optForce bool
	var cmd = &cobra.Command{
		Use:     cmdVolTransferUse,
		Short:   cmdVolTransferShort,
		Aliases: []string{"trans"},
		Args:    cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volume = args[0]
			var userID = args[1]

			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()

			// ask user for confirm
			if !optYes {
				stdout("Transfer volume [%v] to user [%v] (yes/no)[no]:", volume, userID)
				var confirm string
				_, _ = fmt.Scanln(&confirm)
				if confirm != "yes" {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}

			// check target user and volume
			var volSimpleView *proto.SimpleVolView
			if volSimpleView, err = client.AdminAPI().GetVolumeSimpleInfo(volume); err != nil {
				return
			}
			if volSimpleView.Status != 0 {
				err = fmt.Errorf("volume status abnormal")
				return
			}
			var userInfo *proto.UserInfo
			if userInfo, err = client.UserAPI().GetUserInfo(userID); err != nil {
				return
			}
			var param = proto.UserTransferVolParam{
				Volume:  volume,
				UserSrc: volSimpleView.Owner,
				UserDst: userInfo.UserID,
				Force:   optForce,
			}
			if _, err = client.UserAPI().TransferVol(&param); err != nil {
				return
			}
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	cmd.Flags().BoolVarP(&optForce, "force", "f", false, "Force transfer without current owner check")
	return cmd
}

const (
	cmdVolAddDPCmdUse   = "add-dp [VOLUME] [NUMBER]"
	cmdVolAddDPCmdShort = "Create and add more data partition to a volume"
)

func newVolAddDPCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdVolAddDPCmdUse,
		Short: cmdVolAddDPCmdShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var volume = args[0]
			var number = args[1]
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var count int64
			if count, err = strconv.ParseInt(number, 10, 64); err != nil {
				return
			}
			if count < 1 {
				err = fmt.Errorf("number must be larger than 0")
				return
			}
			if err = client.AdminAPI().CreateDataPartition(volume, int(count)); err != nil {
				return
			}
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

const (
	cmdExpandVolCmdShort = "Expand capacity of a volume"
	cmdShrinkVolCmdShort = "Shrink capacity of a volume"
)

func newVolExpandCmd(client *master.MasterClient) *cobra.Command {
	volClient := NewVolumeClient(OpExpandVol, client)
	return newVolSetCapacityCmd(CliOpExpand, cmdExpandVolCmdShort, volClient)
}

func newVolShrinkCmd(client *master.MasterClient) *cobra.Command {
	volClient := NewVolumeClient(OpShrinkVol, client)
	return newVolSetCapacityCmd(CliOpShrink, cmdShrinkVolCmdShort, volClient)
}

func newVolSetCapacityCmd(use, short string, r clientHandler) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   use + " [VOLUME] [CAPACITY]",
		Short: short,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var name = args[0]
			var capacityStr = args[1]
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			volume := r.(*volumeClient)
			if volume.capacity, err = strconv.ParseUint(capacityStr, 10, 64); err != nil {
				return
			}
			volume.name = name
			if err = volume.excuteHttp(); err != nil {
				return
			}
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			volume := r.(*volumeClient)
			return validVols(volume.client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}
