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
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	cmdDataPartitionUse   = "datapartition [COMMAND]"
	cmdDataPartitionShort = "Manage data partition"
)

func newDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdDataPartitionUse,
		Short: cmdDataPartitionShort,
	}
	cmd.AddCommand(
		newDataPartitionGetCmd(client),
		newListCorruptDataPartitionCmd(client),
		newDataPartitionDecommissionCmd(client),
		newDataPartitionReplicateCmd(client),
		newDataPartitionDeleteReplicaCmd(client),
	)
	return cmd
}

const (
	cmdDataPartitionGetShort           = "Display detail information of a data partition"
	cmdCheckCorruptDataPartitionShort  = "Check out corrupt data partitions"
	cmdDataPartitionDecommissionShort  = "Decommission a replication of the data partition to a new address"
	cmdDataPartitionReplicateShort     = "Add a replication of the data partition on a new address"
	cmdDataPartitionDeleteReplicaShort = "Delete a replication of the data partition on a fixed address"
)

func newDataPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [DATA PARTITION ID]",
		Short: cmdDataPartitionGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.DataPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if partition, err = client.AdminAPI().GetDataPartition("", partitionID); err != nil {
				return
			}
			stdout(formatDataPartitionInfo(partition))
		},
	}
	return cmd
}

func newListCorruptDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var optEnableAutoFullfill bool
	var optCheckAll bool
	var cmd = &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckCorruptDataPartitionShort,
		Long: `If the data nodes are marked as "Inactive", it means the nodes has been not available for a time. It is suggested to 
eliminate the network, disk or other problems first. Once the bad nodes can never be "active", they are called corrupt 
nodes. The "decommission" command can be used to discard the corrupt nodes. However, if more than half replicas of
a partition are on the corrupt nodes, the few remaining replicas can not reach an agreement with one leader. In this case, 
you can use the "reset" command to fix the problem.The "reset" command may lead to data loss, be careful to do this.
The "reset" command will be released in next version`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				diagnosis *proto.DataPartitionDiagnosis
				dataNodes []*proto.DataNodeInfo
				err       error
			)
			if optCheckAll {
				err = checkAllDataPartitions(client)
				if err != nil {
					stdout("%v\n", err)
				}
				return
			}
			if diagnosis, err = client.AdminAPI().DiagnoseDataPartition(); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("[Inactive Data nodes]:\n")
			stdout("%v\n", formatDataNodeDetailTableHeader())
			for _, addr := range diagnosis.InactiveDataNodes {
				var node *proto.DataNodeInfo
				node, err = client.NodeAPI().GetDataNode(addr)
				dataNodes = append(dataNodes, node)
			}
			sort.SliceStable(dataNodes, func(i, j int) bool {
				return dataNodes[i].ID < dataNodes[j].ID
			})
			for _, node := range dataNodes {
				stdout("%v\n", formatDataNodeDetail(node, true))
			}
			/*stdout("\n")
			stdout("[Corrupt data partitions](no leader):\n")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.CorruptDataPartitionIDs, func(i, j int) bool {
				return diagnosis.CorruptDataPartitionIDs[i] < diagnosis.CorruptDataPartitionIDs[j]
			})
			for _, pid := range diagnosis.CorruptDataPartitionIDs {
				var partition *proto.DataPartitionInfo
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
					stdout("Partition not found, err:[%v]", err)
					return
				}
				stdout("%v\n", formatDataPartitionInfoRow(partition))
			}*/

			stdout("\n")
			stdout("%v\n", "[Partition lack replicas]:")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.LackReplicaDataPartitionIDs, func(i, j int) bool {
				return diagnosis.LackReplicaDataPartitionIDs[i] < diagnosis.LackReplicaDataPartitionIDs[j]
			})
			cv, _ := client.AdminAPI().GetCluster()
			dns := cv.DataNodes
			var sb = strings.Builder{}

			for _, pid := range diagnosis.LackReplicaDataPartitionIDs {
				var partition *proto.DataPartitionInfo
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
					stdout("Partition is not found, err:[%v]", err)
					return
				}
				if partition != nil {
					stdout("%v\n", formatDataPartitionInfoRow(partition))
					sort.Strings(partition.Hosts)
					if len(partition.MissingNodes) > 0 || partition.Status == -1 {
						stdoutRed(fmt.Sprintf("partition not ready to repair"))
						continue
					}
					var leaderRps map[uint64]*proto.ReplicaStatus
					var canAutoRepair bool
					var peerStrings []string
					canAutoRepair = true
					for i, r := range partition.Replicas {
						var rps map[uint64]*proto.ReplicaStatus
						var dnPartition *proto.DNDataPartitionInfo
						var err error
						addr := strings.Split(r.Addr, ":")[0]
						if dnPartition, err = client.NodeAPI().DataNodeGetPartition(addr, partition.PartitionID); err != nil {
							fmt.Printf(partitionInfoColorTablePattern+"\n",
								"", "", "", fmt.Sprintf("%v(hosts)", r.Addr), fmt.Sprintf("%v/%v", "nil", partition.ReplicaNum), "get partition info failed")
							continue
						}
						sort.Strings(dnPartition.Replicas)
						fmt.Printf(partitionInfoColorTablePattern+"\n",
							"", "", "", fmt.Sprintf("%v(hosts)", r.Addr), fmt.Sprintf("%v/%v", len(dnPartition.Replicas), partition.ReplicaNum), strings.Join(dnPartition.Replicas, "; "))

						if rps = dnPartition.RaftStatus.Replicas; rps != nil {
							leaderRps = rps
						}
						peers := convertPeersToArray(dnPartition.Peers)
						sort.Strings(peers)
						if i == 0 {
							peerStrings = peers
						} else {
							if !isEqualStrings(peers, peerStrings) {
								canAutoRepair = false
							}
						}
						fmt.Printf(partitionInfoColorTablePattern+"\n",
							"", "", "", fmt.Sprintf("%v(peers)", r.Addr), fmt.Sprintf("%v/%v", len(peers), partition.ReplicaNum), strings.Join(peers, "; "))
					}
					if len(leaderRps) != 3 || len(partition.Hosts) != 2 {
						stdoutRed(fmt.Sprintf("raft peer number(expected is 3, but is %v) or replica number(expected is 2, but is %v) not match ", len(leaderRps), len(partition.Hosts)))
						continue
					}
					var lackAddr []string
					for _, dn := range dns {
						if _, ok := leaderRps[dn.ID]; ok {
							if !contains(partition.Hosts, dn.Addr) {
								lackAddr = append(lackAddr, dn.Addr)
							}
						}
					}
					if len(lackAddr) != 1 {
						stdoutRed(fmt.Sprintf("Not classic partition, please check and repair it manually"))
						continue
					}
					stdoutGreen(fmt.Sprintf(" The Lack Address is: %v", lackAddr))
					if canAutoRepair {
						sb.WriteString(fmt.Sprintf("cfs-cli datapartition add-replica %v %v\n", lackAddr[0], partition.PartitionID))
					}
					if optEnableAutoFullfill && canAutoRepair {
						stdoutGreen("     Auto Repair Begin:")
						if err = client.AdminAPI().AddDataReplica(partition.PartitionID, lackAddr[0]); err != nil {
							stdoutRed(fmt.Sprintf("%v err:%v", "     Failed.", err))
							continue
						}
						stdoutGreen("     Done.")
						time.Sleep(2 * time.Second)
					}
					stdoutGreen(strings.Repeat("_ ", len(partitionInfoTableHeader)/2+20) + "\n")
				}
			}
			if !optEnableAutoFullfill {
				stdout(sb.String())
			}
			return
		},
	}
	cmd.Flags().BoolVar(&optEnableAutoFullfill, CliFlagEnableAutoFill, false, "true - automatically full fill the missing replica")
	cmd.Flags().BoolVar(&optCheckAll, "all", false, "true - check all partitions; false - only check partitions which lack of replica")
	return cmd
}
func checkAllDataPartitions(client *master.MasterClient) (err error) {
	var volInfo []*proto.VolInfo
	if volInfo, err = client.AdminAPI().ListVols(""); err != nil {
		stdout("%v\n", err)
		return
	}
	stdout("\n")
	stdout("%v\n", "[Partition peer info not valid]:")
	stdout("%v\n", partitionInfoTableHeader)
	for _, vol := range volInfo {
		var volView *proto.VolView
		if volView, err = client.ClientAPI().GetVolume(vol.Name, calcAuthKey(vol.Owner)); err != nil {
			stdout("Found an invalid vol: %v\n", vol.Name)
			continue
		}
		sort.SliceStable(volView.DataPartitions, func(i, j int) bool {
			return volView.DataPartitions[i].PartitionID < volView.DataPartitions[j].PartitionID
		})
		var wg sync.WaitGroup
		for _, dp := range volView.DataPartitions {
			wg.Add(1)
			go func(dp *proto.DataPartitionResponse) {
				defer wg.Done()
				var outPut string
				var isHealthy bool
				outPut, isHealthy, _ = checkDataPartition(dp.PartitionID, client)
				if !isHealthy {
					fmt.Printf(outPut)
					stdoutGreen(strings.Repeat("_ ", len(partitionInfoTableHeader)/2+20) + "\n")
				}
			}(dp)
		}
		wg.Wait()
	}
	return
}
func checkDataPartition(pid uint64, client *master.MasterClient) (outPut string, isHealthy bool, err error) {
	var partition *proto.DataPartitionInfo
	var sb = strings.Builder{}
	isHealthy = true
	if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
		sb.WriteString(fmt.Sprintf("Partition is not found, err:[%v]", err))
		return
	}
	if partition != nil {
		sb.WriteString(fmt.Sprintf("%v\n", formatDataPartitionInfoRow(partition)))
		sort.Strings(partition.Hosts)
		if len(partition.MissingNodes) > 0 || partition.Status == -1 || len(partition.Hosts) != int(partition.ReplicaNum) {
			errMsg := fmt.Sprintf("The partition is not healthy according to the report message from master")
			sb.WriteString(fmt.Sprintf("\033[1;40;31m%-8v\033[0m\n", errMsg))
			isHealthy = false
		}
		var leaderRps map[uint64]*proto.ReplicaStatus
		for _, r := range partition.Replicas {
			var rps map[uint64]*proto.ReplicaStatus
			var dnPartition *proto.DNDataPartitionInfo
			var err error
			addr := strings.Split(r.Addr, ":")[0]
			if dnPartition, err = client.NodeAPI().DataNodeGetPartition(addr, partition.PartitionID); err != nil {
				sb.WriteString(fmt.Sprintf(partitionInfoColorTablePattern+"\n",
					"", "", "", fmt.Sprintf("%v", r.Addr), fmt.Sprintf("%v/%v", "nil", partition.ReplicaNum), fmt.Sprintf("get partition info failed, err:%v", err)))
				isHealthy = false
				continue
			}
			sort.Strings(dnPartition.Replicas)
			sb.WriteString(fmt.Sprintf(partitionInfoColorTablePattern+"\n",
				"", "", "", fmt.Sprintf("%v(hosts)", r.Addr), fmt.Sprintf("%v/%v", len(dnPartition.Replicas), partition.ReplicaNum), strings.Join(dnPartition.Replicas, "; ")))

			if rps = dnPartition.RaftStatus.Replicas; rps != nil {
				leaderRps = rps
			}
			peerStrings := convertPeersToArray(dnPartition.Peers)
			sort.Strings(peerStrings)
			sb.WriteString(fmt.Sprintf(partitionInfoColorTablePattern+"\n",
				"", "", "", fmt.Sprintf("%v(peers)", r.Addr), fmt.Sprintf("%v/%v", len(peerStrings), partition.ReplicaNum), strings.Join(peerStrings, "; ")))
			if !isEqualStrings(peerStrings, dnPartition.Replicas) {
				isHealthy = false
			}
			if !isEqualStrings(partition.Hosts, peerStrings) {
				isHealthy = false
			}
			if len(peerStrings) != int(partition.ReplicaNum) || len(dnPartition.Replicas) != int(partition.ReplicaNum) {
				isHealthy = false
			}
		}
		if len(leaderRps) == 0 {
			isHealthy = false
			errMsg := fmt.Sprintf("no raft leader")
			sb.WriteString(fmt.Sprintf("\033[1;40;31m%-8v\033[0m\n", errMsg))
		}
	}
	outPut = sb.String()
	return
}
func newDataPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionDecommissionShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().DecommissionDataPartition(partitionID, address); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newDataPartitionReplicateCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpReplicate + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionReplicateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().AddDataReplica(partitionID, address); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newDataPartitionDeleteReplicaCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDelReplica + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionDeleteReplicaShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().DeleteDataReplica(partitionID, address); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}
