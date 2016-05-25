/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import static com.github.ambry.utils.Utils.readStringFromFile;
import static com.github.ambry.utils.Utils.writeJsonToFile;


/**
 * ClusterMapManager allows components in Ambry to query the topology. This covers the {@link HardwareLayout} and the
 * {@link PartitionLayout}.
 */
public class ClusterMapManager implements ClusterMap {
  protected final HardwareLayout hardwareLayout;
  protected final PartitionLayout partitionLayout;
  private final MetricRegistry metricRegistry;
  private final ClusterMapMetrics clusterMapMetrics;

  private Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Comparator for comparing unallocated capacity in two disks
   */
  private final Comparator<Disk> diskCapacityComparator = new Comparator<Disk>() {
    @Override
    public int compare(Disk o1, Disk o2) {
      long o1Capacity = getUnallocatedRawCapacityInBytes(o1);
      long o2Capacity = getUnallocatedRawCapacityInBytes(o2);
      return (o1Capacity < o2Capacity) ? -1 : (o1Capacity == o2Capacity) ? 0 : 1;
    }
  };

  /**
   * How many data nodes to put in a random sample for partition allocation. 2 node samples provided the best balance
   * of speed and allocation quality in testing. Larger samples (ie 3 nodes) took longer to generate but did not
   * improve the quality of the allocated partitions.
   */
  private static final int NUM_CHOICES = 2;

  public ClusterMapManager(PartitionLayout partitionLayout) {
    if (logger.isTraceEnabled()) {
      logger.trace("ClusterMapManager " + partitionLayout);
    }
    this.hardwareLayout = partitionLayout.getHardwareLayout();
    this.partitionLayout = partitionLayout;
    this.metricRegistry = new MetricRegistry();
    this.clusterMapMetrics = new ClusterMapMetrics(this.hardwareLayout, this.partitionLayout, this.metricRegistry);
  }

  public ClusterMapManager(String hardwareLayoutPath, String partitionLayoutPath, ClusterMapConfig clusterMapConfig)
      throws IOException, JSONException {
    logger.trace("ClusterMapManager " + hardwareLayoutPath + ", " + partitionLayoutPath);
    this.hardwareLayout = new HardwareLayout(new JSONObject(readStringFromFile(hardwareLayoutPath)), clusterMapConfig);
    this.partitionLayout = new PartitionLayout(hardwareLayout, new JSONObject(readStringFromFile(partitionLayoutPath)));
    this.metricRegistry = new MetricRegistry();
    this.clusterMapMetrics = new ClusterMapMetrics(this.hardwareLayout, this.partitionLayout, this.metricRegistry);
  }

  public void persist(String hardwareLayoutPath, String partitionLayoutPath)
      throws IOException, JSONException {
    logger.trace("persist " + hardwareLayoutPath + ", " + partitionLayoutPath);
    writeJsonToFile(hardwareLayout.toJSONObject(), hardwareLayoutPath);
    writeJsonToFile(partitionLayout.toJSONObject(), partitionLayoutPath);
  }

  public List<PartitionId> getAllPartitions() {
    return partitionLayout.getPartitions();
  }

  // Implementation of ClusterMap interface
  // --------------------------------------

  @Override
  public List<PartitionId> getWritablePartitionIds() {
    return partitionLayout.getWritablePartitions();
  }

  @Override
  public PartitionId getPartitionIdFromStream(DataInputStream stream)
      throws IOException {
    PartitionId partitionId = partitionLayout.getPartition(stream);
    if (partitionId == null) {
      throw new IOException("Partition id from stream is null");
    }
    return partitionId;
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return hardwareLayout.findDatacenter(datacenterName) != null;
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    return hardwareLayout.findDataNode(hostname, port);
  }

  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    List<Replica> replicas = getReplicas(dataNodeId);
    return new ArrayList<ReplicaId>(replicas);
  }

  public List<Replica> getReplicas(DataNodeId dataNodeId) {
    List<Replica> replicas = new ArrayList<Replica>();
    for (PartitionId partition : partitionLayout.getPartitions()) {
      for (Replica replica : ((Partition) partition).getReplicas()) {
        if (replica.getDataNodeId().equals(dataNodeId)) {
          replicas.add(replica);
        }
      }
    }
    return replicas;
  }

  @Override
  public List<DataNodeId> getDataNodeIds() {
    List<DataNodeId> dataNodeIds = new ArrayList<DataNodeId>();
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      dataNodeIds.addAll(datacenter.getDataNodes());
    }
    return dataNodeIds;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  // Administrative API
  // -----------------------

  public long getRawCapacityInBytes() {
    return hardwareLayout.getRawCapacityInBytes();
  }

  public long getAllocatedRawCapacityInBytes() {
    return partitionLayout.getAllocatedRawCapacityInBytes();
  }

  public long getAllocatedUsableCapacityInBytes() {
    return partitionLayout.getAllocatedUsableCapacityInBytes();
  }

  public long getAllocatedRawCapacityInBytes(Datacenter datacenter) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions()) {
      for (Replica replica : ((Partition) partition).getReplicas()) {
        Disk disk = (Disk) replica.getDiskId();
        if (disk.getDataNode().getDatacenter().equals(datacenter)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  public long getAllocatedRawCapacityInBytes(DataNodeId dataNode) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions()) {
      for (Replica replica : ((Partition) partition).getReplicas()) {
        Disk disk = (Disk) replica.getDiskId();
        if (disk.getDataNode().equals(dataNode)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  public long getAllocatedRawCapacityInBytes(Disk disk) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions()) {
      for (Replica replica : ((Partition) partition).getReplicas()) {
        Disk currentDisk = (Disk) replica.getDiskId();
        if (currentDisk.equals(disk)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  public long getUnallocatedRawCapacityInBytes() {
    return getRawCapacityInBytes() - getAllocatedRawCapacityInBytes();
  }

  public long getUnallocatedRawCapacityInBytes(Datacenter datacenter) {
    return datacenter.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(datacenter);
  }

  public long getUnallocatedRawCapacityInBytes(DataNode dataNode) {
    return dataNode.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(dataNode);
  }

  public long getUnallocatedRawCapacityInBytes(Disk disk) {
    return disk.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(disk);
  }

  public DataNode getDataNodeWithMostUnallocatedRawCapacity(Datacenter dc, Set nodesToExclude) {
    DataNode maxCapacityNode = null;
    List<DataNode> dataNodes = dc.getDataNodes();
    for (DataNode dataNode : dataNodes) {
      if (!nodesToExclude.contains(dataNode) && (maxCapacityNode == null
          || getUnallocatedRawCapacityInBytes(dataNode) > getUnallocatedRawCapacityInBytes(maxCapacityNode))) {
        maxCapacityNode = dataNode;
      }
    }
    return maxCapacityNode;
  }

  public Disk getDiskWithMostUnallocatedRawCapacity(DataNode node, long minCapacity) {
    Disk maxCapacityDisk = null;
    List<Disk> disks = node.getDisks();
    for (Disk disk : disks) {
      if ((maxCapacityDisk == null || getUnallocatedRawCapacityInBytes(disk) > getUnallocatedRawCapacityInBytes(
          maxCapacityDisk)) && getUnallocatedRawCapacityInBytes(disk) >= minCapacity) {
        maxCapacityDisk = disk;
      }
    }
    return maxCapacityDisk;
  }

  public PartitionId addNewPartition(List<Disk> disks, long replicaCapacityInBytes) {
    return partitionLayout.addNewPartition(disks, replicaCapacityInBytes);
  }

  // Determine if there is enough capacity to allocate a PartitionId.
  private boolean checkEnoughUnallocatedRawCapacity(int replicaCountPerDatacenter, long replicaCapacityInBytes) {
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      if (getUnallocatedRawCapacityInBytes(datacenter) < replicaCountPerDatacenter * replicaCapacityInBytes) {
        logger.warn("Insufficient unallocated space in datacenter {} ({} bytes unallocated)", datacenter.getName(),
            getUnallocatedRawCapacityInBytes(datacenter));
        return false;
      }

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          if (getUnallocatedRawCapacityInBytes(disk) >= replicaCapacityInBytes) {
            rcpd--;
            break; // Only one replica per DataNodeId.
          }
        }
      }
      if (rcpd > 0) {
        logger.warn("Insufficient DataNodes ({}) with unallocated space in datacenter {} for {} Replicas)", rcpd,
            datacenter.getName(), replicaCountPerDatacenter);
        return false;
      }
    }

    return true;
  }

  /**
   * Get a sampling of {@code numDisks} random disks from a list of {@link DataNode}s.
   * NOTE 1: This method will change the ordering of the nodes in {@code dataNodes}
   * NOTE 2: If {@code numDisks} valid disks could not be found, the returned list could be shorter than
   *         {@code numDisks}
   *
   * @param dataNodes the list of {@link DataNode}s to sample from
   * @param dataNodesUsed the set of {@link DataNode}s to exclude from the sample
   * @param replicaCapacityInBytes the minimum amount of free space that a disk in the sample should have
   * @param rackAware if {@code true}, only return disks in nodes that do not share racks with the nodes
   *                  in {@code dataNodesUsed}
   * @param numDisks how many disks to return in the sample
   * @return a list of {@link Disk}s of length {@code numDisks}, or fewer if {@code numDisks} valid samples could not
   *         be found
   */
  private List<Disk> getRandomDiskCandidateSample(List<DataNode> dataNodes, Set<DataNode> dataNodesUsed,
      long replicaCapacityInBytes, boolean rackAware, int numDisks) {
    Set<Long> rackIdsUsed = new HashSet<>();
    if (rackAware) {
      for (DataNode dataNode : dataNodesUsed) {
        rackIdsUsed.add(dataNode.getRackId());
      }
    }
    List<Disk> diskCandidates = new ArrayList<>();
    int selectionBound = dataNodes.size();
    Random randomGen = new Random();
    while ((selectionBound > 0) && (diskCandidates.size() < numDisks)) {
      int selectionIndex = randomGen.nextInt(selectionBound);
      DataNode candidate = dataNodes.get(selectionIndex);
      if (!dataNodesUsed.contains(candidate) && !rackIdsUsed.contains(candidate.getRackId())) {
        Disk diskWithMostCapacity = getDiskWithMostUnallocatedRawCapacity(candidate, replicaCapacityInBytes);
        if (diskWithMostCapacity != null) {
          diskCandidates.add(diskWithMostCapacity);
        }
      }
      selectionBound--;
      Collections.swap(dataNodes, selectionIndex, selectionBound);
    }
    return diskCandidates;
  }

  /**
   * Return a list of disks for a new partition in the specified {@link Datacenter}.  Does not retry if fewer than
   * {@code replicaCountPerDatacenter} disks cannot be allocated.
   *
   * @param replicaCountPerDatacenter how many replicas to attempt to allocate in the datacenter
   * @param replicaCapacityInBytes the minimum amount of free space on a disk for a replica
   * @param datacenter the {@link Datacenter} to allocate replicas in
   * @param rackAware if {@code true}, attempt a rack-aware allocation
   * @return A list of {@link Disk}s
   */
  private List<Disk> allocateDisksForPartition(int replicaCountPerDatacenter, long replicaCapacityInBytes,
      Datacenter datacenter, boolean rackAware) {
    ArrayList<Disk> disksToAllocate = new ArrayList<Disk>();
    if (!datacenter.isRackAware() && rackAware) {
      throw new IllegalArgumentException(
          "Rack awareness enabled, but the datacenter: " + datacenter.getName() + " does not have rack information");
    }
    Set<DataNode> nodesToExclude = new HashSet<>();
    List<DataNode> dataNodes = new ArrayList<>(datacenter.getDataNodes());
    for (int i = 0; i < replicaCountPerDatacenter; i++) {
      List<Disk> diskCandidates =
          getRandomDiskCandidateSample(dataNodes, nodesToExclude, replicaCapacityInBytes, rackAware, NUM_CHOICES);
      if (diskCandidates.size() > 0) {
        Disk diskToAdd = Collections.max(diskCandidates, diskCapacityComparator);
        disksToAllocate.add(diskToAdd);
        nodesToExclude.add(diskToAdd.getDataNode());
      } else {
        break;
      }
    }
    return disksToAllocate;
  }

  /**
   * Return a list of disks for a new partition in the specified {@link Datacenter}. Retry a non rack-aware allocation
   * in certain cases described below if {@code retryIfNotRackAware} is enabled.
   *
   * @param replicaCountPerDatacenter how many replicas to attempt to allocate in the datacenter
   * @param replicaCapacityInBytes the minimum amount of free space on a disk for a replica
   * @param datacenter the {@link Datacenter} to allocate replicas in
   * @param retryIfNotRackAware {@code true} if we should attempt a non rack-aware allocation if a rack-aware one
   *                            is not possible.
   * @return a list of {@code replicaCountPerDatacenter} or fewer disks that can be allocated for a new partition in
   *         the specified datacenter
   */
  private List<Disk> allocateDisksForPartitionWithPotentialRetry(int replicaCountPerDatacenter,
      long replicaCapacityInBytes, Datacenter datacenter, boolean retryIfNotRackAware) {
    List<Disk> disks;
    if (datacenter.isRackAware()) {
      disks = allocateDisksForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter, true);
      if (retryIfNotRackAware && (disks.size() < replicaCountPerDatacenter)) {
        System.err.println("Rack-aware allocation failed for a partition on datacenter:" + datacenter.getName()
            + "; attempting to perform a non rack-aware allocation.");
        disks = allocateDisksForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter, false);
      }
    } else if (!retryIfNotRackAware) {
      throw new IllegalArgumentException("retryIfNotRackAware is false, but the datacenter: " + datacenter.getName()
          + " does not have rack information");
    } else {
      disks = allocateDisksForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter, false);
    }

    if (disks.size() < replicaCountPerDatacenter) {
      System.err.println(
          "Could only allocate " + disks.size() + "/" + replicaCountPerDatacenter + " replicas in datacenter: "
              + datacenter.getName());
    }
    return disks;
  }

  /**
   * Allocate partitions for {@code numPartitions} new partitions on all datacenters.
   *
   * @param numPartitions How many partitions to allocate.
   * @param replicaCountPerDatacenter The number of replicas per partition on each datacenter
   * @param replicaCapacityInBytes How large each replica (of a partition) should be
   * @param retryIfNotRackAware {@code true} if we should attempt a non rack-aware allocation if a rack-aware one
   *                            is not possible.
   * @return A list of the new {@link PartitionId}s.
   */
  public List<PartitionId> allocatePartitions(int numPartitions, int replicaCountPerDatacenter,
      long replicaCapacityInBytes, boolean retryIfNotRackAware) {
    ArrayList<PartitionId> partitions = new ArrayList<PartitionId>(numPartitions);

    while (checkEnoughUnallocatedRawCapacity(replicaCountPerDatacenter, replicaCapacityInBytes) && numPartitions > 0) {
      List<Disk> disksToAllocate = new ArrayList<>();
      for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
        List<Disk> disks =
            allocateDisksForPartitionWithPotentialRetry(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter,
                retryIfNotRackAware);
        disksToAllocate.addAll(disks);
      }
      partitions.add(partitionLayout.addNewPartition(disksToAllocate, replicaCapacityInBytes));
      numPartitions--;
    }

    return partitions;
  }

  /**
   * Add a set of replicas on a new datacenter for an existing partition.
   *
   * @param partitionId The partition to add the new datacenter to
   * @param dataCenterName The name of the new datacenter
   * @param retryIfNotRackAware {@code true} if we should attempt a non rack-aware allocation if a rack-aware one
   *                            is not possible.
   */
  public void addReplicas(PartitionId partitionId, String dataCenterName, boolean retryIfNotRackAware) {
    List<ReplicaId> replicaIds = partitionId.getReplicaIds();
    Map<String, Integer> replicaCountByDatacenter = new HashMap<String, Integer>();
    long capacityOfReplicasInBytes = 0;
    // we ensure that the datacenter provided does not have any replicas
    for (ReplicaId replicaId : replicaIds) {
      if (replicaId.getDataNodeId().getDatacenterName().compareToIgnoreCase(dataCenterName) == 0) {
        throw new IllegalArgumentException("Data center " + dataCenterName +
            " provided already contains replica for partition " + partitionId);
      }
      capacityOfReplicasInBytes = replicaId.getCapacityInBytes();
      Integer numberOfReplicas = replicaCountByDatacenter.get(replicaId.getDataNodeId().getDatacenterName());
      if (numberOfReplicas == null) {
        numberOfReplicas = new Integer(0);
      }
      numberOfReplicas++;
      replicaCountByDatacenter.put(replicaId.getDataNodeId().getDatacenterName(), numberOfReplicas);
    }
    if (replicaCountByDatacenter.size() == 0) {
      throw new IllegalArgumentException("No existing replicas present for partition " + partitionId + " in cluster.");
    }
    // verify that all data centers have the same replica
    int numberOfReplicasPerDatacenter = 0;
    int index = 0;
    for (Map.Entry<String, Integer> entry : replicaCountByDatacenter.entrySet()) {
      if (index == 0) {
        numberOfReplicasPerDatacenter = entry.getValue();
      }
      if (numberOfReplicasPerDatacenter != entry.getValue()) {
        throw new IllegalStateException("Datacenters have different replicas for partition " + partitionId);
      }
      index++;
    }
    Datacenter datacenterToAdd = hardwareLayout.findDatacenter(dataCenterName);
    List<Disk> disksForReplicas =
        allocateDisksForPartitionWithPotentialRetry(numberOfReplicasPerDatacenter, capacityOfReplicasInBytes,
            datacenterToAdd, retryIfNotRackAware);
    partitionLayout.addNewReplicas((Partition) partitionId, disksForReplicas);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ClusterMapManager that = (ClusterMapManager) o;

    if (hardwareLayout != null ? !hardwareLayout.equals(that.hardwareLayout) : that.hardwareLayout != null) {
      return false;
    }
    return !(partitionLayout != null ? !partitionLayout.equals(that.partitionLayout) : that.partitionLayout != null);
  }

  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    switch (event) {
      case Disk_Error:
        ((Disk) replicaId.getDiskId()).onDiskError();
        break;
      case Disk_Ok:
        ((Disk) replicaId.getDiskId()).onDiskOk();
        break;
      case Node_Timeout:
        ((DataNode) replicaId.getDataNodeId()).onNodeTimeout();
        break;
      case Node_Response:
        ((DataNode) replicaId.getDataNodeId()).onNodeResponse();
        break;
      case Partition_ReadOnly:
        ((Partition) replicaId.getPartitionId()).onPartitionReadOnly();
        break;
    }
  }
}
