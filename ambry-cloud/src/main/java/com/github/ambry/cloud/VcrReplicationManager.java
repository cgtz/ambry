/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.clustermap.CloudReplica;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.VcrClusterParticipant;
import com.github.ambry.clustermap.VcrClusterParticipantListener;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.replication.PartitionInfo;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * {@link VcrReplicationManager} is used to backup partitions to Cloud. Partitions assignment is handled by Helix.
 */
public class VcrReplicationManager extends ReplicationEngine {
  private final CloudConfig cloudConfig;
  private final StoreConfig storeConfig;
  private final VcrMetrics vcrMetrics;
  private final VcrClusterParticipant vcrClusterParticipant;
  private final CloudStorageCompactor cloudStorageCompactor;
  private final CloudContainerCompactor cloudContainerCompactor;
  private final Map<String, Store> partitionStoreMap = new HashMap<>();
  private final boolean trackPerDatacenterLagInMetric;

  public VcrReplicationManager(CloudConfig cloudConfig, ReplicationConfig replicationConfig,
      ClusterMapConfig clusterMapConfig, StoreConfig storeConfig, StoreManager storeManager,
      StoreKeyFactory storeKeyFactory, ClusterMap clusterMap, VcrClusterParticipant vcrClusterParticipant,
      CloudDestination cloudDestination, ScheduledExecutorService scheduler, ConnectionPool connectionPool,
      VcrMetrics vcrMetrics, NotificationSystem requestNotification, StoreKeyConverterFactory storeKeyConverterFactory,
      String transformerClassName) throws ReplicationException, IllegalStateException {
    super(replicationConfig, clusterMapConfig, storeKeyFactory, clusterMap, scheduler,
        vcrClusterParticipant.getCurrentDataNodeId(), Collections.emptyList(), connectionPool,
        vcrMetrics.getMetricRegistry(), requestNotification, storeKeyConverterFactory, transformerClassName, null,
        storeManager, null);
    this.cloudConfig = cloudConfig;
    this.storeConfig = storeConfig;
    this.vcrClusterParticipant = vcrClusterParticipant;
    this.vcrMetrics = vcrMetrics;
    this.persistor =
        new CloudTokenPersistor(replicaTokenFileName, mountPathToPartitionInfos, replicationMetrics, clusterMap,
            tokenHelper, cloudDestination);
    this.cloudStorageCompactor =
        cloudConfig.cloudBlobCompactionEnabled ? new CloudStorageCompactor(cloudDestination, cloudConfig,
            partitionToPartitionInfo.keySet(), vcrMetrics) : null;
    this.cloudContainerCompactor = cloudDestination.getContainerCompactor();
    trackPerDatacenterLagInMetric = replicationConfig.replicationTrackPerDatacenterLagFromLocal;
    // We need a datacenter to replicate from, which should be specified in the cloud config.
    if (cloudConfig.vcrSourceDatacenters.isEmpty()) {
      throw new IllegalStateException("One or more VCR cross colo replication peer datacenter should be specified");
    }
  }

  @Override
  public void start() throws ReplicationException {
    // Add listener for new coming assigned partition
    vcrClusterParticipant.addListener(new VcrClusterParticipantListener() {
      @Override
      public void onPartitionAdded(PartitionId partitionId) {
        try {
          addReplica(partitionId);
          logger.info("Partition {} added to {}", partitionId, dataNodeId);
        } catch (ReplicationException e) {
          vcrMetrics.addPartitionErrorCount.inc();
          logger.error("Exception on adding Partition {} to {}: ", partitionId, dataNodeId, e);
        } catch (Exception e) {
          // Helix will run into error state if exception throws in Helix context.
          vcrMetrics.addPartitionErrorCount.inc();
          logger.error("Unknown Exception on adding Partition {} to {}: ", partitionId, dataNodeId, e);
        }
      }

      @Override
      public void onPartitionRemoved(PartitionId partitionId) {
        try {
          removeReplica(partitionId);
        } catch (Exception e) {
          // Helix will run into error state if exception throws in Helix context.
          vcrMetrics.removePartitionErrorCount.inc();
          logger.error("Exception on removing Partition {} from {}: ", partitionId, dataNodeId, e);
        }
      }
    });

    try {
      vcrClusterParticipant.participate();
    } catch (Exception e) {
      throw new ReplicationException("Cluster participate failed.", e);
    }

    // start background persistent thread
    // start scheduler thread to persist index in the background
    scheduleTask(persistor, true, replicationConfig.replicationTokenFlushDelaySeconds,
        replicationConfig.replicationTokenFlushIntervalSeconds, "replica token persistor");

    // Schedule thread to purge dead blobs for this VCR's partitions
    // after delay to allow startup to finish.
    scheduleTask(cloudStorageCompactor, cloudConfig.cloudBlobCompactionEnabled,
        cloudConfig.cloudBlobCompactionStartupDelaySecs,
        TimeUnit.HOURS.toSeconds(cloudConfig.cloudBlobCompactionIntervalHours), "cloud blob compaction");

    // Schedule thread to purge blobs belonging to deprecated containers for this VCR's partitions
    // after delay to allow startup to finish.
    scheduleTask(() -> cloudContainerCompactor.compactAssignedDeprecatedContainers(
        vcrClusterParticipant.getAssignedPartitionIds()), cloudConfig.cloudContainerCompactionEnabled,
        cloudConfig.cloudContainerCompactionStartupDelaySecs,
        TimeUnit.HOURS.toSeconds(cloudConfig.cloudContainerCompactionIntervalHours), "cloud container compaction");
  }

  /**
   * Schedule the specified task if enabled with the specified delay and interval.
   * @param task {@link Runnable} task to be scheduled.
   * @param isEnabled flag indicating if the task is enabled. If false the task is not scheduled.
   * @param delaySec initial delay to allow startup to finish before starting task.
   * @param intervalSec period between successive executions.
   * @param taskName name of the task being scheduled.
   */
  private void scheduleTask(Runnable task, boolean isEnabled, long delaySec, long intervalSec, String taskName) {
    if (isEnabled) {
      scheduler.scheduleAtFixedRate(task, delaySec, intervalSec, TimeUnit.SECONDS);
      logger.info("Scheduled {} task to run every {} seconds starting in {} seconds.", taskName, intervalSec, delaySec);
    } else {
      logger.warn("Running with {} turned off!", taskName);
    }
  }

  /**
   * Add a replica of given {@link PartitionId} and its {@link RemoteReplicaInfo}s to backup list.
   * @param partitionId the {@link PartitionId} of the replica to add.
   * @throws ReplicationException if replicas initialization failed.
   */
  void addReplica(PartitionId partitionId) throws ReplicationException {
    if (partitionToPartitionInfo.containsKey(partitionId)) {
      throw new ReplicationException("Partition " + partitionId + " already exists on " + dataNodeId);
    }
    ReplicaId cloudReplica = new CloudReplica(partitionId, vcrClusterParticipant.getCurrentDataNodeId());
    if (!storeManager.addBlobStore(cloudReplica)) {
      logger.error("Can't start cloudstore for replica {}", cloudReplica);
      throw new ReplicationException("Can't start cloudstore for replica " + cloudReplica);
    }
    List<? extends ReplicaId> peerReplicas = cloudReplica.getPeerReplicaIds();
    List<RemoteReplicaInfo> remoteReplicaInfos = new ArrayList<>();
    Store store = storeManager.getStore(partitionId);
    if (peerReplicas != null) {
      for (ReplicaId peerReplica : peerReplicas) {
        if (!shouldReplicateFromDc(peerReplica.getDataNodeId().getDatacenterName())) {
          continue;
        }
        // We need to ensure that a replica token gets persisted only after the corresponding data in the
        // store gets flushed to cloud. We use the store flush interval multiplied by a constant factor
        // to determine the token flush interval
        FindTokenFactory findTokenFactory =
            tokenHelper.getFindTokenFactoryFromReplicaType(peerReplica.getReplicaType());
        RemoteReplicaInfo remoteReplicaInfo =
            new RemoteReplicaInfo(peerReplica, cloudReplica, store, findTokenFactory.getNewFindToken(),
                storeConfig.storeDataFlushIntervalSeconds * SystemTime.MsPerSec * Replication_Delay_Multiplier,
                SystemTime.getInstance(), peerReplica.getDataNodeId().getPortToConnectTo());
        replicationMetrics.addMetricsForRemoteReplicaInfo(remoteReplicaInfo, trackPerDatacenterLagInMetric);
        remoteReplicaInfos.add(remoteReplicaInfo);
      }
      PartitionInfo partitionInfo = new PartitionInfo(remoteReplicaInfos, partitionId, store, cloudReplica);
      partitionToPartitionInfo.put(partitionId, partitionInfo);
      // For CloudBackupManager, at most one PartitionInfo in the set.
      mountPathToPartitionInfos.computeIfAbsent(cloudReplica.getMountPath(), key -> ConcurrentHashMap.newKeySet())
          .add(partitionInfo);
      partitionStoreMap.put(partitionId.toPathString(), store);
    } else {
      try {
        storeManager.shutdownBlobStore(partitionId);
        storeManager.removeBlobStore(partitionId);
      } finally {
        throw new ReplicationException(
            "Failed to add Partition " + partitionId + " on " + dataNodeId + " , because no peer replicas found.");
      }
    }
    // Reload replication token if exist.
    int tokenReloadFailCount = reloadReplicationTokenIfExists(cloudReplica, remoteReplicaInfos);
    vcrMetrics.tokenReloadWarnCount.inc(tokenReloadFailCount);

    // Add remoteReplicaInfos to {@link ReplicaThread}.
    addRemoteReplicaInfoToReplicaThread(remoteReplicaInfos, true);
    if (replicationConfig.replicationTrackPerPartitionLagFromRemote) {
      replicationMetrics.addLagMetricForPartition(partitionId, true);
    }
  }

  /**
   * Remove a replica of given {@link PartitionId} and its {@link RemoteReplicaInfo}s from the backup list.
   * @param partitionId the {@link PartitionId} of the replica to removed.
   */
  void removeReplica(PartitionId partitionId) {
    stopPartitionReplication(partitionId);
    Store cloudStore = partitionStoreMap.get(partitionId.toPathString());
    if (cloudStore != null) {
      storeManager.shutdownBlobStore(partitionId);
      storeManager.removeBlobStore(partitionId);
    } else {
      logger.warn("Store not found for partition {}", partitionId);
    }
    logger.info("Partition {} removed from {}", partitionId, dataNodeId);
    // We don't close cloudBlobStore because because replicate in ReplicaThread is using a copy of
    // remoteReplicaInfo which needs CloudBlobStore.
  }

  @Override
  public void shutdown() throws ReplicationException {
    // TODO: can do these in parallel
    if (cloudStorageCompactor != null) {
      cloudStorageCompactor.shutdown();
    }
    if (cloudContainerCompactor != null) {
      cloudContainerCompactor.shutdown();
    }
    super.shutdown();
  }

  public VcrMetrics getVcrMetrics() {
    return vcrMetrics;
  }

  /** For testing only */
  CloudStorageCompactor getCloudStorageCompactor() {
    return cloudStorageCompactor;
  }

  @Override
  public void updateTotalBytesReadByRemoteReplica(PartitionId partitionId, String hostName, String replicaPath,
      long totalBytesRead) {
    // Since replica metadata request for a single partition can goto multiple vcr nodes, totalBytesReadByRemoteReplica
    // cannot be  populated locally on any vcr node.
  }

  @Override
  public long getRemoteReplicaLagFromLocalInBytes(PartitionId partitionId, String hostName, String replicaPath) {
    // TODO get replica lag from cosmos?
    return -1;
  }

  @Override
  protected String getReplicaThreadName(String datacenterToReplicateFrom, int threadIndexWithinPool) {
    return "Vcr" + super.getReplicaThreadName(datacenterToReplicateFrom, threadIndexWithinPool);
  }

  /**
   * Check if replication is allowed from given datacenter.
   * @param datacenterName datacenter name to check.
   * @return true if replication is allowed. false otherwise.
   */
  private boolean shouldReplicateFromDc(String datacenterName) {
    return cloudConfig.vcrSourceDatacenters.contains(datacenterName);
  }
}
