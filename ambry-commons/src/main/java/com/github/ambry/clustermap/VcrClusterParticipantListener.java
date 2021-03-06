/**
 *
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
package com.github.ambry.clustermap;

/**
 * {@link VcrClusterParticipantListener} takes actions on {@link VcrClusterParticipant} partition add or removal.
 */
public interface VcrClusterParticipantListener {

  /**
   * Action to take when new Partition is added.
   * @param partitionId on add.
   */
  void onPartitionAdded(PartitionId partitionId);

  /**
   * Action to take when new Partition is removed.
   * @param partitionId on remove.
   */
  void onPartitionRemoved(PartitionId partitionId);
}
