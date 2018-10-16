/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import java.util.ArrayList;
import java.util.List;


/**
 * Builder for constructing {@link CompositeBlobInfo} objects.
 */
public class CompositeBlobInfoBuilder {
  private final List<CompositeBlobInfo.Chunk> chunks = new ArrayList<>();
  private long currentOffset = 0L;

  private int firstChunkSize = -1;
  private int lastChunkSize = -1;
  private boolean nonUniformChunkDetected = false;

  /**
   * Add a chunk to the chunk list.
   * @param storeKey the {@link StoreKey} of the chunk.
   * @param chunkSize the size in bytes of the chunk's data.
   * @return this builder.
   */
  public CompositeBlobInfoBuilder addChunk(StoreKey storeKey, int chunkSize) {
    chunks.add(new CompositeBlobInfo.Chunk(storeKey, chunkSize, currentOffset));
    currentOffset += chunkSize;
    // check the last chunk added to see if there is an intermediate chunk that does not match the first chunk size
    nonUniformChunkDetected |= (lastChunkSize != firstChunkSize);
    if (firstChunkSize == -1) {
      firstChunkSize = chunkSize;
    }
    lastChunkSize = chunkSize;
    return this;
  }

  /**
   * @return the constructed {@link CompositeBlobInfo}.
   */
  public CompositeBlobInfo build() {
    // check if the last chunk is larger than the intermediate chunks. If so, this composite blob requires a metadata
    // format that supports non uniform chunk sizes.
    nonUniformChunkDetected |= (lastChunkSize > firstChunkSize);
    return new CompositeBlobInfo(currentOffset, chunks, null, nonUniformChunkDetected);
  }
}
