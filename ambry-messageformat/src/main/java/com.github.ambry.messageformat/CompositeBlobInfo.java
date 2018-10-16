/*
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

package com.github.ambry.messageformat;

import com.github.ambry.router.ByteRange;
import com.github.ambry.store.StoreKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * This class holds information about a composite blob parsed from the metadata blob. It contains the chunk size,
 * total composite blob size, and a list of chunks for the blob's data chunks.
 */
public class CompositeBlobInfo {
  private final long totalSize;
  private final boolean containsNonUniformChunks;
  private final ByteRange range;
  private final List<Chunk> chunks;

  /**
   * Construct a {@link CompositeBlobInfo} object.
   * @param totalSize The total size of the composite blob.
   * @param chunks The list of chunks for this object's data chunks.
   * @param range an optional resolved {@link ByteRange} to indicate that this {@link CompositeBlobInfo} only includes
   *              chunks that include bytes in this range
   * @param containsNonUniformChunks {@code true} if the composite blob contains intermediate data chunks with different
   */
  CompositeBlobInfo(long totalSize, List<Chunk> chunks, ByteRange range, boolean containsNonUniformChunks) {
    this.totalSize = totalSize;
    this.chunks = Collections.unmodifiableList(chunks);
    this.range = range;
    this.containsNonUniformChunks = containsNonUniformChunks;
  }

  /**
   * Get the total size of the composite blob.
   * @return The total size in bytes.
   */
  public long getTotalSize() {
    return totalSize;
  }

  /**
   * Get the list of chunks for the composite blob's data chunks.
   * @return A list of {@link StoreKey}s.
   */
  public List<Chunk> getChunks() {
    return chunks;
  }

  /**
   * @return a resolved (defined start/end) {@link ByteRange} if this {@link CompositeBlobInfo} object was instantiated
   *         with chunks that include bytes in this range. {@code null} if this object contains all chunks.
   */
  public ByteRange getRange() {
    return range;
  }

  /**
   * @return {@code true} if the composite blob contains intermediate data chunks with different sizes.
   */
  public boolean containsNonUniformChunks() {
    return containsNonUniformChunks;
  }

  @Override
  public String toString() {
    return "CompositeBlobInfo{" + "totalSize=" + totalSize + ", chunks=" + chunks + '}';
  }

  /**
   * Contains metadata (key, size, offset) related to a chunk of a composite blob.
   */
  public static class Chunk {
    private final StoreKey storeKey;
    private final int size;
    private final long startOffset;

    /**
     * @param storeKey the {@link StoreKey} of the chunk.
     * @param size the size in bytes of the chunk's data.
     * @param startOffset the offset of the first byte of this chunk, relative to the overall composite blob.
     */
    Chunk(StoreKey storeKey, int size, long startOffset) {
      this.storeKey = storeKey;
      this.size = size;
      this.startOffset = startOffset;
    }

    /**
     * @return the {@link StoreKey} of the chunk.
     */
    public StoreKey getStoreKey() {
      return storeKey;
    }

    /**
     * @return the size in bytes of the chunk's data.
     */
    public int getSize() {
      return size;
    }

    /**
     * @return the offset of the first byte of this chunk, relative to the overall composite blob.
     */
    public long getStartOffset() {
      return startOffset;
    }

    @Override
    public String toString() {
      return "Chunk{" + "storeKey=" + storeKey + ", size=" + size + ", startOffset=" + startOffset + '}';
    }
  }
}
