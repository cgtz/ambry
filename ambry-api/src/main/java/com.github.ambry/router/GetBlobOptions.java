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

package com.github.ambry.router;

/**
 * Represents any options associated with a getBlob request when making a
 * {@link Router#getBlob(String, GetBlobOptions, Callback)} call.
 */
public class GetBlobOptions {
  private final ByteRange range;

  /**
   * Construct a {@link GetBlobOptions} object that represents any options associated with a getBlob request.
   * @param range
   */
  public GetBlobOptions(ByteRange range) {
    this.range = range;
  }

  /**
   * Get the {@link ByteRange} for a getBlob request, if present.
   * @return the {@link ByteRange}, if set, or {@code null} if no range was set.
   */
  public ByteRange getRange() {
    return range;
  }

  @Override
  public String toString() {
    return "GetBlobOptions{range=" + range + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GetBlobOptions that = (GetBlobOptions) o;

    return range != null ? range.equals(that.range) : that.range == null;
  }

  @Override
  public int hashCode() {
    return range != null ? range.hashCode() : 0;
  }
}
