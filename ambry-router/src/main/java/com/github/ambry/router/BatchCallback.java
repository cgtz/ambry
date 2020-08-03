/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.router;

import com.github.ambry.commons.Callback;
import com.github.ambry.utils.Utils;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;


class BatchCallback<T> {
  private final Callback<List<T>> callback;
  private final long numOperations;
  private final AtomicReferenceArray<Result<T>> results;
  private final AtomicLong ackedCount = new AtomicLong(0);
  private final AtomicBoolean completed = new AtomicBoolean(false);

  /**
   * Constructor
   * @param numOperations the number of operations expected in this batch.
   * @param callback the {@link Callback} to be triggered once acks are received for all operations.
   */
  BatchCallback(int numOperations, Callback<List<T>> callback) {
    this.numOperations = numOperations;
    this.callback = callback;
    results = new AtomicReferenceArray<>(numOperations);
  }

  /**
   * Gets a {@link Callback} personalized for {@code operationIndex}.
   * @param operationIndex the operation number used.
   * @return the {@link Callback} to be used with this operation in the batch.
   */
  Callback<T> getCallback(int operationIndex) {
    if (operationIndex >= numOperations) {
      throw new IllegalArgumentException("Only " + numOperations + ", operation index " + operationIndex + " provided");
    }
    return (result, exception) -> {
      if (exception == null) {
        if (!results.compareAndSet(operationIndex, null, new Result<>(result))) {
          // callback for child operation called more than once.
          complete(new RouterException("Result for operation " + operationIndex + " arrived more than once",
              RouterErrorCode.UnexpectedInternalError));
        } else if (ackedCount.incrementAndGet() >= numOperations) {
          // callback called for the first time for this child operation and all child operation results have been set
          complete(null);
        }
      } else {
        complete(exception);
      }
    };
  }

  /**
   * Completes the batch operation
   * @param e the {@link Exception} that occurred (if any).
   */
  private void complete(Exception e) {
    if (completed.compareAndSet(false, true)) {
      List<T> result = e == null ? resultsAsList() : null;
      NonBlockingRouter.completeOperation(null, callback, result, e, false);
    }
  }

  private List<T> resultsAsList() {
    return Utils.listView(results::length,
        i -> Objects.requireNonNull(results.get(i), () -> "No result set at index " + i).value);
  }

  /**
   * Wrapper class to support null results
   */
  private static class Result<T> {
    private final T value;

    public Result(T value) {
      this.value = value;
    }
  }
}
