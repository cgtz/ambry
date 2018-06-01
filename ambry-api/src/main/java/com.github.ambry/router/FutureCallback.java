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
package com.github.ambry.router;

import java.util.concurrent.CompletableFuture;


public class FutureCallback<T> implements Callback<T> {
  private final CompletableFuture<T> future = new CompletableFuture<>();

  /**
   * Complete the {@link CompletableFuture} that is returned by {@link #getFuture}.
   * @param result The result of the request. This would be non null when the request executed successfully
   * @param exception The exception that was reported on execution of the request
   */
  @Override
  public void onCompletion(T result, Exception exception) {
    if (exception != null) {
      future.completeExceptionally(exception);
    } else {
      future.complete(result);
    }
  }

  /**
   * @return the {@link CompletableFuture} that will be completed by {@link #onCompletion}.
   */
  public CompletableFuture<T> getFuture() {
    return future;
  }
}
