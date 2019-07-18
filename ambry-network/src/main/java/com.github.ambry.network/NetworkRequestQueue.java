package com.github.ambry.network;

interface NetworkRequestQueue {
  boolean offer(NetworkRequest request);

  NetworkRequestBundle take() throws InterruptedException;

  int size();
}
