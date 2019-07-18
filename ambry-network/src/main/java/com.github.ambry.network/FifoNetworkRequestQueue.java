package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class FifoNetworkRequestQueue implements NetworkRequestQueue {
  private final int timeout;
  private final Time time;
  private final BlockingQueue<NetworkRequest> queue;

  FifoNetworkRequestQueue(int capacity, int timeout, Time time) {
    this.timeout = timeout;
    this.time = time;
    queue = new ArrayBlockingQueue<>(capacity);
  }

  @Override
  public boolean offer(NetworkRequest request) {
    return queue.offer(request);
  }

  @Override
  public NetworkRequestBundle take() throws InterruptedException {
    NetworkRequest requestToServe = null;
    List<NetworkRequest> requestsToDrop = new ArrayList<>();
    NetworkRequest nextRequest;
    while ((nextRequest = queue.poll()) != null) {
      if (needToDrop(nextRequest)) {
        requestsToDrop.add(nextRequest);
      } else {
        requestToServe = nextRequest;
        break;
      }
    }
    // If there are no requests to drop and no requests to serve currently in the queue, block until a new request comes
    // so we can give the consumer something to do.
    if (requestToServe == null && requestsToDrop.isEmpty()) {
      requestToServe = queue.take();
    }
    return new NetworkRequestBundle(requestToServe, requestsToDrop);
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public String toString() {
    return queue.toString();
  }

  private boolean needToDrop(NetworkRequest request) {
    return time.milliseconds() - request.getStartTimeInMs() > timeout;
  }
}
