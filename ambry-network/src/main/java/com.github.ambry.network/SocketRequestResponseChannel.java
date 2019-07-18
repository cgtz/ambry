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
package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// The request at the network layer
class SocketServerRequest implements NetworkRequest {
  private final int processor;
  private final String connectionId;
  private final InputStream input;
  private final long startTimeInMs;
  private static final Logger logger = LoggerFactory.getLogger(SocketServerRequest.class);
  private Object buffer;

  public SocketServerRequest(int processor, String connectionId, Object buffer, InputStream input) throws IOException {
    this.processor = processor;
    this.connectionId = connectionId;
    this.buffer = buffer;
    this.input = input;
    this.startTimeInMs = SystemTime.getInstance().milliseconds();
    logger.trace("Processor {} received request : {}", processor, connectionId);
  }

  @Override
  public InputStream getInputStream() {
    return input;
  }

  @Override
  public long getStartTimeInMs() {
    return startTimeInMs;
  }

  @Override
  public void release() {
    if (buffer != null) {
      ReferenceCountUtil.release(buffer);
      buffer = null;
    }
  }

  public int getProcessor() {
    return processor;
  }

  public String getConnectionId() {
    return connectionId;
  }
}

// The response at the network layer
class SocketServerResponse implements NetworkResponse {

  private final int processor;
  private final NetworkRequest request;
  private final Send output;
  private final ServerNetworkResponseMetrics metrics;
  private long startQueueTimeInMs;

  public SocketServerResponse(NetworkRequest request, Send output, ServerNetworkResponseMetrics metrics) {
    this.request = request;
    this.output = output;
    this.processor = ((SocketServerRequest) request).getProcessor();
    this.metrics = metrics;
  }

  public Send getPayload() {
    return output;
  }

  public NetworkRequest getRequest() {
    return request;
  }

  public int getProcessor() {
    return processor;
  }

  public void onEnqueueIntoResponseQueue() {
    this.startQueueTimeInMs = SystemTime.getInstance().milliseconds();
  }

  public void onDequeueFromResponseQueue() {
    if (metrics != null) {
      metrics.updateQueueTime(SystemTime.getInstance().milliseconds() - startQueueTimeInMs);
    }
  }

  public ServerNetworkResponseMetrics getMetrics() {
    return metrics;
  }
}

interface ResponseListener {
  public void onResponse(int processorId);
}

/**
 * RequestResponse channel for socket server
 */
public class SocketRequestResponseChannel implements RequestResponseChannel {

  interface ResponseListener {
    void onResponse(int processorId);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SocketRequestResponseChannel.class);

  private final int numProcessors;
  private final NetworkRequestQueue networkRequestQueue;
  private final ArrayList<BlockingQueue<NetworkResponse>> responseQueues;
  private final ArrayList<ResponseListener> responseListeners;
  private final Queue<NetworkRequest> unqueuedRequests = new ConcurrentLinkedQueue<>();

  public SocketRequestResponseChannel(NetworkConfig config) {
    numProcessors = config.numIoThreads;
    Time time = SystemTime.getInstance();
    switch (config.requestQueueType) {
      case ADAPTIVE_LIFO_CO_DEL:
        this.networkRequestQueue =
            new AdaptiveLifoCoDelNetworkRequestQueue(config.queuedMaxRequests, 0.7, 100, config.requestQueueTimeoutMs,
                time);
        break;
      case BASIC_FIFO:
        this.networkRequestQueue =
            new FifoNetworkRequestQueue(config.queuedMaxRequests, config.requestQueueTimeoutMs, time);
        break;
      default:
        throw new IllegalArgumentException("Queue type not supported by channel: " + config.requestQueueType);
    }
    responseQueues = new ArrayList<>(this.numProcessors);
    responseListeners = new ArrayList<>();

    for (int i = 0; i < this.numProcessors; i++) {
      responseQueues.add(i, new LinkedBlockingQueue<>());
    }
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  @Override
  public void sendRequest(NetworkRequest request) {
    if (!networkRequestQueue.offer(request)) {
      LOGGER.debug("Request queue is full, dropping incoming request: {}", request);
      unqueuedRequests.add(request);
    }
  }

  /** Send a response back to the socket server to be sent over the network */
  @Override
  public void sendResponse(Send payloadToSend, NetworkRequest originalRequest, ServerNetworkResponseMetrics metrics)
      throws InterruptedException {
    SocketServerResponse response = new SocketServerResponse(originalRequest, payloadToSend, metrics);
    response.onEnqueueIntoResponseQueue();
    responseQueues.get(response.getProcessor()).put(response);
    for (ResponseListener listener : responseListeners) {
      listener.onResponse(response.getProcessor());
    }
  }

  /**
   * Closes the connection and does not send any response
   */
  @Override
  public void closeConnection(NetworkRequest originalRequest) throws InterruptedException {
    SocketServerResponse response = new SocketServerResponse(originalRequest, null, null);
    responseQueues.get(response.getProcessor()).put(response);
    for (ResponseListener listener : responseListeners) {
      listener.onResponse(response.getProcessor());
    }
  }

  /** Get the next request or block until there is one */
  @Override
  public NetworkRequestBundle receiveRequest() throws InterruptedException {
    NetworkRequestBundle networkRequestBundle = networkRequestQueue.take();
    NetworkRequest unqueuedRequest;
    while ((unqueuedRequest = unqueuedRequests.poll()) != null) {
      networkRequestBundle.getRequestsToDrop().add(unqueuedRequest);
    }
    return networkRequestBundle;
  }

  /** Get a response for the given processor if there is one */
  public NetworkResponse receiveResponse(int processor) {
    return responseQueues.get(processor).poll();
  }

  public void addResponseListener(ResponseListener listener) {
    responseListeners.add(listener);
  }

  public int getRequestQueueSize() {
    return networkRequestQueue.size();
  }

  public int getResponseQueueSize(int processor) {
    return responseQueues.get(processor).size();
  }

  public int getNumberOfProcessors() {
    return numProcessors;
  }

  public void shutdown() {
    networkRequestQueue.forEach(NetworkRequest::release);
    networkRequestQueue.clear();
  }
}


