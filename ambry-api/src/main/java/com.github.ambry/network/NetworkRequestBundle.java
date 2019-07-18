package com.github.ambry.network;

import java.util.Collection;
import java.util.Collections;


/**
 * A collection of requests to either serve or drop.
 */
public class NetworkRequestBundle {
  private final NetworkRequest requestToServe;
  private final Collection<NetworkRequest> requestsToDrop;

  NetworkRequestBundle(NetworkRequest requestToServe, Collection<NetworkRequest> requestsToDrop) {
    this.requestToServe = requestToServe;
    this.requestsToDrop = requestsToDrop;
  }

  /**
   * @return a request that the request handler should serve. Can be null if there are no pending requests that have
   * not timed out.
   */
  public NetworkRequest getRequestToServe() {
    return requestToServe;
  }

  /**
   * @return requests that have spent too long in the queue and should be dropped by the request handler.
   */
  public Collection<NetworkRequest> getRequestsToDrop() {
    return requestsToDrop;
  }
}
