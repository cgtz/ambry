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
package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.SSLFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Netty specific implementation of {@link NioServerFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link NettyServer} and returns a new instance on
 * {@link #getNioServer()}.
 */
public class NettyServerFactory implements NioServerFactory {

  private final NettyConfig nettyConfig;
  private final NettyMetrics nettyMetrics;
  private final Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers;

  /**
   * Creates a new instance of NettyServerFactory.
   * @param verifiableProperties the in-memory {@link VerifiableProperties} to use.
   * @param metricRegistry the {@link MetricRegistry} to use.
   * @param requestHandler the {@link RestRequestHandler} to hand off the requests to.
   * @param publicAccessLogger the {@link PublicAccessLogger} that can be used for public access logging
   * @param restServerState the {@link RestServerState} that can be used to check the health of the system
   *                              to respond to health check requests
   * @param sslFactory the {@link SSLFactory} used to construct the {@link javax.net.ssl.SSLEngine} used for handling
   *                   SSL requests.
   * @throws IllegalArgumentException if any of the arguments are null.
   */
  public NettyServerFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      final RestRequestHandler requestHandler, final PublicAccessLogger publicAccessLogger,
      final RestServerState restServerState, SSLFactory sslFactory) {
    if (verifiableProperties == null || metricRegistry == null || requestHandler == null || publicAccessLogger == null
        || restServerState == null) {
      throw new IllegalArgumentException("Null arg(s) received during instantiation of NettyServerFactory");
    }
    nettyConfig = new NettyConfig(verifiableProperties);
    if (sslFactory == null && nettyConfig.nettyServerSSLEnabled) {
      throw new IllegalArgumentException("NettyServer requires SSL, but sslFactory is null");
    }
    nettyMetrics = new NettyMetrics(metricRegistry);
    ConnectionStatsHandler connectionStatsHandler = new ConnectionStatsHandler(nettyMetrics);

    Map<Integer, ChannelInitializer<SocketChannel>> initializers = new HashMap<>();
    initializers.put(nettyConfig.nettyServerPort,
        new NettyServerChannelInitializer(nettyConfig, nettyMetrics, connectionStatsHandler, requestHandler,
            publicAccessLogger, restServerState, null));
    if (nettyConfig.nettyServerSSLEnabled) {
      initializers.put(nettyConfig.nettyServerSSLPort,
          new NettyServerChannelInitializer(nettyConfig, nettyMetrics, connectionStatsHandler, requestHandler,
              publicAccessLogger, restServerState, sslFactory));
    }
    channelInitializers = Collections.unmodifiableMap(initializers);
  }

  /**
   * Returns a new instance of {@link NettyServer}.
   * @return a new instance of {@link NettyServer}.
   */
  @Override
  public NioServer getNioServer() {
    return new NettyServer(nettyConfig, nettyMetrics, channelInitializers);
  }

  /**
   * Get the {@link ChannelInitializer}s to be used when constructing {@link NettyServer}s.
   * @return an unmodifiable {@link Map} that maps port numbers to {@link ChannelInitializer}s.
   */
  Map<Integer, ChannelInitializer<SocketChannel>> getChannelInitializers() {
    return channelInitializers;
  }
}
