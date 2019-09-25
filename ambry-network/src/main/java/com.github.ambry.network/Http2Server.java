package com.github.ambry.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.buffer.Unpooled.*;
import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpUtil.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import static io.netty.handler.codec.http2.Http2SecurityUtil.*;


public class Http2Server implements NetworkServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(Http2Server.class);
  private static final int PORT = 8443;
  private EventLoopGroup parentGroup = null;
  private EventLoopGroup childGroup = null;

  Http2Server() {
  }

  @Override
  public void start() throws IOException, InterruptedException {
    parentGroup = getSupportedEventLoopGroup();
    childGroup = getSupportedEventLoopGroup();
    final SslContext sslCtx = configureTLS();
    ServerBootstrap b = new ServerBootstrap();
    b.option(ChannelOption.SO_BACKLOG, 1024);
    b.group(parentGroup).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) {
        ch.pipeline().addLast(sslCtx.newHandler(ch.alloc()), new Http2OrHttpHandler());
      }
    });
    b.bind(PORT).sync();
  }

  @Override
  public void shutdown() {
    long shutdownBeginTime = System.currentTimeMillis();
    if (childGroup != null) {
      childGroup.shutdownGracefully();
    }
    if (parentGroup != null) {
      parentGroup.shutdownGracefully();
    }
    try {
      if (childGroup != null && !childGroup.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.error("NettyServer shutdown failed after waiting 30 seconds for child group shutdown");
      }
      if (parentGroup != null && !parentGroup.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.error("NettyServer shutdown failed after waiting 30 seconds for parent group shutdown");
      }
    } catch (InterruptedException e) {
      LOGGER.error("NettyServer termination await was interrupted. Shutdown may have been unsuccessful", e);
    } finally {
      long shutdownTime = System.currentTimeMillis() - shutdownBeginTime;
      LOGGER.info("NettyServer shutdown took {} ms", shutdownTime);
    }
  }

  @Override
  public RequestResponseChannel getRequestResponseChannel() {
    return null;
  }

  private static EventLoopGroup getSupportedEventLoopGroup() {
    if (Epoll.isAvailable()) {
      return new EpollEventLoopGroup();
    } else if (KQueue.isAvailable()) {
      return new KQueueEventLoopGroup();
    } else {
      return new NioEventLoopGroup();
    }
  }

  private static SslContext configureTLS() throws IOException {
    SelfSignedCertificate ssc = null;
    try {
      ssc = new SelfSignedCertificate();
    } catch (CertificateException e) {
      throw new IOException(e);
    }
    ApplicationProtocolConfig apn = new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
        // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
        // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT, ApplicationProtocolNames.HTTP_2);

    return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey(), null)
        .ciphers(CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
        .applicationProtocolConfig(apn)
        .build();
  }

  /**
   * Used during protocol negotiation, the main function of this handler is to
   * return the HTTP/1.1 or HTTP/2 handler once the protocol has been negotiated.
   */
  private static class Http2OrHttpHandler extends ApplicationProtocolNegotiationHandler {

    private static final int MAX_CONTENT_LENGTH = 1024 * 100;

    protected Http2OrHttpHandler() {
      super(ApplicationProtocolNames.HTTP_1_1);
    }

    @Override
    protected void configurePipeline(ChannelHandlerContext ctx, String protocol) throws Exception {
      if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
        configureHttp2(ctx);
        return;
      }

      throw new IllegalStateException("unsupported protocol: " + protocol);
    }

    private static void configureHttp2(ChannelHandlerContext ctx) {
      DefaultHttp2Connection connection = new DefaultHttp2Connection(true);
      InboundHttp2ToHttpAdapter listener = new InboundHttp2ToHttpAdapterBuilder(connection).propagateSettings(true)
          .validateHttpHeaders(false)
          .maxContentLength(MAX_CONTENT_LENGTH)
          .build();

      ctx.pipeline().addLast(new HttpToHttp2ConnectionHandlerBuilder().frameListener(listener)
          // .frameLogger(TilesHttp2ToHttpHandler.logger)
          .connection(connection).build());

      ctx.pipeline().addLast(new Http2RequestHandler());
    }
  }

  /**
   * Handles all the requests for data. It receives a {@link FullHttpRequest},
   * which has been converted by a {@link InboundHttp2ToHttpAdapter} before it
   * arrived here. For further details, check {@link Http2OrHttpHandler} where the
   * pipeline is setup.
   */
  private static class Http2RequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final String V1_PATH = "/v1";

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
      QueryStringDecoder queryString = new QueryStringDecoder(request.uri());
      String streamId = streamId(request);
      if (queryString.path().equalsIgnoreCase(V1_PATH)) {
        handleRequest(ctx, streamId, request);
      } else {
        sendBadRequest(ctx, streamId);
      }
    }

    private static void sendBadRequest(ChannelHandlerContext ctx, String streamId) {
      FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST, EMPTY_BUFFER);
      streamId(response, streamId);
      ctx.writeAndFlush(response);
    }

    private void handleRequest(ChannelHandlerContext ctx, String streamId, FullHttpRequest request) {
      ByteBuf image = EMPTY_BUFFER;
      FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, image.duplicate());
      response.headers().set(CONTENT_TYPE, "image/jpeg");
      sendResponse(ctx, streamId, 1000, response, request);
    }

    protected void sendResponse(final ChannelHandlerContext ctx, String streamId, int latency,
        final FullHttpResponse response, final FullHttpRequest request) {
      setContentLength(response, response.content().readableBytes());
      streamId(response, streamId);
      ctx.executor().schedule(() -> {
        ctx.writeAndFlush(response);
      }, latency, TimeUnit.MILLISECONDS);
    }

    private static String streamId(FullHttpRequest request) {
      return request.headers().get(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
    }

    private static void streamId(FullHttpResponse response, String streamId) {
      response.headers().set(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      cause.printStackTrace();
      ctx.close();
    }
  }
}


