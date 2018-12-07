package com.github.ambry.network;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.SSLFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;

import java.util.List;

public class NettyNetworkClient implements NetworkClient {
    private final SSLFactory sslFactory;
    private final EventLoopGroup group;
    private final Bootstrap bootstrap;

    public NettyNetworkClient(SSLFactory sslFactory) {
        this.sslFactory = sslFactory;
        group = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new Initializer());
    }

    @Override
    public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestInfos, int pollTimeoutMs) {
        requestInfos.forEach(this::sendRequest);
        return null;
    }

    @Override
    public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode, long timeForWarmUp) {
        return 0;
    }

    @Override
    public void close() {
        group.shutdownGracefully();
    }

    @Override
    public void wakeup() {

    }

    private void sendRequest(RequestInfo requestInfo) {
        // Make a new connection.
        ChannelFuture f = bootstrap.connect(requestInfo.getHost(), requestInfo.getPort().getPort());
        f.channel().writeAndFlush(requestInfo.getRequest());
    }

    private class Initializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            if (sslFactory != null) {
                p.addLast(new SslHandler(sslFactory.createSSLEngine(ch.remoteAddress().getHostName(), ch.remoteAddress().getPort(), SSLFactory.Mode.CLIENT)));
            }

            p.addLast(new ProtobufVarint32FrameDecoder());
            p.addLast(new ProtobufDecoder(WorldClockProtocol.LocalTimes.getDefaultInstance()));

            p.addLast(new ProtobufVarint32LengthFieldPrepender());
            p.addLast(new ProtobufEncoder());

            p.addLast(new WorldClockClientHandler());
        }
    }
}
