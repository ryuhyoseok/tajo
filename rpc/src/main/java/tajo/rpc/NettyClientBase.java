/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.rpc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class NettyClientBase {
  @SuppressWarnings("unused")
  private static Log LOG = LogFactory.getLog(NettyClientBase.class);

  private ClientSocketChannelFactory factory;
  protected ClientBootstrap bootstrap;
  private ChannelFuture channelFuture;
  private Channel channel;
  protected InetSocketAddress addr;

  public NettyClientBase() {
  }

  public NettyClientBase(InetSocketAddress addr,
      ChannelPipelineFactory pipeFactory) {
    init(addr, pipeFactory);
  }

  public void init(InetSocketAddress addr, ChannelPipelineFactory pipeFactory) {
    this.addr = addr;

    this.factory =
        new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool());

    this.bootstrap = new ClientBootstrap(factory);
    this.bootstrap.setPipelineFactory(pipeFactory);
    // TODO - should be configurable
    this.bootstrap.setOption("connectTimeoutMillis", 10000);
    this.bootstrap.setOption("connectResponseTimeoutMillis", 10000);
    this.bootstrap.setOption("receiveBufferSize", 1048576*2);
    this.bootstrap.setOption("tcpNoDelay", false);
    this.bootstrap.setOption("keepAlive", true);

    this.channelFuture = bootstrap.connect(addr);
    this.channelFuture.awaitUninterruptibly();
    if (!channelFuture.isSuccess()) {
      channelFuture.getCause().printStackTrace();
      throw new RuntimeException(channelFuture.getCause());
    }
    this.channel = channelFuture.getChannel();
  }

  public InetSocketAddress getAddress() {
    return this.addr;
  }

  public Channel getChannel() {
    return this.channel;
  }

  public void shutdown() {
    this.channel.close().awaitUninterruptibly();
    this.bootstrap.releaseExternalResources();
  }
}
