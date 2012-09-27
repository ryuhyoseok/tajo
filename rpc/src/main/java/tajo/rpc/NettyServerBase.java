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
import org.apache.hadoop.io.IOUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Random;
import java.util.concurrent.Executors;

public class NettyServerBase {
  private static final Log LOG = LogFactory.getLog(NettyServerBase.class);

  protected InetSocketAddress bindAddress;
  protected ChannelFactory factory;
  protected ChannelPipelineFactory pipelineFactory;
  protected ServerBootstrap bootstrap;
  private Channel channel;

  public NettyServerBase(InetSocketAddress addr) {
    if (addr.getPort() == 0) {
      try {
        int port = getUnusedPort();
        bindAddress = new InetSocketAddress(addr.getHostName(), port);
      } catch (IOException e) {
        LOG.error(e);
      }
    } else {
      bindAddress = addr;
    }
  }

  public void init(ChannelPipelineFactory pipeline) {
    this.factory =
        new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool());

    pipelineFactory = pipeline;
    bootstrap = new ServerBootstrap(factory);
    bootstrap.setPipelineFactory(pipelineFactory);
    // TODO - should be configurable
    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.tcpNoDelay", false);
    bootstrap.setOption("child.keepAlive", true);
    bootstrap.setOption("child.connectTimeoutMillis", 10000);
    bootstrap.setOption("child.connectResponseTimeoutMillis", 10000);
    bootstrap.setOption("child.receiveBufferSize", 1048576*3);
  }

  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  public void start() {
    this.channel = bootstrap.bind(bindAddress);
    LOG.info("RpcServer on " + this.bindAddress);
  }

  public Channel getChannel() {
    return this.channel;
  }

  public void shutdown() {
    channel.close().awaitUninterruptibly();
    LOG.info("RpcServer (" + tajo.util.NetUtils.getIpPortString(bindAddress)
        + ") shutdown");
  }

  private static final Random randomPort = new Random(System.currentTimeMillis());
  private static int getUnusedPort() throws IOException {
    while (true) {
      int port = randomPort.nextInt(10000) + 50000;
      if (available(port)) {
        return port;
      }
    }
  }

  private static boolean available(int port) throws IOException {
    if (port < 1024 || port > 65535) {
      throw new IllegalArgumentException("Port Number Out of Bound: " + port);
    }

    ServerSocket ss = null;
    DatagramSocket ds = null;

    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);

      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);

      return true;

    } catch (IOException e) {
      throw new IOException(e);

    } finally {
      if (ss != null) {
        IOUtils.closeStream(ss);
      }

      if (ds != null) {
        IOUtils.closeStream(ds);
      }
    }
  }
}
