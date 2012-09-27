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

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.Service;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class NettyRpc {

  public static ProtoAsyncRpcServer getProtoAsyncRpcServer(Service instance,
      InetSocketAddress bindAddress) {

    return new ProtoAsyncRpcServer(instance, bindAddress);
  }

  public static ProtoBlockingRpcServer getProtoBlockingRpcServer(
      BlockingService instance, InetSocketAddress bindAddress) {
    return new ProtoBlockingRpcServer(instance, bindAddress);
  }

  public static ProtoAsyncRpcServer getProtoAsyncRpcServer(Class<?> protocol,
      InetSocketAddress bindAddress) throws Exception {

    String serviceClassName = protocol.getName() + "$" +
        protocol.getSimpleName() + "Service";
    Class<?> serviceClass = Class.forName(serviceClassName);
    Class<?> interfaceClass = Class.forName(serviceClassName + "$Interface");

    Method method = serviceClass.getMethod("newReflectiveService", interfaceClass);

    Class<?> implClass = Class.forName(protocol.getPackage().getName() +
        ".impl." + protocol.getSimpleName()+"AsyncImpl");
    Service service = (Service) method.invoke(null, implClass.newInstance());

    return NettyRpc.getProtoAsyncRpcServer(service, bindAddress);
  }

  public static ProtoBlockingRpcServer getProtoBlockingRpcServer(Class<?> protocol,
                                                                 InetSocketAddress bindAddress) throws Exception {
    String serviceClassName = protocol.getName()+"$"+protocol.getSimpleName()+"Service";
    Class<?> serviceClass = Class.forName(serviceClassName);
    Class<?> interfaceClass = Class.forName(serviceClassName+"$BlockingInterface");
    Method method = serviceClass.getMethod("newReflectiveBlockingService", interfaceClass);

    Class<?> implClass = Class.forName(protocol.getPackage().getName()+".impl."+protocol.getSimpleName()+"BlockingImpl");
    BlockingService service = (BlockingService) method.invoke(null, implClass.newInstance());

    return NettyRpc.getProtoBlockingRpcServer(service,bindAddress);
  }

  public static Object getProtoAsyncRpcProxy(Class<?> protocol, InetSocketAddress bindAddress)
      throws Exception {
    String serviceClassName = protocol.getName()+"$"+protocol.getSimpleName()+"Service";
    Class<?> serviceClass = Class.forName(serviceClassName);

    Method method = serviceClass.getMethod("newStub", RpcChannel.class);

    ProtoAsyncRpcProxy proxy = new ProtoAsyncRpcProxy(bindAddress);
    RpcChannel c = proxy.getRpcChannel();

    return method.invoke(null, c);
  }

  public static Object getProtoBlockingRpcProxy(Class<?> protocol, InetSocketAddress bindAddress) throws Exception {
    String serviceClassName = protocol.getName()+"$"+protocol.getSimpleName()+"Service";
    Class<?> serviceClass = Class.forName(serviceClassName);

    Method method = serviceClass.getMethod("newBlockingStub", BlockingRpcChannel.class);

    ProtoBlockingRpcProxy proxy = new ProtoBlockingRpcProxy(bindAddress);
    BlockingRpcChannel c = proxy.getBlockingRpcChannel();

    return method.invoke(null, c);
  }

  public static NettyRpcServer getProtoParamRpcServer(Object instance,
      Class<?> interfaceClass, InetSocketAddress addr) {

    InetSocketAddress newAddress = null;

    if (addr.getPort() == 0) {
      try {
        int port = getUnusedPort(addr.getHostName());
        newAddress = new InetSocketAddress(addr.getHostName(), port);
      } catch (IOException e) {
        e.printStackTrace();
      }

    } else {
      newAddress = addr;
    }

    return new NettyRpcServer(instance, interfaceClass, newAddress);
  }

  @Deprecated
  public static NettyRpcServer getProtoParamRpcServer(Object instance,
      InetSocketAddress addr) {

    InetSocketAddress newAddress = null;

    if (addr.getPort() == 0) {
      try {
        int port = getUnusedPort(addr.getHostName());
        newAddress = new InetSocketAddress(addr.getHostName(), port);
      } catch (IOException e) {
        e.printStackTrace();
      }

    } else {
      newAddress = addr;
    }

    return new NettyRpcServer(instance, newAddress);
  }

  public static Object getProtoParamAsyncRpcProxy(Class<?> serverClass,
      Class<?> clientClass, InetSocketAddress addr) {
    return new NettyAsyncRpcProxy(serverClass, clientClass, addr)
        .getProxy();
  }

  public static Object getProtoParamBlockingRpcProxy(Class<?> protocol,
      InetSocketAddress addr) {
    return new NettyBlockingRpcProxy(protocol, addr).getProxy();
  }

  public static int getUnusedPort(String hostname) throws IOException {
    while (true) {
      int port = (int) (10000 * Math.random() + 10000);
      if (available(port)) {
        return port;
      }
    }
  }

  public static boolean available(int port) {
    if (port < 10000 || port > 20000) {
      throw new IllegalArgumentException("Invalid start port: " + port);
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
    } finally {
      if (ds != null) {
        ds.close();
      }

      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          /* should not be thrown */
        }
      }
    }

    return false;
  }
}
