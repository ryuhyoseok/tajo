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

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.*;
import tajo.rpc.RpcProtos.RpcRequest;
import tajo.rpc.RpcProtos.RpcResponse;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;

public class ProtoBlockingRpcServer extends NettyServerBase {
  private static Log LOG = LogFactory.getLog(ProtoBlockingRpcServer.class);
  private final BlockingService service;
  private final ChannelPipelineFactory pipeline;

  public ProtoBlockingRpcServer(final Class<?> protocol,
                                final Object instance,
                                final InetSocketAddress bindAddress)
      throws Exception {

    super(bindAddress);

    String serviceClassName = protocol.getName() + "$" +
        protocol.getSimpleName() + "Service";
    Class<?> serviceClass = Class.forName(serviceClassName);
    Class<?> interfaceClass = Class.forName(serviceClassName +
        "$BlockingInterface");
    Method method = serviceClass.getMethod(
        "newReflectiveBlockingService", interfaceClass);

    this.service = (BlockingService) method.invoke(null, instance);
    this.pipeline = new ProtoPipelineFactory(new ServerHandler(),
        RpcRequest.getDefaultInstance());

    super.init(this.pipeline);
  }

  private class ServerHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {

      final RpcRequest request = (RpcRequest) e.getMessage();
      String methodName = request.getMethodName();

      MethodDescriptor methodDescriptor =
          service.getDescriptorForType().findMethodByName(methodName);

      if (methodDescriptor == null) {
        throw new NoSuchMethodException(methodName);

      } else {
        Message paramProto = null;

        try {
          paramProto = service.getRequestPrototype(methodDescriptor)
                  .newBuilderForType().mergeFrom(request.getRequestMessage()).
                  build();

        } catch (InvalidProtocolBufferException ipb) {
          LOG.error(ipb.getMessage());
        }

        Message methodResponse = null;
        RpcController controller = new NettyRpcController();

        try {
          methodResponse = service.callBlockingMethod(methodDescriptor, controller,
              paramProto);
        } catch (ServiceException se) {
          LOG.error(se.getMessage());
        }

        RpcResponse.Builder resBuilder =
            RpcResponse.newBuilder().setId(request.getId());
        if (methodResponse != null) {
          resBuilder.setResponseMessage(methodResponse.toByteString());
        } else {
          resBuilder.setErrorMessage(controller.errorText());
        }

        e.getChannel().write(resBuilder.build());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      LOG.error(e.getCause());
      e.getChannel().close();
    }
  }
}