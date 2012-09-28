/*
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

public class ProtoAsyncRpcServer extends NettyServerBase {
  private static final Log LOG = LogFactory.getLog(ProtoAsyncRpcServer.class);

  private final Service service;
  private final ChannelPipelineFactory pipeline;

  public ProtoAsyncRpcServer(final Class<?> protocol,
                             final Object instance,
                             final InetSocketAddress bindAddress)
      throws Exception {
    super(bindAddress);

    String serviceClassName = protocol.getName() + "$" +
        protocol.getSimpleName() + "Service";
    Class<?> serviceClass = Class.forName(serviceClassName);
    Class<?> interfaceClass = Class.forName(serviceClassName + "$Interface");
    Method method = serviceClass.getMethod("newReflectiveService", interfaceClass);
    this.service = (Service) method.invoke(null, instance);

    ServerHandler handler = new ServerHandler();
    this.pipeline = new ProtoPipelineFactory(handler,
        RpcRequest.getDefaultInstance());
    super.init(this.pipeline);
  }

  private class ServerHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {

      final RpcRequest request = (RpcRequest) e.getMessage();

      MethodDescriptor methodDescriptor = service.getDescriptorForType().
          findMethodByName(request.getMethodName());

      Message methodRequest = null;
      if (request.hasRequestMessage()) {
        try {
          methodRequest = service.getRequestPrototype(methodDescriptor)
                  .newBuilderForType().mergeFrom(request.getRequestMessage()).
                  build();
        } catch (Throwable t) {
          throw new RemoteCallException(request.getId(), methodDescriptor, t);
        }
      }

      final Channel channel = e.getChannel();
      final RpcController controller = new NettyRpcController();

      RpcCallback<Message> callback =
          !request.hasId() ? null : new RpcCallback<Message>() {

        public void run(Message methodResponse) {

          RpcResponse.Builder builder = RpcResponse.newBuilder()
              .setId(request.getId());

          if (methodResponse != null) {
            builder.setResponseMessage(methodResponse.toByteString());
          }

          if (controller.failed()) {
            builder.setErrorMessage(controller.errorText());
          }

          channel.write(builder.build());
        }
      };

      service.callMethod(methodDescriptor, controller, methodRequest, callback);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception{
      if (e.getCause() instanceof RemoteCallException) {
        RemoteCallException callException = (RemoteCallException) e.getCause();
        e.getChannel().write(callException.getResponse());
      }
      throw new RemoteException(e.getCause());
    }
  }
}