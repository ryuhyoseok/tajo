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

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.Service;
import tajo.rpc.RpcProtos.RpcRequest;
import tajo.rpc.RpcProtos.RpcResponse;

public class ProtoAsyncRpcServer extends NettyServerBase {
  private static final Log LOG = LogFactory.getLog(ProtoAsyncRpcServer.class);

  private final Service service;
  private final ChannelPipelineFactory pipeline;

  public ProtoAsyncRpcServer(Service instance, InetSocketAddress bindAddress) {
    super(bindAddress);
    this.service = instance;
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

      String methodName = request.getMethodName();

      MethodDescriptor methodDescriptor = service.getDescriptorForType().
          findMethodByName(methodName);

      Message methodRequest = null;
      try {
        methodRequest = service.getRequestPrototype(methodDescriptor)
                .newBuilderForType().mergeFrom(request.getRequestMessage()).
                build();
      } catch (InvalidProtocolBufferException ipb) {
        LOG.error(ipb.getMessage());
      }

      final Channel channel = e.getChannel();

      RpcCallback<Message> callback = !request.hasId() ? null : new RpcCallback<Message>() {
        public void run(Message methodResponse) {
          if (methodResponse != null) {
            channel.write(RpcResponse.newBuilder()
                .setId(request.getId())
                .setResponseMessage(methodResponse.toByteString())
                .build());
          } else { // when it returns null
            RpcResponse.Builder builder = RpcResponse.newBuilder()
                .setId(request.getId());
            channel.write(builder.build());
          }
        }
      };

      service.callMethod(methodDescriptor, null, methodRequest, callback);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      e.getChannel().close();
    }
  }
}