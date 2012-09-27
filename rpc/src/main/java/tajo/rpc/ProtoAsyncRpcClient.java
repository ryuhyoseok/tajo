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

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.*;
import tajo.rpc.RpcProtos.RpcRequest;
import tajo.rpc.RpcProtos.RpcResponse;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ProtoAsyncRpcClient extends NettyClientBase {
  private static final Log LOG = LogFactory.getLog(RpcProtos.class);

  private final ChannelUpstreamHandler handler;
  private final ChannelPipelineFactory pipeFactory;
  private final ProxyRpcChannel rpcChannel;

  private final AtomicInteger sequence = new AtomicInteger(0);
  private final Map<Integer, ResponseCallback> requests =
      new ConcurrentHashMap<>();

  public ProtoAsyncRpcClient(final InetSocketAddress addr) {
    this.handler = new ClientChannelUpstreamHandler();
    pipeFactory = new ProtoPipelineFactory(handler,
        RpcResponse.getDefaultInstance());
    super.init(addr, pipeFactory);
    rpcChannel = new ProxyRpcChannel(getChannel());
  }

  public <T> T getStub(Class<?> protocol)
      throws Exception {
    String serviceClassName = protocol.getName() + "$"
        + protocol.getSimpleName() + "Service";

    Class<?> serviceClass = Class.forName(serviceClassName);
    Method method = serviceClass.getMethod("newStub", RpcChannel.class);

    return (T) method.invoke(null, rpcChannel);
  }

  private class ProxyRpcChannel implements RpcChannel {
    private final Channel channel;
    private final ClientChannelUpstreamHandler handler;

    public ProxyRpcChannel(Channel channel) {
      this.channel = channel;
      this.handler = channel.getPipeline()
          .get(ClientChannelUpstreamHandler.class);

      if (handler == null) {
        throw new IllegalArgumentException("Channel does not have " +
            "proper handler");
      }
    }

    public void callMethod(final MethodDescriptor method,
                           final RpcController controller,
                           final Message param,
                           final Message responseType,
                           RpcCallback<Message> done) {

      int nextSeqId = sequence.getAndIncrement();

      Message request = buildRequest(done != null, nextSeqId, method, param);

      if (done != null) {
        handler.registerCallback(nextSeqId, new ResponseCallback(controller,
            responseType, done));
      }
      channel.write(request);
    }

    private Message buildRequest(boolean hasSequence, int seqId,
                                 MethodDescriptor method,
                                 Message request) {

      RpcRequest.Builder requestBuilder = RpcRequest.newBuilder();

      if (hasSequence) {
        requestBuilder.setId(seqId);
      }

      return requestBuilder
          .setMethodName(method.getName())
          .setRequestMessage(request.toByteString())
          .build();
    }
  }

  private class ResponseCallback implements RpcCallback<RpcResponse> {
    private final RpcController controller;
    private final Message responsePrototype;
    private final RpcCallback<Message> callback;

    public ResponseCallback(RpcController controller,
                            Message responsePrototype,
                            RpcCallback<Message> callback) {
      this.controller = controller;
      this.responsePrototype = responsePrototype;
      this.callback = callback;
    }

    public void run(RpcResponse rpcResponse) {

      if (controller != null || rpcResponse.hasErrorMessage()) {
        this.controller.setFailed(rpcResponse.getErrorMessage());
      } else {
        try {

          Message responseMessage;
          if (rpcResponse == null || !rpcResponse.hasResponseMessage()) {
            responseMessage = null;
          } else {
            responseMessage = responsePrototype.newBuilderForType().mergeFrom(
                rpcResponse.getResponseMessage()).build();
          }
          callback.run(responseMessage);

        } catch (InvalidProtocolBufferException e) {
          LOG.error(e);
          if (controller != null) {
            this.controller.setFailed(e.getMessage());
          }
          callback.run(null);
        }
      }
    }
  }

  private class ClientChannelUpstreamHandler extends SimpleChannelUpstreamHandler {

    synchronized void registerCallback(int seqId, ResponseCallback callback) {

      if (requests.containsKey(seqId)) {
        throw new IllegalArgumentException("Duplicated call");
      }

      requests.put(seqId, callback);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {

      RpcResponse response = (RpcResponse) e.getMessage();
      ResponseCallback callback = requests.remove(response.getId());

      if (callback == null) {
        LOG.error("dangling rpc call");

      } else {
        callback.run(response);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
      e.getChannel().close();
      LOG.error(e.getCause());
    }
  }
}