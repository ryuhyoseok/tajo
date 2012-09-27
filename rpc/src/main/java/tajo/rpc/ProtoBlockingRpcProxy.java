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
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.*;
import tajo.rpc.RpcProtos.RpcRequest;
import tajo.rpc.RpcProtos.RpcResponse;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ProtoBlockingRpcProxy extends NettyClientBase {
  private static final Log LOG = LogFactory.getLog(RpcProtos.class);

  private final ChannelUpstreamHandler handler;
  private final ChannelPipelineFactory pipeFactory;
  private final ProxyRpcChannel rpcChannel;

  private final AtomicInteger sequence = new AtomicInteger(0);
  private Map<Integer, ProtoCallFuture> requests
      = new ConcurrentHashMap<Integer, ProtoCallFuture>();

  public ProtoBlockingRpcProxy(InetSocketAddress addr) {
    this.handler = new ClientHandler();
    pipeFactory = new ProtoPipelineFactory(handler,
        RpcResponse.getDefaultInstance());
    super.init(addr, pipeFactory);
    rpcChannel = new ProxyRpcChannel(getChannel());
  }

  public BlockingRpcChannel getBlockingRpcChannel() {
    return this.rpcChannel;
  }

  private class ProxyRpcChannel implements BlockingRpcChannel {
    private final Channel channel;
    private final ClientHandler handler;

    public ProxyRpcChannel(Channel channel) {
      this.channel = channel;
      this.handler = channel.getPipeline().get(ClientHandler.class);
      if (handler == null) {
        throw new IllegalArgumentException("Channel does not have proper handler");
      }
    }

    public Message callBlockingMethod(MethodDescriptor method,
                                      RpcController controller, Message request, Message responsePrototype)
        throws ServiceException {
      LOG.debug("calling blocking method: " + method.getFullName());
      int nextSeqId = sequence.getAndAdd(1);
      Message rpcRequest = buildRequest(true, nextSeqId, true, method, request);
      ProtoCallFuture callFuture = new ProtoCallFuture(responsePrototype);
      requests.put(nextSeqId, callFuture);
      channel.write(rpcRequest);

      try {
        return callFuture.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    private Message buildRequest(boolean hasSequence, int seqId, boolean isBlocking, MethodDescriptor method, Message request) {
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

  private class ClientHandler extends SimpleChannelUpstreamHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      RpcResponse response = (RpcResponse) e.getMessage();
      ProtoCallFuture callback = requests.remove(response.getId());

      if (callback == null) {
        LOG.debug("dangling rpc call");
      } else {
        Message m = (response == null || !response.hasResponseMessage()) ?
            null :
            callback.getReturnType().
                newBuilderForType().mergeFrom(response.getResponseMessage()).build();

        callback.setResponse(m);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      e.getChannel().close();
      LOG.error(e.getCause());
    }
  }

  class ProtoCallFuture implements Future<Message> {
    private Semaphore sem = new Semaphore(0);
    private Message response = null;
    private Message returnType;

    public ProtoCallFuture(Message message) {
      this.returnType = message;
    }

    public Message getReturnType() {
      return this.returnType;
    }

    @Override
    public boolean cancel(boolean arg0) {
      return false;
    }

    @Override
    public Message get() throws InterruptedException, ExecutionException {
      sem.acquire();
      return response;
    }

    @Override
    public Message get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if(sem.tryAcquire(timeout, unit)) {
        return response;
      } else {
        throw new TimeoutException();
      }
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return sem.availablePermits() > 0;
    }

    public void setResponse(Message response) {
      this.response = response;
      sem.release();
    }
  }
}