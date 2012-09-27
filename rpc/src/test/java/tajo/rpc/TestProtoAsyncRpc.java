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

import com.google.protobuf.RpcCallback;
import org.junit.Test;
import tajo.rpc.test.DummyProtocol;
import tajo.rpc.test.DummyProtocol.DummyProtocolService.Interface;
import tajo.rpc.test.TestProtos.EchoMessage;
import tajo.rpc.test.TestProtos.SumRequest;
import tajo.rpc.test.TestProtos.SumResponse;
import tajo.rpc.test.impl.DummyProtocolAsyncImpl;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestProtoAsyncRpc {
  private static String MESSAGE = "TestProtoAsyncRpc";

  double sum;
  String echo;

  @Test
  public void testRpc() throws Exception {
    ProtoAsyncRpcServer server = new ProtoAsyncRpcServer(DummyProtocol.class,
        new DummyProtocolAsyncImpl(), new InetSocketAddress(10003));
    server.start();

    ProtoAsyncRpcClient client = new ProtoAsyncRpcClient(new InetSocketAddress(10003));
    Interface service = client.getStub(DummyProtocol.class);

    SumRequest sumRequest = SumRequest.newBuilder()
        .setX1(1)
        .setX2(2)
        .setX3(3.15d)
        .setX4(2.0f).build();

    service.sum(null, sumRequest, new RpcCallback<SumResponse>() {
      @Override
      public void run(SumResponse parameter) {
        sum = parameter.getResult();
        assertTrue(8.15d == sum);
      }
    });


    EchoMessage echoMessage = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();

    service.echo(null, echoMessage, new RpcCallback<EchoMessage>() {
      @Override
      public void run(EchoMessage parameter) {
        echo = parameter.getMessage();
        assertEquals(MESSAGE, echo);
      }
    });

    Thread.sleep(1000);
    client.close();
    server.shutdown();
  }
}