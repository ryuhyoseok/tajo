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

import org.junit.Test;
import tajo.rpc.test.DummyProtocol;
import tajo.rpc.test.DummyProtocol.DummyProtocolService.BlockingInterface;
import tajo.rpc.test.TestProtos.EchoMessage;
import tajo.rpc.test.TestProtos.SumRequest;
import tajo.rpc.test.TestProtos.SumResponse;
import tajo.rpc.test.impl.DummyProtocolBlockingImpl;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestProtoBlockingRpc {
  public static String MESSAGE = "TestProtoBlockingRpc";

  @Test
  public void testRpc() throws Exception {
    ProtoBlockingRpcServer server =
        new ProtoBlockingRpcServer(DummyProtocol.class,
            new DummyProtocolBlockingImpl(),
            new InetSocketAddress(10000));
    server.start();

    ProtoBlockingRpcClient client = new ProtoBlockingRpcClient(
        new InetSocketAddress(10000));
    BlockingInterface service = client.getStub(DummyProtocol.class);

    SumRequest request = SumRequest.newBuilder()
        .setX1(1)
        .setX2(2)
        .setX3(3.15d)
        .setX4(2.0f).build();
    SumResponse response1 = service.sum(null, request);
    assertTrue(8.15d == response1.getResult());

    EchoMessage message = EchoMessage.newBuilder()
        .setMessage(MESSAGE).build();
    EchoMessage response2 = service.echo(null, message);
    assertEquals(MESSAGE, response2.getMessage());

    client.close();
    server.shutdown();
  }
}