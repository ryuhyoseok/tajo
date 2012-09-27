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

package tajo.rpc.test.impl;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import tajo.rpc.test.DummyProtocol.DummyProtocolService.Interface;
import tajo.rpc.test.TestProtos.EchoMessage;
import tajo.rpc.test.TestProtos.SumRequest;
import tajo.rpc.test.TestProtos.SumResponse;

@SuppressWarnings("UnusedDeclaration")
public class DummyProtocolAsyncImpl implements Interface {

  @Override
  public void sum(RpcController controller, SumRequest request,
                  RpcCallback<SumResponse> done) {

    SumResponse response = SumResponse.newBuilder().setResult(
        request.getX1()+request.getX2()+request.getX3()+request.getX4()
    ).build();
    done.run(response);
  }

  @Override
  public void echo(RpcController controller, EchoMessage request,
                   RpcCallback<EchoMessage> done) {

    done.run(
        EchoMessage.newBuilder().
            setMessage(request.getMessage()).build()
    );
  }

  @Override
  public void error(RpcController controller, EchoMessage request,
                    RpcCallback<EchoMessage> done) {
    controller.setFailed("Fatal Error!");
    done.run(null);
  }
}