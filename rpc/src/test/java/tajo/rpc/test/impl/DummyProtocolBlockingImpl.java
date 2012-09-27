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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import tajo.rpc.test.DummyProtocol.DummyProtocolService.BlockingInterface;
import tajo.rpc.test.TestProtos.EchoMessage;
import tajo.rpc.test.TestProtos.SumRequest;
import tajo.rpc.test.TestProtos.SumResponse;

public class DummyProtocolBlockingImpl implements BlockingInterface {

  @Override
  public SumResponse sum(RpcController controller, SumRequest request)
      throws ServiceException {
    return SumResponse.newBuilder().setResult(
        request.getX1()+request.getX2()+request.getX3()+request.getX4()
    ).build();
  }

  @Override
  public EchoMessage echo(RpcController controller, EchoMessage request)
      throws ServiceException {
    return EchoMessage.newBuilder().
        setMessage(request.getMessage()).build();
  }
}