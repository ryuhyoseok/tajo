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

import java.util.concurrent.*;

public class CallFuture2<T> implements RpcCallback<T>, Future<T> {

  private final Semaphore sem = new Semaphore(0);
  private boolean done = false;
  private T response;

  @Override
  public void run(T t) {
    this.response = t;
    done = true;
    sem.release();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    // TODO - to be implemented
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCancelled() {
    // TODO - to be implemented
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDone() {
    return done;
  }

  @Override
  public T get() throws InterruptedException {
    sem.acquire();

    return response;
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws InterruptedException, TimeoutException {
    if (sem.tryAcquire(timeout, unit)) {
      return response;
    } else {
      throw new TimeoutException();
    }
  }
}
