/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import tajo.conf.TajoConf;

import java.io.IOException;

public class MockupShutdownWorker extends MockupWorker {
  private final static Log LOG = LogFactory.getLog(MockupShutdownWorker.class);
  private int lifetime;

  public MockupShutdownWorker(final TajoConf conf, final int lifetime) {
    super(conf, Type.SHUTDOWN);
    this.lifetime = lifetime;
  }

  @Override
  public void run() {
    try {
      prepareServing();
      participateCluster();
      if (!this.stopped) {
        long before = -1;
        long sleeptime = 3000;
        long time;
        while (!this.stopped) {
          time = System.currentTimeMillis();
          if (before == -1) {
            sleeptime = 3000;
          } else {
            sleeptime = 3000 - (time - before);
            if (sleeptime > 0) {
              sleep(sleeptime);
            }
          }
          lifetime -= 3000;
          if (lifetime <= 0) {
            shutdown("shutdown test");
            break;
          }
          progressTask();

          sendHeartbeat(time);
          before = time;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      clear();
    }
  }
}
