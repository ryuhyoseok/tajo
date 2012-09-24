/*
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

package tajo.engine.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkListener;
import tajo.zookeeper.ZkUtil;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

public class WorkerTracker extends ZkListener {
  private final Log LOG = LogFactory.getLog(WorkerTracker.class);
  private NavigableSet<ServerName> members = new TreeSet<>();
  private final ZkClient client;
  public static final String LEAF_SERVERS = "/nta/leafservers";

  public WorkerTracker(ZkClient client) throws Exception {
    this.client = client;
  }

  public void start() throws Exception {
    this.client.subscribe(this);
    List<String> servers = ZkUtil
        .listChildrenAndWatchThem(this.client, LEAF_SERVERS);
    add(servers);
  }

  private void add(final List<String> servers) throws IOException {
    synchronized(this.members) {
      this.members.clear();
      for (String n: servers) {
        ServerName sn = ServerName.create(ZkUtil.getNodeName(n));
        this.members.add(sn);
      }
    }
  }

  private void remove(ServerName sn) {
    synchronized (members) {
      members.remove(sn);
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (path.startsWith(LEAF_SERVERS)) {
      String serverName = ZkUtil.getNodeName(path);
      LOG.info("Worker ephemeral node deleted, processing expiration [" +
          serverName + "]");
      ServerName sn = ServerName.create(serverName);
      // TODO - node updates
      remove(sn);
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals("/nta/leafservers")) {
      try {
        List<String> servers =
            ZkUtil.listChildrenAndWatchThem(client, LEAF_SERVERS);
        add(servers);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      } catch (KeeperException e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  public List<String> getMembers() {
    try {
      return client.getChildren(LEAF_SERVERS);
    } catch (KeeperException e) {
      LOG.warn("Unable to list children of znode " + LEAF_SERVERS + " ", e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn("Unable to list children of znode " + LEAF_SERVERS + " ", e);
      return null;
    }
  }

  public void close() {
  }
}
