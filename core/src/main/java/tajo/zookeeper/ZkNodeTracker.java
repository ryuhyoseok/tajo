/*
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

package tajo.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import tajo.NConstants;

public abstract class ZkNodeTracker extends ZkListener {
  private static final Log LOG = LogFactory.getLog(ZkNodeTracker.class);

  private final ZkClient client;
  protected final String node;

  private byte[] data;

  private boolean stopped = false;

  /**
	 * 
	 */
  public ZkNodeTracker(ZkClient client, String node) {
    this.client = client;
    this.node = node;
  }

  public synchronized void start() {
    this.client.subscribe(this);
    try {
      if (ZkUtil.watchAndCheckExists(this.client, node)) {
        byte[] data = ZkUtil.getDataAndWatch(this.client, node);
        if (data != null) {
          this.data = data;
        } else {
          // It existed but now does not, try again to ensure a watch is set
          start();
        }
      }
    } catch (KeeperException e) {
      LOG.error("Unexpected exception during initialization, aborting", e);
    }
  }

  public synchronized void stop() {
    this.stopped = true;
    notifyAll();
  }

  /**
   * Gets the data of the node, blocking until the node is available.
   * 
   * @return data of the node
   * @throws InterruptedException
   *           if the waiting thread is interrupted
   */
  public synchronized byte[] blockUntilAvailable() throws InterruptedException {
    return blockUntilAvailable(0);
  }

  /**
   * Gets the data of the node, blocking until the node is available or the
   * specified timeout has elapsed.
   * 
   * @param timeout
   *          maximum time to wait for the node data to be available, n
   *          milliseconds. Pass 0 for no timeout.
   * @return data of the node
   * @throws InterruptedException
   *           if the waiting thread is interrupted
   */
  public synchronized byte[] blockUntilAvailable(long timeout)
      throws InterruptedException {
    if (timeout < 0)
      throw new IllegalArgumentException();
    boolean notimeout = timeout == 0;
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    while (!this.stopped && (notimeout || remaining > 0) && this.data == null) {
      if (notimeout) {
        wait();
        continue;
      }
      wait(remaining);
      remaining = timeout - (System.currentTimeMillis() - startTime);
    }
    return data;
  }

  /**
   * Gets the data of the node.
   * 
   * <p>
   * If the node is currently available, the most up-to-date known version of
   * the data is returned. If the node is not currently available, null is
   * returned.
   * 
   * @return data of the node, null if unavailable
   */
  public synchronized byte[] getData() {
    return data;
  }

  public String getNode() {
    return this.node;
  }

  @Override
  public synchronized void nodeCreated(String path) {
    if (!path.equals(node))
      return;
    try {
      byte[] data = ZkUtil.getDataAndWatch(client, node);
      if (data != null) {
        this.data = data;
        notifyAll();
      } else {
        nodeDeleted(path);
      }
    } catch (KeeperException e) {
      LOG.error("Unexpected exception handling nodeCreated event", e);
    }
  }

  @Override
  public synchronized void nodeDeleted(String path) {
    if (path.equals(node)) {
      try {
        if (ZkUtil.watchAndCheckExists(client, node)) {
          nodeCreated(path);
        } else {
          this.data = null;
        }
      } catch (KeeperException e) {
        LOG.error("Unexpected exception handling nodeDeleted event", e);
      }
    }
  }

  @Override
  public synchronized void nodeDataChanged(String path) {
    if (path.equals(node)) {
      nodeCreated(path);
    }
  }

  /**
   * Checks if the baseznode set as per the property 'zookeeper.znode.parent'
   * exists.
   * 
   * @return true if baseznode exists. false if doesnot exists.
   */
  public boolean checkIfBaseNodeAvailable() {
    try {
      if (ZkUtil.checkExists(client, NConstants.ZNODE_BASE) == -1) {
        return false;
      }
    } catch (KeeperException e) {
      LOG.error("Exception while checking if basenode exists.", e);
    }
    return true;
  }
}