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

package tajo.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import tajo.engine.cluster.ClusterManager.WorkerResource;
import tajo.master.event.WorkerEvent;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class WorkerManager extends AbstractService
    implements EventHandler<WorkerEvent> {
  private final EventHandler eventHandler;

  private Map<String, WorkerResource> workers = new ConcurrentHashMap<>();

  private BlockingQueue<WorkerEvent> eventQueue = new LinkedBlockingQueue<>();

  public WorkerManager(EventHandler eventHandler) {
    super(WorkerManager.class.getName());
    this.eventHandler = eventHandler;
  }

  public void init(Configuration conf) {

    super.init(conf);
  }

  public void start() {

  }

  public void stop() {

  }

  @Override
  public void handle(WorkerEvent workerEvent) {
    switch (workerEvent.getType()) {
      case STATUS_UPDATE:

      case SHUTDOWN:
    }
  }
}
