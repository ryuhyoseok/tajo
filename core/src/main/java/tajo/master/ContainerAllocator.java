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

package tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import tajo.QueryUnitAttemptId;
import tajo.master.event.ContainerAllocatorEvent;
import tajo.master.event.ContainerAllocatorEventType;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ContainerAllocator extends AbstractService
    implements EventHandler<ContainerAllocatorEvent> {
  private static final Log LOG = LogFactory.getLog(ContainerAllocator.class);

  private Thread eventHandlingThread;
  private volatile boolean stopEventHandling;

  BlockingQueue<ContainerAllocatorEvent> eventQueue
      = new LinkedBlockingQueue<>();

  private ScheduledRequests scheduledRequests;

  public ContainerAllocator() {
    super(ContainerAllocator.class.getName());
  }

  public void init(Configuration conf) {

    scheduledRequests = new ScheduledRequests();

    super.init(conf);
  }

  public void start() {
    this.eventHandlingThread = new Thread() {
      public void run() {

        ContainerAllocatorEvent event;
        while(!stopEventHandling && Thread.currentThread().isInterrupted()) {
          try {
          event = eventQueue.take();
            handleEvent(event);
          } catch (InterruptedException e) {
            LOG.error("Returning, iterrupted : " + e);
          }
        }
      }
    };

    this.eventHandlingThread.start();
    super.start();
  }

  public void stop() {
    stopEventHandling = true;
    eventHandlingThread.interrupt();
    super.stop();
  }

  private void handleEvent(ContainerAllocatorEvent event) {
    if (event.getType() == ContainerAllocatorEventType.CONTAINER_REQ) {
      if (event.isLeafQuery()) {
        scheduledRequests.addLeafTask(event);
      }
    }
  }

  @Override
  public void handle(ContainerAllocatorEvent event) {
    int qSize = eventQueue.size();
    if (qSize != 0 && qSize % 1000 == 0) {
      LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "of RMContainerAllocator: " + remCapacity);
    }

    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new InternalError(e.getMessage());
    }
  }

  private class ScheduledRequests {
    private final Map<String, LinkedList<QueryUnitAttemptId>> leafTasksHostMapping =
        new HashMap<>();
    private final Map<String, LinkedList<QueryUnitAttemptId>> leafTasksRackMapping =
        new HashMap<>();

    public void addLeafTask(ContainerAllocatorEvent event) {
      for (String host : event.getHosts()) {
        LinkedList<QueryUnitAttemptId> list = leafTasksHostMapping.get(host);
        if (list == null) {
          list = new LinkedList<>();
          leafTasksHostMapping.put(host, list);
        }
        list.add(event.getAttemptId());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Added attempt req to host " + host);
        }
      }
      for (String rack: event.getRacks()) {
        LinkedList<QueryUnitAttemptId> list = leafTasksRackMapping.get(rack);
        if (list == null) {
          list = new LinkedList<>();
          leafTasksRackMapping.put(rack, list);
        }
        list.add(event.getAttemptId());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Added attempt req to rack " + rack);
        }
      }
    }
  }
}
