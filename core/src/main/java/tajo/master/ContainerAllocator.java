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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.RackResolver;
import tajo.QueryUnitAttemptId;
import tajo.engine.query.QueryUnitRequestImpl;
import tajo.ipc.protocolrecords.QueryUnitRequest;
import tajo.master.TajoMaster.MasterContext;
import tajo.master.event.*;
import tajo.master.event.TaskRequestEvent.TaskRequestEventType;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

public class ContainerAllocator extends AbstractService
    implements EventHandler<ContainerAllocatorEvent> {
  private static final Log LOG = LogFactory.getLog(ContainerAllocator.class);

  private final MasterContext context;
  private AsyncDispatcher dispatcher;

  private Thread eventHandlingThread;
  private Thread schedulingThread;
  private volatile boolean stopEventHandling;

  BlockingQueue<ContainerAllocatorEvent> eventQueue
      = new LinkedBlockingQueue<>();

  private ScheduledRequests scheduledRequests;
  private TaskRequests taskRequests;

  public ContainerAllocator(MasterContext context, AsyncDispatcher dispatcher) {
    super(ContainerAllocator.class.getName());
    this.context = context;
    this.dispatcher = dispatcher;
  }

  public void init(Configuration conf) {

    scheduledRequests = new ScheduledRequests();
    taskRequests  = new TaskRequests();
    dispatcher.register(TaskRequestEventType.class, taskRequests);

    super.init(conf);
  }

  public void start() {
    this.eventHandlingThread = new Thread() {
      public void run() {

        ContainerAllocatorEvent event;
        while(!stopEventHandling && !Thread.currentThread().isInterrupted()) {
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

    this.schedulingThread = new Thread() {
      public void run() {

        while(!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.warn(e);
          }

          schedule();
        }
      }
    };

    this.schedulingThread.start();
    super.start();
  }

  public void stop() {
    stopEventHandling = true;
    eventHandlingThread.interrupt();
    schedulingThread.interrupt();
    super.stop();
  }

  private void handleEvent(ContainerAllocatorEvent event) {
    if (event.getType() == ContainerAllocatorEventType.CONTAINER_REQ) {
      if (event.isLeafQuery()) {
        scheduledRequests.addLeafTask(event);
      }
    }
  }

  List<TaskRequestEvent> taskRequestEvents = new ArrayList<>();
  public void schedule() {
    if (scheduledRequests.size() > 0 && taskRequests.size() > 0) {
      LOG.info("Try to schedule tasks - taskRequestEvents: " +
          taskRequests.size() + ", Schedule Request: " + scheduledRequests.size());
      taskRequests.getTaskRequests(taskRequestEvents, scheduledRequests.size());
      scheduledRequests.assignToLeafTasks(taskRequestEvents);
      taskRequestEvents.clear();
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

  private class TaskRequests implements EventHandler<TaskRequestEvent> {
    private final TransferQueue<TaskRequestEvent> taskRequestQueue =
        new LinkedTransferQueue<>();

    @Override
    public void handle(TaskRequestEvent event) {
      int qSize = taskRequestQueue.size();
      if (qSize != 0 && qSize % 1000 == 0) {
        LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
      }
      int remCapacity = taskRequestQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn("Very low remaining capacity in the event-queue "
            + "of RMContainerAllocator: " + remCapacity);
      }

      taskRequestQueue.add(event);
    }

    public void getTaskRequests(final Collection<TaskRequestEvent> taskRequests,
                                int num) {
      taskRequestQueue.drainTo(taskRequests, num);
    }

    public int size() {
      return taskRequestQueue.size();
    }
  }

  private class ScheduledRequests {
    private final HashSet<QueryUnitAttemptId> leafTasks = new HashSet<>();
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

      leafTasks.add(event.getAttemptId());
    }

    public int size() {
      return leafTasks.size();
    }

    public void assignToLeafTasks(List<TaskRequestEvent> taskRequests) {
      Iterator<TaskRequestEvent> it = taskRequests.iterator();
      LOG.info("Got task requests " + taskRequests.size());

      TaskRequestEvent taskRequest;
      while (it.hasNext()) {
        taskRequest = it.next();

        QueryUnitAttemptId attemptId = null;
        while (attemptId == null && scheduledRequests.size() > 0) {

          String hostName = taskRequest.getWorkerId().getHostAddress();

          // local allocation
          LinkedList<QueryUnitAttemptId> list = leafTasksHostMapping.get(hostName);
          while(list != null && list.size() > 0) {
            attemptId = list.removeFirst();
            if (leafTasks.contains(attemptId)) {
              leafTasks.remove(attemptId);
              LOG.debug("Assigned based on host match " + hostName);
              break;
            }
          }

          // rack allocation
          if (attemptId == null) {
            String rack = RackResolver.resolve(hostName).getNetworkLocation();
            list = leafTasksRackMapping.get(rack);
            while(list != null && list.size() > 0) {
              attemptId = list.removeFirst();
              if (leafTasks.contains(attemptId)) {
                leafTasks.remove(attemptId);
                LOG.debug("Assigned based on rack match " + rack);
                break;
              }
            }
          }

          // random allocation
          if (attemptId == null) {
            attemptId = leafTasks.iterator().next();
            leafTasks.remove(attemptId);
            LOG.debug("Assigned based on * match");
          }

          QueryUnit task = context.getQuery(attemptId.getQueryId())
              .getSubQuery(attemptId.getSubQueryId()).getQueryUnit(attemptId.getQueryUnitId());
          QueryUnitRequest taskAssign = new QueryUnitRequestImpl(
              attemptId,
              Lists.newArrayList(task.getAllFragments()),
              task.getOutputName(),
              false,
              task.getLogicalPlan().toJSON());
          context.getEventHandler().handle(new TaskAttemptEvent(attemptId,
              TaskAttemptEventType.TA_ASSIGNED));
          taskRequest.getCallback().run(taskAssign.getProto());
        }
      }
    }
  }
}
