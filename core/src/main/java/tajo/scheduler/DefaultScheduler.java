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

package tajo.scheduler;

import com.google.protobuf.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import tajo.QueryUnitAttemptId;
import tajo.engine.MasterWorkerProtos.CommandRequestProto;
import tajo.engine.MasterWorkerProtos.QueryUnitRequestProto;
import tajo.engine.planner.logical.ScanNode;
import tajo.engine.query.QueryUnitRequestImpl;
import tajo.ipc.protocolrecords.Fragment;
import tajo.ipc.protocolrecords.QueryUnitRequest;
import tajo.master.QueryUnitAttempt;
import tajo.master.TajoMaster.MasterContext;
import tajo.master.cluster.ClusterManager;
import tajo.master.cluster.FragmentServingInfo;
import tajo.master.event.TaskAttemptEvent;
import tajo.master.event.TaskAttemptEventType;
import tajo.scheduler.event.ScheduleTaskEvent;
import tajo.scheduler.event.SchedulerEvent;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DefaultScheduler extends AbstractService
    implements EventHandler<SchedulerEvent> {

  private static final Log LOG = LogFactory.getLog(DefaultScheduler.class);

  private final MasterContext context;
  protected Thread eventHandlingThread;
  private volatile boolean stopped;
  private final Object lock = new Object();

  private BlockingQueue<SchedulerEvent> eventQueue
      = new LinkedBlockingQueue<SchedulerEvent>();

  public DefaultScheduler(MasterContext context) {
    super(DefaultScheduler.class.getName());
    this.context = context;
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
  }

  @Override
  public void start() {
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        SchedulerEvent event = null;
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();

            //context.getClusterManager().updateOnlineWorker();
            //context.getClusterManager().resetResourceInfo();

            switch (event.getType()) {
              case SCHEDULE:
                QueryUnitAttemptId attemptId = ((ScheduleTaskEvent) event).
                    getTaskAttemptId();
                QueryUnitAttempt attempt =
                    context.getQuery(attemptId.getQueryId()).
                        getSubQuery(attemptId.getSubQueryId())
                    .getQueryUnit(attemptId.getQueryUnitId()).getAttempt(attemptId);

                List<Fragment> fragList = new ArrayList<Fragment>();
                for (ScanNode scan : attempt.getQueryUnit().getScanNodes()) {
                  fragList.add(
                      attempt.getQueryUnit().getFragment(scan.getTableId()));
                }
                attempt.setHost(selectWorker(fragList));
                QueryUnitRequest request = createQueryUnitRequest(attempt,
                    fragList);
                printQueryUnitRequestInfo(attempt, request);
                context.getEventHandler().handle(
                    new TaskAttemptEvent(attemptId,
                        TaskAttemptEventType.TA_ASSIGNED));
                requestToWC(attempt.getHost(), request.getProto());
                break;
            }

          } catch (Exception e) {
            LOG.error(e);
          }
        }
      }
    });
    eventHandlingThread.start();
    super.start();
  }

  @Override
  public void stop() {
    stopped = true;
    super.stop();
  }


  @Override
  public void handle(SchedulerEvent schedulerEvent) {
    try {
      eventQueue.put(schedulerEvent);
    } catch (InterruptedException e) {
      LOG.error(e);
    }
  }

  private String selectWorker(List<Fragment> fragList) {
    ClusterManager cm = null;
    if (cm.remainFreeResource()) {
      FragmentServingInfo info;
      ClusterManager.WorkerResource wr;
      if (fragList.size() == 1) {
        info = cm.getServingInfo(fragList.get(0));
      } else {
        // TODO: to be improved
        info = cm.getServingInfo(fragList.get(0));
      }
      if (info == null) {
        String host = cm.getNextFreeHost();
        cm.allocateSlot(host);
        return host;
      }
      if (cm.getOnlineWorkers().containsKey(info.getPrimaryHost())) {
        List<String> workers = cm.getOnlineWorkers().get(info.getPrimaryHost());
        for (String worker : workers) {
          wr = cm.getResource(worker);
          if (wr.hasFreeResource()) {
            cm.allocateSlot(worker);
            return worker;
          }
        }
      }
      String backup;
      while ((backup=info.getNextBackupHost()) != null) {
        if (cm.getOnlineWorkers().containsKey(backup)) {
          List<String>workers = cm.getOnlineWorkers().get(backup);
          for (String worker : workers) {
            wr = cm.getResource(worker);
            if (wr.hasFreeResource()) {
              cm.allocateSlot(worker);
              return worker;
            }
          }
        }
      }
      backup = cm.getNextFreeHost();
      cm.allocateSlot(backup);
      return backup;
    } else {
      return null;
    }
  }

  private QueryUnitRequest createQueryUnitRequest(QueryUnitAttempt q,
                                                  List<Fragment> fragList) {
    QueryUnitRequest request = new QueryUnitRequestImpl(q.getId(), fragList,
        q.getQueryUnit().getOutputName(), false,
        q.getQueryUnit().getLogicalPlan().toJSON());

    if (q.getQueryUnit().getStoreTableNode().isLocal()) {
      request.setInterQuery();
    }

    for (ScanNode scan : q.getQueryUnit().getScanNodes()) {
      Collection<URI> fetches = q.getQueryUnit().getFetch(scan);
      if (fetches != null) {
        for (URI fetch : fetches) {
          request.addFetch(scan.getTableId(), fetch);
        }
      }
    }
    return request;
  }

  private boolean requestToWC(String host, Message proto) throws Exception {
    boolean result = true;
    try {
      if (proto instanceof QueryUnitRequestProto) {
        QueryUnitRequestProto request = (QueryUnitRequestProto) proto;
        //context.getWorkerCommunicator().requestQueryUnit(host, request);

        context.getEventHandler().handle(
            new TaskAttemptEvent(new QueryUnitAttemptId(request.getId()),
                TaskAttemptEventType.TA_LAUNCHED));
      } else if (proto instanceof CommandRequestProto) {
        //context.getWorkerCommunicator().requestCommand(host, (CommandRequestProto) proto);
      }
    } catch (Exception e) {
      result = false;
    }
    return result;
  }

  private void printQueryUnitRequestInfo(QueryUnitAttempt q,
                                         QueryUnitRequest request) {
    LOG.info("QueryUnitRequest " + request.getId() + " is sent to "
        + (q.getHost()));
  }
}
