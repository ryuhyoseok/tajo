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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import org.apache.hadoop.yarn.util.RackResolver;
import tajo.QueryUnitAttemptId;
import tajo.TajoProtos.TaskAttemptState;
import tajo.catalog.statistics.TableStat;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.master.event.*;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueryUnitAttempt implements EventHandler<TaskAttemptEvent> {

  private static final Log LOG = LogFactory.getLog(QueryUnitAttempt.class);

  private final static int EXPIRE_TIME = 15000;

  private final QueryUnitAttemptId id;
  private final QueryUnit queryUnit;
  final EventHandler eventHandler;

  private String hostName;
  private int expire;

  private final Lock readLock;
  private final Lock writeLock;

  private static final StateMachineFactory
      <QueryUnitAttempt, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      stateMachineFactory = new StateMachineFactory
      <QueryUnitAttempt, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
      (TaskAttemptState.TA_NEW)

      .addTransition(TaskAttemptState.TA_NEW, TaskAttemptState.TA_UNASSIGNED,
          TaskAttemptEventType.TA_SCHEDULE, new RequestScheduleTransition())

      .addTransition(TaskAttemptState.TA_UNASSIGNED, TaskAttemptState.TA_ASSIGNED,
          TaskAttemptEventType.TA_ASSIGNED, new LaunchTransition())

      .addTransition(TaskAttemptState.TA_ASSIGNED,
          EnumSet.of(TaskAttemptState.TA_RUNNING, TaskAttemptState.TA_FAILED,
              TaskAttemptState.TA_KILLED, TaskAttemptState.TA_SUCCEEDED),
          TaskAttemptEventType.TA_UPDATE,
          new StatusUpdateTransition())

      .addTransition(TaskAttemptState.TA_RUNNING,
          EnumSet.of(TaskAttemptState.TA_RUNNING, TaskAttemptState.TA_FAILED,
          TaskAttemptState.TA_KILLED, TaskAttemptState.TA_SUCCEEDED),
          TaskAttemptEventType.TA_UPDATE,
          new StatusUpdateTransition())

      .addTransition(TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptState.TA_SUCCEEDED,
          TaskAttemptEventType.TA_UPDATE)

      .installTopology();

  private final StateMachine<TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
    stateMachine;


  public QueryUnitAttempt(final QueryUnitAttemptId id, final QueryUnit queryUnit,
                          final EventHandler eventHandler) {
    this.id = id;
    this.expire = QueryUnitAttempt.EXPIRE_TIME;
    this.queryUnit = queryUnit;
    this.eventHandler = eventHandler;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
  }

  public TaskAttemptState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  public QueryUnitAttemptId getId() {
    return this.id;
  }

  public boolean isLeafTask() {
    return this.queryUnit.isLeafTask();
  }

  public QueryUnit getQueryUnit() {
    return this.queryUnit;
  }

  public String getHost() {
    return this.hostName;
  }

  public void setHost(String host) {
    this.hostName = host;
  }

  public synchronized void setExpireTime(int expire) {
    this.expire = expire;
  }

  public synchronized void updateExpireTime(int period) {
    this.setExpireTime(this.expire - period);
  }

  public synchronized void resetExpireTime() {
    this.setExpireTime(QueryUnitAttempt.EXPIRE_TIME);
  }

  public int getLeftTime() {
    return this.expire;
  }

  private void updateProgress(TaskStatusProto progress) {
    if (progress.getPartitionsCount() > 0) {
      this.getQueryUnit().setPartitions(progress.getPartitionsList());
    }
    if (progress.hasResultStats()) {
      this.getQueryUnit().setStats(new TableStat(progress.getResultStats()));
    }
  }

  private static class RequestScheduleTransition implements
    SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent> {

    @Override
    public void transition(QueryUnitAttempt taskAttempt,
                           TaskAttemptEvent taskAttemptEvent) {

      if (taskAttempt.isLeafTask()
          && taskAttempt.getQueryUnit().getScanNodes().length == 1) {
        Set<String> racks = new HashSet<>();
        for (String host : taskAttempt.getQueryUnit().getDataLocations()) {
          racks.add(RackResolver.resolve(host).getNetworkLocation());
        }

        taskAttempt.eventHandler.handle(new ContainerAllocatorEvent(
            taskAttempt.getId(), true,
            taskAttempt.getQueryUnit().getDataLocations(),
            racks.toArray(new String[racks.size()]),
            ContainerAllocatorEventType.CONTAINER_REQ));
      } else {
        taskAttempt.eventHandler.handle(new ContainerAllocatorEvent(
            taskAttempt.getId(), false,
            null,
            null,
            ContainerAllocatorEventType.CONTAINER_REQ));
      }
    }
  }

  private static class LaunchTransition
      implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent> {

    @Override
    public void transition(QueryUnitAttempt taskAttempt,
                           TaskAttemptEvent taskAttemptEvent) {
      taskAttempt.eventHandler.handle(
          new TaskTAttemptEvent(taskAttempt.getId(),
              TaskEventType.T_ATTEMPT_LAUNCHED));
    }
  }

  private static class StatusUpdateTransition
      implements MultipleArcTransition<QueryUnitAttempt, TaskAttemptEvent, TaskAttemptState> {

    @Override
    public TaskAttemptState transition(QueryUnitAttempt taskAttempt,
                                       TaskAttemptEvent event) {
      TaskAttemptStatusUpdateEvent updateEvent =
          (TaskAttemptStatusUpdateEvent) event;

      switch (updateEvent.getStatus().getState()) {
        case TA_PENDING:
        case TA_RUNNING:
          return TaskAttemptState.TA_RUNNING;

        case TA_SUCCEEDED:
          taskAttempt.updateProgress(updateEvent.getStatus());
          taskAttempt.eventHandler.handle(new TaskTAttemptEvent(updateEvent.getTaskAttemptId(),
              TaskEventType.T_ATTEMPT_SUCCEEDED));

          return TaskAttemptState.TA_SUCCEEDED;

        case TA_FAILED:
          taskAttempt.eventHandler.handle(
              new TaskTAttemptEvent(updateEvent.getTaskAttemptId(),
              TaskEventType.T_ATTEMPT_FAILED));

          return TaskAttemptState.TA_FAILED;
        default:
          return taskAttempt.getState();
      }
    }
  }

  private static class SucceededTransition
      implements SingleArcTransition<QueryUnitAttempt, TaskAttemptEvent>{
    @Override
    public void transition(QueryUnitAttempt queryUnitAttempt,
                           TaskAttemptEvent taskAttemptEvent) {
      queryUnitAttempt.eventHandler.handle(
          new TaskEvent(queryUnitAttempt.getId().getQueryUnitId(),
              TaskEventType.T_ATTEMPT_FAILED));
    }
  }

  @Override
  public void handle(TaskAttemptEvent event) {
    LOG.info("Processing " + event.getTaskAttemptId() + " of type "
        + event.getType());
    try {
      writeLock.lock();
      TaskAttemptState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        eventHandler.handle(new QueryEvent(getId().getQueryId(),
            QueryEventType.INTERNAL_ERROR));
      }

      //notify the eventhandler of state change
      if (oldState != getState()) {
        LOG.info(id + " TaskAttempt Transitioned from " + oldState + " to "
            + getState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }
}
