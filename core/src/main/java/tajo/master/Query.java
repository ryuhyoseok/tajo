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

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import tajo.QueryId;
import tajo.QueryUnitId;
import tajo.SubQueryId;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.planner.global.MasterPlan;
import tajo.engine.planner.global.QueryUnit;
import tajo.master.event.QueryEvent;
import tajo.master.event.QueryEventType;
import tajo.master.event.SubQueryEvent;
import tajo.master.event.SubQueryEventType;
import tajo.storage.StorageManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Query extends AbstractQuery implements EventHandler<QueryEvent> {
  private static final Log LOG = LogFactory.getLog(Query.class);

  private final QueryId id;
  private String queryStr;
  private Map<SubQueryId, SubQuery> subqueries;
  private QueryStatus status;
  private final EventHandler eventHandler;
  private final StateMachine<QueryStatus, QueryEventType, QueryEvent> stateMachine;
  private final MasterPlan plan;
  private final StorageManager sm;

  private final Lock readLock;
  private final Lock writeLock;

  private static final StateMachineFactory
      <Query,QueryStatus,QueryEventType,QueryEvent> stateMachineFactory =
      new StateMachineFactory<Query, QueryStatus, QueryEventType, QueryEvent>
          (QueryStatus.QUERY_NEW)

      .addTransition(QueryStatus.QUERY_NEW, QueryStatus.QUERY_INITED,
          QueryEventType.QUERY_INIT, new InitTransition())
      .addTransition(QueryStatus.QUERY_INITED, QueryStatus.QUERY_INPROGRESS,
          QueryEventType.QUERY_START, new StartTransition());

  private PriorityQueue<SubQuery> scheduleQueue;

  public Query(final QueryId id, final String queryStr,
               final EventHandler eventHandler,
               final GlobalPlanner planner,
               final MasterPlan plan, final StorageManager sm) {
    this.id = id;
    this.queryStr = queryStr;
    subqueries = Maps.newHashMap();
    this.eventHandler = eventHandler;
    this.plan = plan;
    this.sm = sm;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.scheduleQueue = new PriorityQueue<SubQuery>(1,
        new PriorityComparator());

    stateMachine = stateMachineFactory.make(this);
  }

  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }

  protected StorageManager getStorageManager() {
    return this.sm;
  }

  class PriorityComparator implements Comparator<SubQuery> {
    public PriorityComparator() {

    }

    @Override
    public int compare(SubQuery s1, SubQuery s2) {
      return s1.getPriority().get() - s2.getPriority().get();
    }
  }

  public MasterPlan getPlan() {
    return plan;
  }

  public StateMachine<QueryStatus, QueryEventType, QueryEvent> getStateMachine() {
    return stateMachine;
  }
  
  public void addSubQuery(SubQuery q) {
    subqueries.put(q.getId(), q);
  }
  
  public QueryId getId() {
    return this.id;
  }

  public String getQueryStr() {
    return this.queryStr;
  }

  public Iterator<SubQuery> getSubQueryIterator() {
    return this.subqueries.values().iterator();
  }
  
  public SubQuery getSubQuery(SubQueryId id) {
    return this.subqueries.get(id);
  }
  
  public Collection<SubQuery> getSubQueries() {
    return this.subqueries.values();
  }
  
  public QueryUnit getQueryUnit(QueryUnitId id) {
    return this.getSubQuery(id.getSubQueryId()).getQueryUnit(id);
  }

  public QueryStatus getState() {
    return this.status;
  }

  public void setState(QueryStatus status) {
    this.status = status;
  }

  public int getScheduleQueueSize() {
    return scheduleQueue.size();
  }

  static class InitTransition implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent queryEvent) {
      scheduleSubQueries(query, query.getPlan().getRoot());
      LOG.info("==> Scheduled SubQueries: " + query.getScheduleQueueSize());
    }

    private void scheduleSubQueries(Query query, SubQuery subQuery) {
      int priority;

      if (subQuery.hasChildQuery()) {

        int maxPriority = 0;
        Iterator<SubQuery> it = subQuery.getChildIterator();

        while (it.hasNext()) {
          SubQuery su = it.next();
          scheduleSubQueries(query, su);
          if (su.getPriority().get() > maxPriority) {
            maxPriority = su.getPriority().get();
          }
        }

        priority = maxPriority + 1;

      } else {
        priority = 0;
      }

      subQuery.setPriority(priority);
      // TODO
      query.addSubQuery(subQuery);
      query.schedule(subQuery);
    }
  }

  public static class StartTransition
      implements SingleArcTransition<Query, QueryEvent> {

    @Override
    public void transition(Query query, QueryEvent queryEvent) {
      SubQuery subQuery = query.removeFromScheduleQueue();
      LOG.info("Schedule unit plan: \n" + subQuery.getLogicalPlan());
      subQuery.handle(new SubQueryEvent(subQuery.getId(),
          SubQueryEventType.SQ_INIT));
      subQuery.handle(new SubQueryEvent(subQuery.getId(),
          SubQueryEventType.SQ_START));
    }
  }

  @Override
  public void handle(QueryEvent event) {
    LOG.info("Processing " + event.getQueryId() + " of type " + event.getType());
    try {
    writeLock.lock();
    QueryStatus oldState = getState();
      try {
        getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        eventHandler.handle(new QueryEvent(this.id,
            QueryEventType.INTERNAL_ERROR));
      }

      //notify the eventhandler of state change
      if (oldState != getState()) {
        LOG.info(id + "Job Transitioned from " + oldState + " to "
            + getState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }

  public void schedule(SubQuery subQuery) {
    scheduleQueue.add(subQuery);
  }

  private SubQuery removeFromScheduleQueue() {
    if (scheduleQueue.isEmpty()) {
      return null;
    } else {
      return scheduleQueue.remove();
    }
  }
}
