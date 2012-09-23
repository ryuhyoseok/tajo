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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import tajo.QueryIdFactory;
import tajo.QueryUnitAttemptId;
import tajo.QueryUnitId;
import tajo.catalog.Schema;
import tajo.catalog.statistics.TableStat;
import tajo.engine.MasterWorkerProtos.Partition;
import tajo.ipc.protocolrecords.Fragment;
import tajo.engine.planner.logical.*;
import tajo.master.event.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueryUnit implements EventHandler<TaskEvent> {
  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(QueryUnit.class);

	private QueryUnitId taskId;
  private EventHandler eventHandler;
	private StoreTableNode store = null;
	private LogicalNode plan = null;
	private List<ScanNode> scan;
	
	private Map<String, Fragment> fragMap;
	private Map<String, Set<URI>> fetchMap;
	
  private List<Partition> partitions;
	private TableStat stats;

  private Map<QueryUnitAttemptId, QueryUnitAttempt> attempts;
  private final int maxAttempts = 3;
  private Integer lastAttemptId;

  private QueryUnitAttemptId successfulTaskAttempt;

  private int failedAttempts;
  private int finishedAttempts;//finish are total of success, failed and killed

  private static final StateMachineFactory
      <QueryUnit, TaskState, TaskEventType, TaskEvent> stateMachineFactory =
      new StateMachineFactory
          <QueryUnit, TaskState, TaskEventType, TaskEvent>(TaskState.NEW)

      .addTransition(TaskState.NEW, TaskState.SCHEDULED,
          TaskEventType.T_SCHEDULE, new InitialScheduleTransition())

       .addTransition(TaskState.SCHEDULED, TaskState.RUNNING,
           TaskEventType.T_ATTEMPT_LAUNCHED)

       .addTransition(TaskState.RUNNING, TaskState.SUCCEEDED,
           TaskEventType.T_ATTEMPT_SUCCEEDED, new AttemptSucceededTransition())

        .addTransition(TaskState.RUNNING,
            EnumSet.of(TaskState.RUNNING, TaskState.FAILED),
            TaskEventType.T_ATTEMPT_FAILED, new AttemptFailedTransition())

      .installTopology();
  private final StateMachine<TaskState, TaskEventType, TaskEvent> stateMachine;


  private final Lock readLock;
  private final Lock writeLock;

	public QueryUnit(QueryUnitId id, EventHandler eventHandler) {
		this.taskId = id;
    this.eventHandler = eventHandler;
		scan = new ArrayList<ScanNode>();
    fetchMap = Maps.newHashMap();
    fragMap = Maps.newHashMap();
    partitions = new ArrayList<Partition>();
    attempts = Collections.emptyMap();
    lastAttemptId = -1;
    failedAttempts = 0;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    stateMachine = stateMachineFactory.make(this);
	}

  public TaskState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }
	
	public void setLogicalPlan(LogicalNode plan) {
    Preconditions.checkArgument(plan.getType() == ExprType.STORE ||
        plan.getType() == ExprType.CREATE_INDEX);
    
	  this.plan = plan;
	  if (plan instanceof StoreTableNode) {
      store = (StoreTableNode) plan;      
    } else {
      store = (StoreTableNode) ((IndexWriteNode)plan).getSubNode();
    }
	  LogicalNode node = plan;
	  ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();
	  s.add(node);
	  while (!s.isEmpty()) {
	    node = s.remove(s.size()-1);
	    if (node instanceof UnaryNode) {
	      UnaryNode unary = (UnaryNode) node;
	      s.add(s.size(), unary.getSubNode());
	    } else if (node instanceof BinaryNode) {
	      BinaryNode binary = (BinaryNode) node;
	      s.add(s.size(), binary.getOuterNode());
	      s.add(s.size(), binary.getInnerNode());
	    } else if (node instanceof ScanNode) {
	      scan.add((ScanNode)node);
	    }
	  }
	}

  public void setFragment(String tableId, Fragment fragment) {
    this.fragMap.put(tableId, fragment);
  }
	
	public void addFetch(String tableId, String uri) throws URISyntaxException {
	  this.addFetch(tableId, new URI(uri));
	}
	
	public void addFetch(String tableId, URI uri) {
	  Set<URI> uris;
	  if (fetchMap.containsKey(tableId)) {
	    uris = fetchMap.get(tableId);
	  } else {
	    uris = Sets.newHashSet();
	  }
	  uris.add(uri);
    fetchMap.put(tableId, uris);
	}
	
	public void addFetches(String tableId, List<URI> urilist) {
	  Set<URI> uris;
    if (fetchMap.containsKey(tableId)) {
      uris = fetchMap.get(tableId);
    } else {
      uris = Sets.newHashSet();
    }
    uris.addAll(urilist);
    fetchMap.put(tableId, uris);
	}
	
	public void setFetches(Map<String, Set<URI>> fetches) {
	  this.fetchMap.clear();
	  this.fetchMap.putAll(fetches);
	}
	
  public Fragment getFragment(String tableId) {
    return this.fragMap.get(tableId);
  }

  public Collection<Fragment> getAllFragments() {
    return fragMap.values();
  }
	
	public LogicalNode getLogicalPlan() {
	  return this.plan;
	}
	
	public QueryUnitId getId() {
		return taskId;
	}
	
	public Collection<URI> getFetchHosts(String tableId) {
	  return fetchMap.get(tableId);
	}
	
	public Collection<Set<URI>> getFetches() {
	  return fetchMap.values();
	}
	
	public Collection<URI> getFetch(ScanNode scan) {
	  return this.fetchMap.get(scan.getTableId());
	}

	public String getOutputName() {
		return this.store.getTableName();
	}
	
	public Schema getOutputSchema() {
	  return this.store.getOutSchema();
	}
	
	public StoreTableNode getStoreTableNode() {
	  return this.store;
	}
	
	public ScanNode[] getScanNodes() {
	  return this.scan.toArray(new ScanNode[scan.size()]);
	}
	
	@Override
	public String toString() {
		String str = new String(plan.getType() + " \n");
		for (Entry<String, Fragment> e : fragMap.entrySet()) {
		  str += e.getKey() + " : ";
      str += e.getValue() + " ";
		}
		for (Entry<String, Set<URI>> e : fetchMap.entrySet()) {
      str += e.getKey() + " : ";
      for (URI t : e.getValue()) {
        str += t + " ";
      }
    }
		
		return str;
	}
	
	public void setStats(TableStat stats) {
	  this.stats = stats;
	}
	
	public void setPartitions(List<Partition> partitions) {
	  this.partitions = Collections.unmodifiableList(partitions);
	}
	
	public TableStat getStats() {
	  return this.stats;
	}
	
	public List<Partition> getPartitions() {
	  return this.partitions;
	}
	
	public int getPartitionNum() {
	  return this.partitions.size();
	}

  public QueryUnitAttempt newAttempt() {
    QueryUnitAttempt attempt = new QueryUnitAttempt(
        QueryIdFactory.newQueryUnitAttemptId(this.getId(),
            ++lastAttemptId), this, eventHandler);
    return attempt;
  }

  public QueryUnitAttempt getAttempt(QueryUnitAttemptId attemptId) {
    return attempts.get(attemptId);
  }

  public QueryUnitAttempt getAttempt(int attempt) {
    return this.attempts.get(new QueryUnitAttemptId(this.getId(), attempt));
  }

  public QueryUnitAttempt getLastAttempt() {
    return this.attempts.get(this.lastAttemptId);
  }

  public int getRetryCount () {
    return this.lastAttemptId;
  }

  private static class InitialScheduleTransition implements
    SingleArcTransition<QueryUnit, TaskEvent> {

    @Override
    public void transition(QueryUnit task, TaskEvent taskEvent) {
      task.addAndScheduleAttempt();
    }
  }

  // This is always called in the Write Lock
  private void addAndScheduleAttempt() {
    QueryUnitAttempt attempt = newAttempt();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created attempt " + attempt.getId());
    }
    switch (attempts.size()) {
      case 0:
        attempts = Collections.singletonMap(attempt.getId(), attempt);
        break;

      case 1:
        Map<QueryUnitAttemptId, QueryUnitAttempt> newAttempts
            = new LinkedHashMap<QueryUnitAttemptId, QueryUnitAttempt>(3);
        newAttempts.putAll(attempts);
        attempts = newAttempts;
        attempts.put(attempt.getId(), attempt);
        break;

      default:
        attempts.put(attempt.getId(), attempt);
        break;
    }

    eventHandler.handle(new TaskAttemptEvent(attempt.getId(),
        TaskAttemptEventType.TA_SCHEDULE));
  }

  private static class AttemptSucceededTransition
      implements SingleArcTransition<QueryUnit, TaskEvent>{

    @Override
    public void transition(QueryUnit task,
                           TaskEvent event) {
      TaskTAttemptEvent attemptEvent = (TaskTAttemptEvent) event;
      task.successfulTaskAttempt = attemptEvent.getTaskAttemptId();
      task.eventHandler.handle(new SubQueryTaskEvent(event.getTaskId(),
          SubQueryEventType.SQ_TASK_COMPLETED));
    }
  }

  private static class AttemptFailedTransition implements
    MultipleArcTransition<QueryUnit, TaskEvent, TaskState> {

    @Override
    public TaskState transition(QueryUnit task, TaskEvent taskEvent) {
      TaskTAttemptEvent attemptEvent = (TaskTAttemptEvent) taskEvent;
      task.failedAttempts++;

      QueryUnitAttempt attempt = task.getAttempt(attemptEvent.getTaskAttemptId());

      task.finishedAttempts++;

      if (task.failedAttempts < task.maxAttempts) {
        if (task.successfulTaskAttempt == null) {
          task.addAndScheduleAttempt();
        }
      } else {
        task.eventHandler.handle(
            new SubQueryTaskEvent(task.getId(), SubQueryEventType.SQ_FAILED));
        return TaskState.FAILED;
      }

      return task.getState();
    }
  }

  @Override
  public void handle(TaskEvent event) {
    LOG.info("Processing " + event.getTaskId() + " of type "
        + event.getType());
    try {
      writeLock.lock();
      TaskState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        eventHandler.handle(new QueryEvent(getId().getQueryId(),
            QueryEventType.INTERNAL_ERROR));
      }

      //notify the eventhandler of state change
      if (oldState != getState()) {
        LOG.info(taskId + " Task Transitioned from " + oldState + " to "
            + getState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }
}
