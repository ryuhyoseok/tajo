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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;
import tajo.QueryUnitId;
import tajo.SubQueryId;
import tajo.catalog.Schema;
import tajo.catalog.statistics.TableStat;
import tajo.engine.cluster.ClusterManager;
import tajo.engine.planner.PlannerUtil;
import tajo.engine.planner.global.QueryUnit;
import tajo.engine.planner.logical.*;
import tajo.master.event.SubQueryCompletedEvent;
import tajo.master.event.SubQueryEvent;
import tajo.master.event.SubQueryEventType;
import tajo.storage.StorageManager;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class SubQuery implements EventHandler<SubQueryEvent> {

  private static final Log LOG = LogFactory.getLog(SubQuery.class);

  public enum PARTITION_TYPE {
    /** for hash partitioning */
    HASH,
    LIST,
    /** for map-side join */
    BROADCAST,
    /** for range partitioning */
    RANGE
  }

  private SubQueryId id;
  private LogicalNode plan = null;
  private StoreTableNode store = null;
  private List<ScanNode> scanlist = null;
  private SubQuery next;
  private Map<ScanNode, SubQuery> prevs;
  private PARTITION_TYPE outputType;
  private QueryUnit[] queryUnits;
  private boolean hasJoinPlan;
  private boolean hasUnionPlan;
  private Priority priority;
  private TableStat stats;
  private final EventHandler eventHandler;
  private final StorageManager sm;
  private final GlobalPlanner planner;
  private final ClusterManager cm;

  private StateMachine<SubQueryState, SubQueryEventType, SubQueryEvent> stateMachine;

  private StateMachineFactory<SubQuery, SubQueryState,
      SubQueryEventType, SubQueryEvent> stateMachineFactory =
      new StateMachineFactory <SubQuery, SubQueryState,
          SubQueryEventType, SubQueryEvent> (SubQueryState.NEW)

      .addTransition(SubQueryState.NEW,
          EnumSet.of(SubQueryState.INIT, SubQueryState.FAILED),
          SubQueryEventType.SQ_INIT, new InitTransition())

      .addTransition(SubQueryState.INIT, SubQueryState.RUNNING,
          SubQueryEventType.SQ_START, new StartTransition())

      .addTransition(SubQueryState.RUNNING,
          EnumSet.of(SubQueryState.RUNNING, SubQueryState.SUCCEEDED,
              SubQueryState.FAILED),
          SubQueryEventType.SQ_TASK_COMPLETED, new TaskCompletedTransition())

      .addTransition(SubQueryState.RUNNING, SubQueryState.FAILED,
          SubQueryEventType.SQ_ABORT, new InternalErrorTransition());

  private final Lock readLock;
  private final Lock writeLock;
  
  public SubQuery(SubQueryId id, EventHandler eventHandler, StorageManager sm,
                  GlobalPlanner planner, ClusterManager cm) {
    this.id = id;
    prevs = new HashMap<ScanNode, SubQuery>();
    scanlist = new ArrayList<ScanNode>();
    hasJoinPlan = false;
    hasUnionPlan = false;
    this.eventHandler = eventHandler;
    this.sm = sm;
    this.planner = planner;
    this.cm = cm;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();


    stateMachine = stateMachineFactory.make(this);
  }
  
  public void setOutputType(PARTITION_TYPE type) {
    this.outputType = type;
  }

  public GlobalPlanner getPlanner() {
    return planner;
  }

  public ClusterManager getClusterManager() {
    return cm;
  }

  
  public void setLogicalPlan(LogicalNode plan) {
    hasJoinPlan = false;
    Preconditions.checkArgument(plan.getType() == ExprType.STORE
        || plan.getType() == ExprType.CREATE_INDEX);

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
        if (binary.getType() == ExprType.JOIN) {
          hasJoinPlan = true;
        } else if (binary.getType() == ExprType.UNION) {
          hasUnionPlan = true;
        }
        s.add(s.size(), binary.getOuterNode());
        s.add(s.size(), binary.getInnerNode());
      } else if (node instanceof ScanNode) {
        scanlist.add((ScanNode)node);
      }
    }
  }

  public void abortSubQuery(SubQueryState finalState) {
    // TODO -
    // - committer.abortSubQuery(...)
    // - record SubQuery Finish Time
    // - CleanUp Tasks
    // - Record History

    eventHandler.handle(new SubQueryCompletedEvent(getId(), finalState));
  }

  public StateMachine<SubQueryState, SubQueryEventType, SubQueryEvent> getStateMachine() {
    return this.stateMachine;
  }

  public boolean hasJoinPlan() {
    return this.hasJoinPlan;
  }

  public boolean hasUnionPlan() {
    return this.hasUnionPlan;
  }
  
  public void setParentQuery(SubQuery next) {
    this.next = next;
  }
  
  public void addChildQuery(ScanNode prevscan, SubQuery prev) {
    prevs.put(prevscan, prev);
  }
  
  public void addChildQueries(Map<ScanNode, SubQuery> prevs) {
    this.prevs.putAll(prevs);
  }
  
  public void setQueryUnits(QueryUnit[] queryUnits) {
    this.queryUnits = queryUnits;
  }
  
  public void removeChildQuery(ScanNode scan) {
    scanlist.remove(scan);
    this.prevs.remove(scan);
  }
  
  public void removeScan(ScanNode scan) {
    scanlist.remove(scan);
  }
  
  public void addScan(ScanNode scan) {
    scanlist.add(scan);
  }

  public void setPriority(Priority priority) {
    this.priority = priority;
  }

  public void setPriority(int priority) {
    if (this.priority == null) {
      this.priority = new Priority(priority);
    }
  }

  public StorageManager getStorageManager() {
    return sm;
  }

  public void setStats(TableStat stat) {
    this.stats = stat;
  }
  
  public SubQuery getParentQuery() {
    return this.next;
  }
  
  public boolean hasChildQuery() {
    return !this.prevs.isEmpty();
  }
  
  public Iterator<SubQuery> getChildIterator() {
    return this.prevs.values().iterator();
  }
  
  public Collection<SubQuery> getChildQueries() {
    return this.prevs.values();
  }
  
  public Map<ScanNode, SubQuery> getChildMaps() {
    return this.prevs;
  }
  
  public SubQuery getChildQuery(ScanNode prevscan) {
    return this.prevs.get(prevscan);
  }
  
  public String getOutputName() {
    return this.store.getTableName();
  }
  
  public PARTITION_TYPE getOutputType() {
    return this.outputType;
  }
  
  public Schema getOutputSchema() {
    return this.store.getOutSchema();
  }
  
  public StoreTableNode getStoreTableNode() {
    return this.store;
  }
  
  public ScanNode[] getScanNodes() {
    return this.scanlist.toArray(new ScanNode[scanlist.size()]);
  }
  
  public LogicalNode getLogicalPlan() {
    return this.plan;
  }
  
  public SubQueryId getId() {
    return this.id;
  }
  
  public QueryUnit[] getQueryUnits() {
    return this.queryUnits;
  }
  
  public QueryUnit getQueryUnit(QueryUnitId qid) {
    for (QueryUnit unit : queryUnits) {
      if (unit.getId().equals(qid)) {
        return unit;
      }
    }
    return null;
  }

  public Priority getPriority() {
    return this.priority;
  }

  public TableStat getStats() {
    return this.stats;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.id);
/*    sb.append(" plan: " + plan.toString());
    sb.append("next: " + next + " prevs:");
    Iterator<SubQuery> it = getChildIterator();
    while (it.hasNext()) {
      sb.append(" " + it.next());
    }*/
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof SubQuery) {
      SubQuery other = (SubQuery)o;
      return this.id.equals(other.getId());
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.id.hashCode();
  }
  
  public int compareTo(SubQuery other) {
    return this.id.compareTo(other.id);
  }

  public SubQueryState getState() {
    return this.stateMachine.getCurrentState();
  }

  static class InitTransition implements
      MultipleArcTransition<SubQuery, SubQueryEvent, SubQueryState> {

    @Override
    public SubQueryState transition(SubQuery subQuery,
                           SubQueryEvent subQueryEvent) {
      try {
        initOutputDir(subQuery.getStorageManager(), subQuery.getOutputName(),
            subQuery.getOutputType());

        int numTasks = getTaskNum(subQuery);
        QueryUnit[] units = subQuery.getPlanner().localize(subQuery, numTasks);
        LOG.info("Create " + units.length + " Tasks");
        return  SubQueryState.INIT;
      } catch (Exception e) {
        LOG.warn("SubQuery (" + subQuery.getId() + ") failed", e);
        return SubQueryState.FAILED;
      }
    }

    private void initOutputDir(StorageManager sm, String outputName,
                               PARTITION_TYPE type)
        throws IOException {
      switch (type) {
        case HASH:
          Path tablePath = sm.getTablePath(outputName);
          sm.getFileSystem().mkdirs(tablePath);
          LOG.info("Table path " + sm.getTablePath(outputName).toString()
              + " is initialized for " + outputName);
          break;
        case RANGE: // TODO - to be improved

        default:
          if (!sm.getFileSystem().exists(sm.getTablePath(outputName))) {
            sm.initTableBase(null, outputName);
            LOG.info("Table path " + sm.getTablePath(outputName).toString()
                + " is initialized for " + outputName);
          }
      }
    }

    private int getTaskNum(SubQuery subQuery) {
      int numTasks;
      GroupbyNode grpNode = (GroupbyNode) PlannerUtil.findTopNode(
          subQuery.getLogicalPlan(), ExprType.GROUP_BY);
      if (subQuery.getParentQuery() == null && grpNode != null
          && grpNode.getGroupingColumns().length == 0) {
        numTasks = 1;
      } else {
        numTasks = subQuery.getClusterManager().getOnlineWorkers().size();
      }
      return numTasks;
    }
  }



  class StartTransition implements
      SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery,
                           SubQueryEvent subQueryEvent) {

    }
  }

  class TaskCompletedTransition implements
      MultipleArcTransition<SubQuery, SubQueryEvent, SubQueryState> {


    @Override
    public SubQueryState transition(SubQuery subQuery,
                                     SubQueryEvent event) {
      return null;
    }
  }

  class InternalErrorTransition
      implements SingleArcTransition<SubQuery, SubQueryEvent> {

    @Override
    public void transition(SubQuery subQuery,
                           SubQueryEvent subQueryEvent) {
    }
  }

  @Override
  public void handle(SubQueryEvent event) {
    LOG.info("Processing " + event.getSubQueryId() + " of type " + event.getType());
    try {
      writeLock.lock();
      SubQueryState oldState = getState();
      try {
        getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        eventHandler.handle(new SubQueryEvent(this.id,
            SubQueryEventType.SQ_INTERNAL_ERROR));
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
}
