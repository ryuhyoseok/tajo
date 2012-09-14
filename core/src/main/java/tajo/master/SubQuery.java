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
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import tajo.QueryUnitId;
import tajo.SubQueryId;
import tajo.catalog.Schema;
import tajo.catalog.statistics.TableStat;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.planner.global.QueryUnit;
import tajo.engine.planner.logical.*;
import tajo.master.event.SubQueryEvent;
import tajo.master.event.SubQueryEventType;

import java.util.*;


public class SubQuery extends AbstractQuery {

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
  private QueryStatus status;

  private StateMachineFactory<SubQueryExecutor, SubQueryStatus,
      SubQueryEventType, SubQueryEvent> stateMachineFactory =
      new StateMachineFactory <SubQueryExecutor, SubQueryStatus,
          SubQueryEventType, SubQueryEvent> (SubQueryStatus.NEW)

      .addTransition(SubQueryStatus.NEW, SubQueryStatus.RUNNING,
          SubQueryEventType.SQ_START, new SubQueryStartTransition())
      .addTransition(SubQueryStatus.RUNNING,
          EnumSet.of(SubQueryStatus.RUNNING, SubQueryStatus.SUCCEEDED,
              SubQueryStatus.FAILED),
          SubQueryEventType.SQ_TASK_COMPLETED, new TaskCompletedTransition())
      .addTransition(SubQueryStatus.RUNNING, SubQueryStatus.FAILED,
          SubQueryEventType.SQ_ABORT, new InternalErrorTransition());


  
  public SubQuery(SubQueryId id) {
    this.id = id;
    prevs = new HashMap<ScanNode, SubQuery>();
    scanlist = new ArrayList<ScanNode>();
    hasJoinPlan = false;
    hasUnionPlan = false;
  }
  
  public void setOutputType(PARTITION_TYPE type) {
    this.outputType = type;
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

  public void setStatus(QueryStatus status) {
    this.status = status;
  }

  public QueryStatus getStatus() {
    return this.status;
  }

  class SubQueryStartTransition implements
      SingleArcTransition<SubQueryExecutor, SubQueryEvent> {

    @Override
    public void transition(SubQueryExecutor subQueryExecutor,
                           SubQueryEvent subQueryEvent) {

    }
  }

  class TaskCompletedTransition implements
      MultipleArcTransition<SubQueryExecutor, SubQueryEvent, SubQueryStatus> {


    @Override
    public SubQueryStatus transition(SubQueryExecutor subQueryExecutor,
                                     SubQueryEvent event) {
      return null;
    }
  }

  class InternalErrorTransition
      implements SingleArcTransition<SubQueryExecutor, SubQueryEvent> {

    @Override
    public void transition(SubQueryExecutor subQueryExecutor,
                           SubQueryEvent subQueryEvent) {
    }
  }
}
