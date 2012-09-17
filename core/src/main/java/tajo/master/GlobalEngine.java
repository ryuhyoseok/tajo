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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.QueryId;
import tajo.QueryIdFactory;
import tajo.TajoProtos.QueryState;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableDesc;
import tajo.catalog.TableMeta;
import tajo.catalog.statistics.TableStat;
import tajo.engine.cluster.ClusterManager;
import tajo.engine.exception.EmptyClusterException;
import tajo.engine.exception.IllegalQueryStatusException;
import tajo.engine.exception.NoSuchQueryIdException;
import tajo.engine.exception.UnknownWorkerException;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalOptimizer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.PlanningContext;
import tajo.engine.planner.global.GlobalOptimizer;
import tajo.engine.planner.global.MasterPlan;
import tajo.engine.planner.logical.CreateTableNode;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.logical.LogicalRootNode;
import tajo.master.TajoMaster.MasterContext;
import tajo.master.event.QueryEvent;
import tajo.master.event.QueryEventType;
import tajo.storage.StorageManager;
import tajo.storage.StorageUtil;

import java.io.IOException;

@SuppressWarnings("unchecked")
public class GlobalEngine {
  /** Class Logger */
  private final static Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final MasterContext context;
  private final StorageManager sm;

  private ClusterManager cm;

  private final QueryAnalyzer analyzer;
  private final LogicalPlanner planner;
  private final GlobalPlanner globalPlanner;
  private final GlobalOptimizer globalOptimizer;

  public GlobalEngine(final MasterContext context, final StorageManager sm)
      throws IOException {
    this.context = context;
    this.sm = sm;
    this.cm = context.getClusterManager();

    analyzer = new QueryAnalyzer(context.getCatalog());
    planner = new LogicalPlanner(context.getCatalog());
    globalPlanner = new GlobalPlanner(context.getConf(), context.getCatalog(),
        context.getClusterManager(), sm, context.getEventHandler());
    globalOptimizer = new GlobalOptimizer();
  }

  private String createTable(LogicalRootNode root) throws IOException {
    // create table queries are executed by the master
    CreateTableNode createTable = (CreateTableNode) root.getSubNode();
    TableMeta meta;
    if (createTable.hasOptions()) {
      meta = TCatUtil.newTableMeta(createTable.getSchema(),
          createTable.getStoreType(), createTable.getOptions());
    } else {
      meta = TCatUtil.newTableMeta(createTable.getSchema(),
          createTable.getStoreType());
    }

    long totalSize = 0;
    try {
      totalSize = sm.calculateSize(createTable.getPath());
    } catch (IOException e) {
      LOG.error("Cannot calculate the size of the relation", e);
    }
    TableStat stat = new TableStat();
    stat.setNumBytes(totalSize);
    meta.setStat(stat);

    StorageUtil.writeTableMeta(context.getConf(), createTable.getPath(), meta);
    TableDesc desc = TCatUtil.newTableDesc(createTable.getTableName(), meta,
        createTable.getPath());
    context.getCatalog().addTable(desc);
    return desc.getId();
  }
  
  public String executeQuery(String tql)
      throws InterruptedException, IOException,
      NoSuchQueryIdException, IllegalQueryStatusException,
      UnknownWorkerException, EmptyClusterException {

    LOG.info("TQL: " + tql);
    // parse the query
    PlanningContext planningContext = analyzer.parse(tql);
    LogicalRootNode plan = (LogicalRootNode) createLogicalPlan(planningContext);

    if (plan.getSubNode().getType() == ExprType.CREATE_TABLE) {
      return createTable(plan);
    } else {

      // other queries are executed by workers
      updateFragmentServingInfo(planningContext);

      QueryId queryId = QueryIdFactory.newQueryId();
      MasterPlan masterPlan = createGlobalPlan(queryId, plan);
      Query query = createQuery(queryId, tql, masterPlan);
      startQuery(query);

      while(true) {
        if (query.getState() == QueryState.QUERY_SUCCEEDED
          || query.getState() == QueryState.QUERY_KILLED
          || query.getState() == QueryState.QUERY_FAILED) {
          break;
        }

        Thread.sleep(1000);
      }

      String outDir = sm.getTablePath(masterPlan.getOutputTable()).toString();

      FileSystem fs = FileSystem.get(context.getConf());
      LOG.info(fs.exists(new Path(outDir)));
      for (FileStatus status : fs.listStatus(new Path(outDir, "data"))) {
        System.out.println(status.getPath() + " " + status.getLen());
      }
      return outDir;
    }
  }

  private LogicalNode createLogicalPlan(PlanningContext planningContext)
      throws IOException {

    LogicalNode plan = planner.createPlan(planningContext);
    plan = LogicalOptimizer.optimize(planningContext, plan);
    LogicalNode optimizedPlan = LogicalOptimizer.pushIndex(plan, sm);
    LOG.info("LogicalPlan:\n" + plan);

    return optimizedPlan;
  }

  private MasterPlan createGlobalPlan(QueryId id, LogicalRootNode rootNode)
      throws IOException {
    MasterPlan globalPlan = globalPlanner.build(id, rootNode);
    return globalOptimizer.optimize(globalPlan);
  }

  private Query createQuery(QueryId id, String tql, MasterPlan plan) {
    return new Query(id, tql, context.getEventHandler(), globalPlanner, plan, sm);
  }

  private void startQuery(Query query) {
    context.getAllQueries().put(query.getId(), query);
    context.getEventHandler().handle(new QueryEvent(query.getId(),
        QueryEventType.INIT));
    context.getEventHandler().handle(
        new QueryEvent(query.getId(), QueryEventType.START));
  }

  private void updateFragmentServingInfo(PlanningContext context)
      throws IOException {
    cm.updateOnlineWorker();
    for (String table : context.getParseTree().getAllTableNames()) {
      cm.updateFragmentServingInfo2(table);
    }
  }
}
