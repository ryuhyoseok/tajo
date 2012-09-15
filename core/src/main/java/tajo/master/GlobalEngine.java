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
import org.apache.hadoop.yarn.event.EventHandler;
import tajo.QueryId;
import tajo.QueryIdFactory;
import tajo.catalog.CatalogService;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableDesc;
import tajo.catalog.TableMeta;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.engine.cluster.ClusterManager;
import tajo.engine.cluster.QueryManager;
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
import tajo.engine.planner.logical.*;
import tajo.master.TajoMaster.MasterContext;
import tajo.master.event.QueryEvent;
import tajo.master.event.QueryEventType;
import tajo.master.event.QueryState;
import tajo.storage.StorageManager;
import tajo.storage.StorageUtil;

import java.io.IOException;

public class GlobalEngine implements EngineService {
  private final static Log LOG = LogFactory.getLog(GlobalEngine.class);

  private final MasterContext context;
  private final TajoConf conf;
  private final CatalogService catalog;
  private final StorageManager sm;

  private QueryManager qm;
  private ClusterManager cm;

  private final EventHandler eventHandler;

  public GlobalEngine(final MasterContext context,
                      final QueryManager qm, final StorageManager sm)
      throws IOException {
    this.context = context;
    this.conf = context.getConf();
    this.catalog = context.getCatalog();
    this.qm = qm;
    this.sm = sm;
    this.cm = context.getClusterManager();
    this.eventHandler = context.getEventHandler();
  }

  private LogicalNode buildLogicalPlan(PlanningContext context) throws IOException {
    // build the logical plan
    LogicalPlanner planner = new LogicalPlanner(catalog);
    LogicalNode plan = planner.createPlan(context);
    plan = LogicalOptimizer.optimize(context, plan);
    plan = LogicalOptimizer.pushIndex(plan, sm);
    LOG.info("* logical plan:\n" + plan);

    return plan;
  }

  private String executeCreateTable(LogicalRootNode root) throws IOException {
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

    StorageUtil.writeTableMeta(conf, createTable.getPath(), meta);
    TableDesc desc = TCatUtil.newTableDesc(createTable.getTableName(), meta,
        createTable.getPath());
    catalog.addTable(desc);
    return desc.getId();
  }
  
  public String executeQuery(String tql)
      throws InterruptedException, IOException,
      NoSuchQueryIdException, IllegalQueryStatusException,
      UnknownWorkerException, EmptyClusterException {
    LOG.info("* issued query: " + tql);
    QueryAnalyzer analyzer = new QueryAnalyzer(catalog);
    PlanningContext planningContext = analyzer.parse(tql);
    LogicalRootNode plan = (LogicalRootNode) buildLogicalPlan(planningContext);

    if (plan.getSubNode().getType() == ExprType.CREATE_TABLE) {
      return executeCreateTable(plan);
    } else {
      boolean hasStoreNode = false;
      if (plan.getSubNode().getType() == ExprType.STORE) {
        hasStoreNode = true;
      }
      // other queries are executed by workers
      updateFragmentServingInfo(planningContext);

      QueryId queryId = QueryIdFactory.newQueryId();
      // build the master plan
      GlobalPlanner globalPlanner =
          new GlobalPlanner(conf, this.sm, this.qm, this.catalog, eventHandler, cm);
      GlobalOptimizer globalOptimizer = new GlobalOptimizer();
      MasterPlan globalPlan = globalPlanner.build(queryId, plan);
      globalPlan = globalOptimizer.optimize(globalPlan.getRoot());

      Query query = new Query(queryId, tql, eventHandler, globalPlanner, globalPlan, sm);
      context.getAllQueries().put(queryId, query);
      eventHandler.handle(new QueryEvent(query.getId(),
          QueryEventType.INIT));

      eventHandler.handle(new QueryEvent(query.getId(), QueryEventType.START));

//      query.setState(QueryStatus.QUERY_INPROGRESS);
//      SubQueryExecutor executor = new SubQueryExecutor(conf,
//          wc, globalPlanner, cm, qm, sm, globalPlan, eventHandler);
//      executor.start();
//      executor.join();

      //finalizeQuery(query);

      while(true) {
        if (query.getState() == QueryState.SUCCEEDED
          || query.getState() == QueryState.FAILED
          || query.getState() == QueryState.KILLED) {
          break;
        }

        Thread.sleep(1000);
      }

      if (hasStoreNode) {
        // create table queries are executed by the master
        StoreTableNode stn = (StoreTableNode) plan.getSubNode();
        TableDesc desc = TCatUtil.newTableDesc(stn.getTableName(),
            sm.getTableMeta(globalPlan.getRoot().getOutputName()),
            sm.getTablePath(globalPlan.getRoot().getOutputName()));
        catalog.addTable(desc);
      }

      String outDir = sm.getTablePath(globalPlan.getRoot().getOutputName()).toString();

      FileSystem fs = FileSystem.get(conf);
      LOG.info(fs.exists(new Path(outDir)));
      for (FileStatus status : fs.listStatus(new Path(outDir, "data"))) {
        System.out.println(status.getPath() + " " + status.getLen());
      }
      return outDir;
    }
  }

  public void finalizeQuery(Query query)
      throws IllegalQueryStatusException, UnknownWorkerException {
    QueryStatus status = updateQueryStatus(query);
    switch (status) {
      case QUERY_FINISHED:
        LOG.info("Query " + query.getId() + " is finished.");
        break;
      case QUERY_ABORTED:
        LOG.info("Query " + query.getId() + " is aborted!!");
        break;
      case QUERY_KILLED:
        LOG.info("Query " + query.getId() + " is killed!!");
        break;
      default:
        throw new IllegalQueryStatusException(
            "Illegal final status of query " +
                query.getId() + ": " + status);
    }
  }

  private QueryStatus updateQueryStatus(Query query) {
    int i = 0, size = query.getSubQueries().size();
    QueryStatus queryStatus = QueryStatus.QUERY_ABORTED;
    for (SubQuery sq : query.getSubQueries()) {
      if (sq.getState() != SubQueryState.SUCCEEDED) {
        break;
      }
      ++i;
    }
    if (i > 0 && i == size) {
      queryStatus = QueryStatus.QUERY_FINISHED;
    }
    query.setState(queryStatus);
    return queryStatus;
  }

  private void updateFragmentServingInfo(PlanningContext context)
      throws IOException {
    cm.updateOnlineWorker();
    for (String table : context.getParseTree().getAllTableNames()) {
      cm.updateFragmentServingInfo2(table);
    }
  }

  @Override
  public void init() throws IOException {

  }

  /*
   * (non-Javadoc)
   * 
   * @see EngineService#shutdown()
   */
  @Override
  public void shutdown() throws IOException {
    LOG.info(GlobalEngine.class.getName() + " is being stopped");
  }
}
