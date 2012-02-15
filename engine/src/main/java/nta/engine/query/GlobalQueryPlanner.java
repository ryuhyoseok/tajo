package nta.engine.query;

/**
 * @author jihoon
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import nta.catalog.CatalogService;
import nta.catalog.Column;
import nta.catalog.HostInfo;
import nta.catalog.Schema;
import nta.catalog.TableDescImpl;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.distexec.DistPlan;
import nta.engine.QueryIdFactory;
import nta.engine.SubQueryId;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryBlock.FromTable;
import nta.engine.parser.QueryBlock.SortKey;
import nta.engine.planner.PlannerUtil;
// import nta.engine.planner.global.GlobalOptimizer;
import nta.engine.planner.global.GlobalQueryPlan;
import nta.engine.planner.global.MappingType;
import nta.engine.planner.global.OptimizationPlan;
import nta.engine.planner.global.QueryStep;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.QueryUnitGraph;
import nta.engine.planner.global.QueryStep.Phase;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.UnaryNode;
import nta.storage.StorageManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class GlobalQueryPlanner {
  private static Log LOG = LogFactory.getLog(GlobalQueryPlanner.class);

  // private GlobalOptimizer optimizer;
  private CatalogService catalog;
  private StorageManager sm;

  public GlobalQueryPlanner(CatalogService catalog, StorageManager sm)
      throws IOException {
    this.catalog = catalog;
    // this.optimizer = new GlobalOptimizer();
    this.sm = sm;

    String[] plans = { "merge", "local" };
    int[] nodeNum = { 1, 1 };
    MappingType[] mappings = { MappingType.ONE_TO_ONE, MappingType.ONE_TO_ONE };
    OptimizationPlan plan = new OptimizationPlan(2, plans, nodeNum, mappings);
    // this.optimizer.addOptimizationPlan(ExprType.GROUP_BY, plan);
    // this.optimizer.addOptimizationPlan(ExprType.SORT, plan);
  }

  public GlobalQueryPlan build(SubQueryId subQueryId, LogicalNode logicalPlan)
      throws IOException {
    // convert 2-phase plan
    LogicalNode tp = convertTo2Phase(logicalPlan);
    // make query graph
    GlobalQueryPlan globalPlan = convertToGlobalPlan(subQueryId, tp);

    // QueryUnitGraph localized = localize(logicalPlan);
    // QueryUnitGraph optimized = optimize(localized);
    // GlobalQueryPlan plan = breakup2(subQueryId, optimized);
    return globalPlan;
  }

  private LogicalNode convertTo2Phase(LogicalNode logicalPlan) {
    // TODO
    GroupbyNode node = (GroupbyNode) PlannerUtil.findTopNode(logicalPlan,
        ExprType.GROUP_BY);
    if (node != null)
      PlannerUtil.transformTwoPhase(node);

    return logicalPlan;
  }

  public QueryStep localize(boolean first, QueryStep step, int n)
      throws IOException {
    QueryStep newStep = new QueryStep(step.getSubQueryId());
    QueryUnit unit = step.getQuery(0);
    QueryUnit[] units;
    Fragment[] frags;
    unit.buildLogicalPlan();
    Path tablePath;
    if (first) {
      tablePath = catalog.getTableDesc(unit.getInputName()).getPath();
    } else {
      tablePath = sm.getTablePath(unit.getInputName());
    }

    if (step.getPhase() == Phase.LOCAL || step.getPhase() == Phase.MAP) {
      if (step.getPhase() == Phase.MAP) {
        Column[] cols = unit.getStoreTableNode().getPartitionKeys();
        unit.getStoreTableNode().setPartitions(cols, n);
      }
      frags = sm.split(tablePath);
      unit.setFragments(frags);
      units = splitQueryUnitByRoundRobin(unit, n);
    } else {
      LOG.info("그오오오오오오오오옹오오오오오 " + tablePath);
      FileStatus[] files = sm.getFileSystem().listStatus(tablePath);
      for (FileStatus f : files) {
        frags = sm.split(f.getPath());
        unit.addFragments(frags);
      }
      units = splitQueryUnitByHash(unit, n);
    }
    newStep.addQueries(units);
    return newStep;
  }

  private GlobalQueryPlan convertToGlobalPlan(SubQueryId subQueryId,
      LogicalNode logicalPlan) {
    QueryUnit parent = null, cur = null;
    LogicalNode op = logicalPlan;
    ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();

    // push nodes to the stack
    while (op instanceof UnaryNode) {
      s.add(op);
      op = ((UnaryNode) op).getSubNode();
    }
    s.add(op); // scan

    cur = new QueryUnit(QueryIdFactory.newQueryUnitId());
    QueryStep step = new QueryStep(subQueryId);
    QueryStep prevStep = null;
    step.setPhase(Phase.LOCAL);
    GlobalQueryPlan plan = new GlobalQueryPlan();

    while (!s.isEmpty()) {
      op = s.remove(s.size() - 1);
      switch (op.getType()) {
      case SORT:
      case GROUP_BY:
        if (cur.getUnaryNode() != null) {
          parent = cur;
          if (parent.getScanNode() == null) {
            addScan(prevStep.getId(), parent);
          }
          if (parent.getStoreTableNode() == null) {
            addStore(step.getId(), parent, step.getPhase());
          }
          step.addQuery(parent);
          plan.addQueryStep(step);
          cur = new QueryUnit(QueryIdFactory.newQueryUnitId());
          prevStep = step;
          step = new QueryStep(subQueryId);
          step.setPhase(Phase.MERGE);
          parent.addNextQuery(cur);
          cur.addPrevQuery(parent);
        } else {
          step.setPhase(Phase.MAP);
        }
        UnaryNode unary = (UnaryNode) op;
        cur.setUnaryNode(unary);
        break;
      case STORE:
        StoreTableNode store = (StoreTableNode) op;
        cur.setStoreNode(store);
        break;
      case SCAN:
        ScanNode scan = (ScanNode) op;
        cur.setScanNode(scan);
        break;
      default:
        break;
      }
    }

    if (cur.getScanNode() == null) {
      addScan(prevStep.getId(), cur);
    }
    if (cur.getStoreTableNode() == null) {
      addStore(step.getId(), cur, step.getPhase());
    }
    step.addQuery(cur);
    plan.addQueryStep(step);

    return plan;
  }

  //
  // private void localizeQuery(QueryUnit query) {
  // LogicalNode op = query.getOp();
  // QueryUnit[] localizedQueries;
  // Set<QueryUnit> prevQuerySet = query.getNextQueries();
  // Set<QueryUnit> nextQuerySet = query.getPrevQueries();
  //
  // switch (op.getType()) {
  // case SCAN:
  // case SELECTION:
  // case PROJECTION:
  // localizedQueries = localizeSimpleQuery(query);
  // // if prev exist, it is still not be localized
  // if (prevQuerySet.size() > 0) {
  // QueryUnit prev = prevQuerySet.iterator().next();
  // prev.removePrevQuery(query);
  // for (QueryUnit localize : localizedQueries) {
  // prev.addPrevQuery(localize);
  // }
  // }
  //
  // // if next exist..?
  //
  // // for (UnitQuery next: nextQuerySet) {
  // // next.removePrevQuery(query);
  // // for (UnitQuery localize: localizedQueries) {
  // // next.addPrevQuery(localize);
  // // }
  // // }-
  // break;
  // case JOIN:
  // break;
  // case GROUP_BY:
  // break;
  // case SORT:
  // break;
  // case SET_UNION:
  // break;
  // case SET_DIFF:
  // break;
  // case SET_INTERSECT:
  // break;
  // }
  // }
  //
  // private QueryUnitGraph optimize(QueryUnitGraph graph) {
  // return optimizer.optimize(graph);
  // }
  //
  // class LevelLabeledUnitQuery {
  // int level;
  // QueryUnit query;q.getStoreTableNode().setPartitions(cols, splitSize);
  //
  // public LevelLabeledUnitQuery(int level, QueryUnit query) {
  // this.level = level;
  // this.query = query;
  // }
  // }
  //
  // private GlobalQueryPlan breakup2(SubQueryId subQueryId, GlobalQueryPlan
  // graph) {-
  // Set<QueryUnit> nextQuerySet;
  // LevelLabeledUnitQuery e;
  // int curLevel = 0;
  // GlobalQueryPlan globalPlan = new GlobalQueryPlan();
  // ArrayList<LevelLabeledUnitQuery> s = new
  // ArrayList<GlobalQueryPlanner.LevelLabeledUnitQuery>();
  // QueryStep queryStep = new QueryStep(subQueryId);
  //
  // s = tr(s, new LevelLabeledUnitQuery(0, graph.getRoot()));
  //
  // DistPlan plan;
  // while (!s.isEmpty()) {
  // e = s.remove(s.size() - 1);
  // if (curLevel != e.level) {
  // if (queryStep.size() > 0) {
  // globalPlan.addQueryStep(queryStep);
  // queryStep = new QueryStep(subQueryId);
  // }
  // curLevel = e.level;
  // }
  //
  // switch (e.query.getOp().getType()) {
  // case GROUP_BY:
  // case SORT:
  // addScan(e.level, e.query);
  // addStore(queryStep.getId(), e.query);
  // plan = e.query.getDistPlan();
  // if (plan.getPlanName().equals("local")) {
  // plan.setOutputNum(e.query.getPrevQueries().size());
  // }
  // queryStep.addQuery(e.query);q.getStoreTableNode().setPartitions(cols, splitSize);
  // break;-
  // case SCAN:
  // nextQuerySet = e.query.getNextQueries();
  // if (nextQuerySet.size() == 0 ||
  // nextQuerySet.iterator().next().getOp().getType() == ExprType.ROOT) {
  // addStore(queryStep.getId(), e.query);
  // queryStep.addQuery(e.query);
  // }
  // break;
  // default:
  // break;
  // }
  //
  // }q.getStoreTableNode().setPartitions(cols, splitSize);
  //
  // if (queryStep.size() > 0) {
  // globalPlan.addQueryStep(queryStep);
  // }
  //
  // return globalPlan;
  // }

  // private ArrayList<LevelLabeledUnitQuery> tr(
  // ArrayList<LevelLabeledUnitQuery> cur, LevelLabeledUnitQuery q) {
  //
  // cur.add(q);
  // if (q.query.getPrevQueries().size() > 0) {
  // Set<QueryUnit> nexts = q.query.getPrevQueries();
  // for (QueryUnit next : nexts) {
  // cur = tr(cur, new LevelLabeledUnitQuery(q.level+1, next));
  // }
  // }
  // return cur;
  // }

  private QueryUnit[] splitQueryUnitByRoundRobin(QueryUnit q, int splitSize) {
    QueryUnit[] splitted = splitQueryUnitExceptFragments(q, splitSize > q
        .getFragments().size() ? q.getFragments().size() : splitSize);
    assignFragmentsByRoundRobin(splitted, q.getFragments());
    reconnectQueryUnitGraph(q, splitted);
    return splitted;
  }

  private QueryUnit[] splitQueryUnitByHash(QueryUnit q, int splitSize) {
    Collection<List<Fragment>> hashed = hashFragments(q.getFragments());
    QueryUnit[] splitted = splitQueryUnitExceptFragments(q,
        splitSize > hashed.size() ? hashed.size() : splitSize);
    int i = 0;
    for (List<Fragment> frags : hashed) {
      splitted[i++].setFragments(frags.toArray(new Fragment[frags.size()]));
      if (i == splitted.length) {
        i = 0;
      }
    }

    reconnectQueryUnitGraph(q, splitted);

    return splitted;
  }

  private QueryUnit[] splitQueryUnitExceptFragments(QueryUnit q, int size) {
    int i = 0;
    QueryUnit[] splitted = new QueryUnit[size];
    for (i = 0; i < splitted.length; i++) {
      splitted[i] = q.cloneExceptFragments();
    }
    return splitted;
  }

  private void reconnectQueryUnitGraph(QueryUnit orig, QueryUnit[] news) {
    int i;
    Set<QueryUnit> nexts = orig.getNextQueries();
    for (QueryUnit next : nexts) {
      next.removePrevQuery(orig);
      for (i = 0; i < news.length; i++) {
        next.addPrevQuery(news[i]);
      }
    }
    Set<QueryUnit> prevs = orig.getPrevQueries();
    for (QueryUnit prev : prevs) {
      prev.removeNextQuery(orig);
      for (i = 0; i < news.length; i++) {
        prev.addNextQuery(news[i]);
      }
    }
  }

  private void assignFragmentsByRoundRobin(QueryUnit[] units,
      List<Fragment> frags) {
    int i = 0;
    for (Fragment f : frags) {
      units[i].addFragment(f);
      if (++i == units.length) {
        i = 0;
      }
    }
  }

  private Collection<List<Fragment>> hashFragments(List<Fragment> frags) {
    SortedMap<String, List<Fragment>> hashed = new TreeMap<String, List<Fragment>>();
    int i = 0;
    for (Fragment f : frags) {
      if (hashed.containsKey(f.getPath().getName())) {
        hashed.get(f.getPath().getName()).add(f);
      } else {
        List<Fragment> list = new ArrayList<Fragment>();
        list.add(f);
        hashed.put(f.getPath().getName(), list);
      }
    }

    return hashed.values();
  }

  private void addStore(String outputName, QueryUnit q, Phase phase) {
    StoreTableNode store = new StoreTableNode(outputName);
    if (phase == Phase.MAP) {
      UnaryNode unary = q.getUnaryNode();
      if (unary.getType() == ExprType.GROUP_BY) {
        GroupbyNode groupby = (GroupbyNode) unary;
        // partition number will be set later
        store.setPartitions(groupby.getGroupingColumns(), -1);
      } else if (unary.getType() == ExprType.SORT) {
        SortNode sort = (SortNode) unary;
        SortKey[] keys = sort.getSortKeys();
        Column[] cols = new Column[keys.length];
        for (int i = 0; i < keys.length; i++) {
          cols[i] = keys[i].getSortKey();
        }
        store.setPartitions(cols, -1);
      }
    }
    if (q.getUnaryNode() != null) {
      store.setInputSchema(q.getUnaryNode().getOutputSchema());
    } else {
      store.setInputSchema(q.getScanNode().getOutputSchema());
    }
    store.setOutputSchema(store.getInputSchema());
    q.setStoreNode(store);
  }

  private void addScan(String inputName, QueryUnit q) {
    Schema schema = null;
    if (q.getUnaryNode() != null) {
      schema = q.getUnaryNode().getInputSchema();
    } else {
      schema = q.getStoreTableNode().getInputSchema();
    }
    ScanNode scan = new ScanNode(new FromTable(new TableDescImpl(inputName,
        schema, StoreType.CSV)));
    scan.setInputSchema(schema);
    scan.setOutputSchema(schema);
    scan.setTargetList(schema);
    q.setScanNode(scan);
  }

  // private GlobalQueryPlan breakup(QueryUnitGraph graph) {
  // Set<QueryUnit> nextQuerySet;
  // LevelLabeledUnitQuery e;
  // int curLevel = 0;
  // GlobalQueryPlan globalPlan = new GlobalQueryPlan();
  // ArrayList<LevelLabeledUnitQuery> s = new
  // ArrayList<GlobalQueryPlanner.LevelLabeledUnitQuery>();
  // QueryStep queryStep = new QueryStep();
  // s.add(new LevelLabeledUnitQuery(0, graph.getRoot()));
  //
  // while (!s.isEmpty()) {
  // e = s.remove(0);
  // nextQuerySet = e.query.getPrevQueries();
  // if (e.query.getOp().getType() != ExprType.ROOT) {
  // // remove root operator
  //
  // if (curLevel != e.level) {
  // if (queryStep.size() > 0) {
  // globalPlan.addQueryStep(queryStep);
  // queryStep = new QueryStep();
  // }
  // curLevel = e.level;
  // }
  //
  // // if an n-ary node or a leaf node is visited, break up the graph
  // // if ((nextQuerySet.size() > 1) ||
  // // nextQuerySet.size() == 0) {
  // queryStep.addQuery(e.query);
  // // } else {
  // //
  // // }
  // }
  // nextQuerySet = e.query.getPrevQueries();
  // for (QueryUnit t : nextQuerySet) {
  // s.add(new LevelLabeledUnitQuery(e.level + 1, t));
  // }
  // }
  //
  // if (queryStep.size() > 0) {
  // globalPlan.addQueryStep(queryStep);
  // }
  //
  // return globalPlan;
  // }

  // private QueryUnit[] localizeSimpleQuery(QueryUnit query) {
  // ScanNode op = (ScanNode) query.getOp();
  // List<HostInfo> fragments = catalog.getHostByTable(op.getTableId());
  // QueryUnit[] localized = new QueryUnit[fragments.size()];
  //
  // for (int i = 0; i < localized.length; i++) {
  // // TODO: make tableInfo from tablets.get(i)
  // localized[i] = new QueryUnit(QueryIdFactory.newQueryUnitId(),
  // query.getOp(), query.getAnnotation());
  // localized[i].setInputName(op.getTableId());
  //
  // // TODO: keep the alias
  // // TODO: set startKey and endKey of TableInfo
  // localized[i].addFragment(fragments.get(i).getFragment());
  // }
  //
  // return localized;
  // }
  //
  // private QueryUnit[] localizedCompexQuery(QueryUnit query) {
  // Set<QueryUnit> nextQuerySet = query.getPrevQueries();
  // // TODO: localize시킬 unit query의 수를 결정하는 것이 필요
  // // TODO: 한 스텝으로 끝나지 않을 수도 있음
  // QueryUnit[] localized = new QueryUnit[nextQuerySet.size()];
  // for (int i = 0; i < localized.length; i++) {
  // localized[i] = new QueryUnit(QueryIdFactory.newQueryUnitId(),
  // query.getOp(), query.getAnnotation());
  // }
  // return localized;
  // }
  //
  // private String selectHost(LogicalNode plan) {
  // // select the host which serves most tablets in the oplist
  // return null;
  // }
}
