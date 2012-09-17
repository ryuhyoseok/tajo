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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.TajoTestingUtility;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.proto.CatalogProtos.TableProto;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.datum.DatumFactory;
import tajo.datum.NullDatum;
import tajo.engine.ClientServiceProtos.ExecuteQueryRequest;
import tajo.engine.ClientServiceProtos.ExecuteQueryRespose;
import tajo.storage.*;
import tajo.storage.Scanner;
import tajo.util.FileUtil;
import tajo.worker.Worker;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class TestGlobalEngine {
  private static Log LOG = LogFactory.getLog(TestGlobalEngine.class);

  private static TajoTestingUtility util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static TajoMaster master;
  private static StorageManager sm;

  private static class CompositeKey {
    String deptname;
    int year;

    public CompositeKey(String deptname, int year) {
      this.deptname = deptname;
      this.year = year;
    }

    @Override
    public String toString() {
      return "(" + this.deptname + ", " + year + ")";
    }

    @Override
    public int hashCode() {
      int hashCode = deptname.hashCode();
      return hashCode ^ year;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CompositeKey) {
        CompositeKey k = (CompositeKey) o;
        if (this.deptname.equals(k.deptname) && this.year == k.year) {
          return true;
        }
      }
      return false;
    }
  }

  private String[] query = {
//      "select deptname, sum(score) from score group by deptname having sum(score) > 30",
      "select deptname, year, sum(score) from score group by deptname, year", // 0
      "select deptname from score", // 1
      "select dept.deptname, score.score from dept,score where score.deptname = dept.deptname", // 2
      "create table test (id int, name string) using csv location '/tmp/data' with ('csv.delimiter'='|')", // 3
      "select dept.deptname, score.score from dept,score where score.deptname = dept.deptname and score.score > 50", // 4
      "select deptname, year, sum(score) from score group by cube (deptname, year)" // 5
  };
  private static Map<CompositeKey, Integer> groupbyResult;
  private static Map<CompositeKey, Integer> cubebyResult;
  private static Set<String> scanResult;
  private static Map<String, List<Integer>> joinResult;
  private static Map<String, List<Integer>> selectAfterJoinResult;

  private String tablename;

  @Before
  public void setup() throws Exception {
    util = new TajoTestingUtility();
    util.startMiniCluster(1);
    Thread.sleep(2000);
    master = util.getMiniTajoCluster().getMaster();
    conf = util.getConfiguration();
    sm = master.getStorageManager();

    catalog = master.getCatalog();
    groupbyResult = new HashMap<CompositeKey, Integer>();
    cubebyResult = new HashMap<CompositeKey, Integer>();
    scanResult = new HashSet<String>();
    joinResult = new HashMap<String, List<Integer>>();
    selectAfterJoinResult = new HashMap<String, List<Integer>>();

    Schema scoreSchema = new Schema();
    scoreSchema.addColumn("deptname", DataType.STRING);
    scoreSchema.addColumn("score", DataType.INT);
    scoreSchema.addColumn("year", DataType.INT);
    TableMeta scoreMeta = TCatUtil.newTableMeta(scoreSchema, StoreType.CSV);

    Schema deptSchema = new Schema();
    deptSchema.addColumn("id", DataType.INT);
    deptSchema.addColumn("deptname", DataType.STRING);
    deptSchema.addColumn("since", DataType.INT);
    TableMeta deptMeta = TCatUtil.newTableMeta(deptSchema, StoreType.CSV);

    Appender appender = sm.getTableAppender(scoreMeta, "score");
    int deptSize = 10;
    int tupleNum = 100;
    int allScoreSum = 0;
    Tuple tuple;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(3);
      String id = "test" + (i % deptSize);
      tuple.put(0, DatumFactory.createString(id));          // id
      tuple.put(1, DatumFactory.createInt(i + 1));          // score
      tuple.put(2, DatumFactory.createInt(i + 1900));       // year
      appender.addTuple(tuple);
      scanResult.add(id);
      allScoreSum += i+1;
      CompositeKey compkey = new CompositeKey(id, i+1900);
      if (!groupbyResult.containsKey(compkey)) {
        groupbyResult.put(compkey, i + 1);
      } else {
        int n = groupbyResult.get(compkey);
        groupbyResult.put(compkey, n + i + 1);
      }
      if (!cubebyResult.containsKey(compkey)) {
        cubebyResult.put(compkey, i+1);
      } else {
        cubebyResult.put(compkey, cubebyResult.get(compkey)+i+1);
      }
      CompositeKey idkey = new CompositeKey(id, NullDatum.get().asInt());
      if (!cubebyResult.containsKey(idkey)) {
        cubebyResult.put(idkey, i+1);
      } else {
        cubebyResult.put(idkey, cubebyResult.get(idkey)+i+1);
      }
      CompositeKey yearkey = new CompositeKey(NullDatum.get().asChars(), i+1900);
      if (!cubebyResult.containsKey(yearkey)) {
        cubebyResult.put(yearkey, i+1);
      } else {
        cubebyResult.put(yearkey, cubebyResult.get(yearkey)+i+1);
      }
      if (!joinResult.containsKey(id)) {
        List<Integer> list = new ArrayList<Integer>();
        list.add((i+1));
        joinResult.put(id, list);
      } else {
        joinResult.get(id).add((i+1));
      }

      if (i+1 > 50) {
        if (!selectAfterJoinResult.containsKey(id)) {
          List<Integer> list = new ArrayList<Integer>();
          list.add((i+1));
          selectAfterJoinResult.put(id, list);
        } else {
          selectAfterJoinResult.get(id).add((i+1));
        }
      }
    }
    cubebyResult.put(new CompositeKey(NullDatum.get().asChars(), NullDatum.get().asInt()), allScoreSum);
    appender.close();

    TableDesc score = new TableDescImpl("score", scoreSchema, StoreType.CSV,
        new Options(), new Path(conf.getVar(ConfVars.ENGINE_DATA_DIR), "score"));
    catalog.addTable(score);

    appender = sm.getTableAppender(deptMeta, "dept");
    tupleNum = deptSize;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(3);
      tuple.put(0, DatumFactory.createInt(i+1));
      tuple.put(1, DatumFactory.createString("test"+i));
      tuple.put(2, DatumFactory.createInt(i%1000));
      appender.addTuple(tuple);
    }
    appender.close();

    TableDesc dept = TCatUtil.newTableDesc("dept", deptMeta, sm.getTablePath("dept"));
    catalog.addTable(dept);
  }

  @After
  public void terminate() throws IOException {
    util.shutdownMiniCluster();
  }

  @Test
  public void testCreateTable() throws Exception {
    ExecuteQueryRequest.Builder builder
        = ExecuteQueryRequest.newBuilder();
    builder.setQuery(query[3]);
    ExecuteQueryRespose res = master.executeQuery(builder.build());
    String tablename = res.getPath();
    assertNotNull(tablename);
    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(new Path("/tmp/data/.meta")));
    TableProto proto = TableProto.getDefaultInstance();
    proto = (TableProto) FileUtil.loadProto(conf, new Path("/tmp/data/.meta"),
        proto);
    TableMeta meta = new TableMetaImpl(proto);
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("name", DataType.STRING);
    assertEquals(schema, meta.getSchema());
    assertEquals(StoreType.CSV, meta.getStoreType());
  }

  @Test
  public void testScanQuery() throws Exception {
    ExecuteQueryRequest.Builder builder
        = ExecuteQueryRequest.newBuilder();
    builder.setQuery(query[1]);
    ExecuteQueryRespose res = master.executeQuery(builder.build());
    String tablename = res.getPath();
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    TableMeta meta = sm.getTableMeta(tablename);
    assertNotNull(meta.getStat());
    Tuple tuple;
    String deptname;
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      assertTrue(scanResult.contains(deptname));
    }
  }

  @Test
  public void testGroupbyQuery() throws Exception {
    ExecuteQueryRequest.Builder builder
        = ExecuteQueryRequest.newBuilder();
    builder.setQuery(query[0]);
    ExecuteQueryRespose res = master.executeQuery(builder.build());
    String tablename = res.getPath();
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple;
    String deptname;
    int year;
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      year = tuple.get(1).asInt();
      assertEquals(groupbyResult.get(new CompositeKey(deptname, year)).intValue(),
          tuple.get(2).asInt());
    }
  }

  @Test
  public void testJoin() throws Exception {
    ExecuteQueryRequest.Builder builder
        = ExecuteQueryRequest.newBuilder();
    builder.setQuery(query[2]);
    ExecuteQueryRespose res = master.executeQuery(builder.build());
    String tablename = res.getPath();
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple;
    String deptname;
    Set<Integer> results;
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      results = new HashSet<Integer>(joinResult.get(deptname));
      assertTrue(results.contains(tuple.get(1).asInt()));
    }
  }

  @Test
  public void testSelectAfterJoin() throws Exception {
    ExecuteQueryRequest.Builder builder
        = ExecuteQueryRequest.newBuilder();
    builder.setQuery(query[4]);
    ExecuteQueryRespose res = master.executeQuery(builder.build());
    String tablename = res.getPath();
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple;
    String deptname;
    Set<Integer> results;
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      results = new HashSet<Integer>(selectAfterJoinResult.get(deptname));
      assertTrue(results.contains(tuple.get(1).asInt()));
    }
  }

  //@Test TODO
  public void testCubeby() throws Exception {
    ExecuteQueryRequest.Builder builder
        = ExecuteQueryRequest.newBuilder();
    builder.setQuery(query[5]);
    ExecuteQueryRespose res = master.executeQuery(builder.build());
    String tablename = res.getPath();
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple;
    String deptname;
    int year;
    
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      year = tuple.get(1).asInt();
      CompositeKey key = new CompositeKey(deptname, year);
      int expected = cubebyResult.get(key);
      int value = tuple.get(2).asInt();
      assertEquals(expected, value);
    }
  }

  //  @Test
  public void testFaultTolerant() throws Exception {
    Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
          Worker leaf = util.getMiniTajoCluster().getWorker(0);
          LOG.info(">>> " + leaf.getServerName() + " will be halted!!");
          leaf.shutdown(">>> Aborted! <<<");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    Thread t2 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          ExecuteQueryRequest.Builder builder
              = ExecuteQueryRequest.newBuilder();
          builder.setQuery(query[0]);
          tablename = master.executeQuery(builder.build()).getPath();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    t2.start();
    t1.start();
    t2.join();
    t1.join();
    assertNotNull(tablename);
    Scanner scanner = sm.getTableScanner(tablename);
    Tuple tuple;
    String deptname;
    while ((tuple = scanner.next()) != null) {
      deptname = tuple.get(0).asChars();
      assertEquals(groupbyResult.get(deptname).intValue(), tuple.get(1).asInt());
    }
  }
}
