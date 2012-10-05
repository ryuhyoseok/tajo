package tajo.cluster;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.TajoTestingUtility;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.datum.DatumFactory;
import tajo.ipc.protocolrecords.Fragment;
import tajo.master.TajoMaster;
import tajo.master.cluster.ClusterManager;
import tajo.master.cluster.ClusterManager.DiskInfo;
import tajo.master.cluster.ClusterManager.WorkerInfo;
import tajo.master.cluster.FragmentServingInfo;
import tajo.master.cluster.WorkerCommunicator;
import tajo.storage.*;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class TestClusterManager {
  static final Log LOG = LogFactory.getLog(TestClusterManager.class);
  
  private static ClusterManager cm;
  private static TajoTestingUtility util;
  private static WorkerCommunicator wc;
  private Collection<List<String>> workersCollection;
  private static CatalogService local;
  private static TajoMaster master;

  final static int CLUST_NUM = 4;
  final static int tbNum = 5;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingUtility();
    util.startMiniCluster(CLUST_NUM);
    Thread.sleep(4000);

    master = util.getMiniTajoCluster().getMaster();
    assertNotNull(master);
    wc = null;//master.getWorkerCommunicator();
    cm = null;//master.getClusterManager();
    assertNotNull(wc);
    assertNotNull(cm);
    
    cm.updateOnlineWorker();
    cm.resetResourceInfo();

    local = util.getMiniTajoCluster().getMaster()
        .getCatalog();

    int i, j;
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT);
    schema.addColumn("age", DataType.INT);
    schema.addColumn("name", DataType.STRING);

    TableMeta meta;

    Random random = new Random();
    int tupleNum;
    StorageManager sm = master.getStorageManager();
    Tuple t;

    for (i = 0; i < tbNum; i++) {
      String tbname = "test" + i;
      meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
      meta.putOption(CSVFile2.DELIMITER, ",");
      Appender appender = sm.getTableAppender(meta, tbname);

      tupleNum = random.nextInt(49) + 10000001;
      for (j = 0; j < tupleNum; j++) {
        t = new VTuple(3);
        t.put(0, DatumFactory.createInt(1));
        t.put(1, DatumFactory.createInt(29));
        t.put(2, DatumFactory.createString("jihoon"));
        appender.addTuple(t);
      }
      appender.close();

      TableDesc desc = new TableDescImpl(tbname, meta,
          sm.getTablePath(tbname));
      local.addTable(desc);
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    util.shutdownMiniCluster();
  }

  @Test
  public void testGetOnlineWorker() throws Exception {
    int i = 0;
    for (List<String> workers : cm.getOnlineWorkers().values()) {
      i += workers.size();
    }
    assertEquals(i, CLUST_NUM);
  }

  @Test
  public void testUpdateFragmentServingInfo2() throws IOException {
    ClusterManager cm = null;//master.getClusterManager();
    StorageManager sm = master.getStorageManager();
    int fragNum = 0;
    for (int i = 0; i < tbNum; i++) {
      cm.updateFragmentServingInfo2("test"+i);
      TableDesc desc = local.getTableDesc("test"+i);
      fragNum += sm.split(desc.getId()).length;
    }

    Map<Fragment, FragmentServingInfo> map = cm.getServingInfoMap();
    assertEquals(fragNum, map.size());
    for (FragmentServingInfo info : map.values()) {
      assertEquals(1, info.getHostNum());
    }
  }

  @Test
  public void testNextFreeHost() {
    ClusterManager cm = null;//master.getClusterManager();
    ClusterManager.WorkerResource wr;
    Set<String> onlineWorkers = Sets.newHashSet();
    for (int i = 0; i < 4; i++) {

      onlineWorkers.clear();

      for (List<String> workers : cm.getOnlineWorkers().values()) {
        onlineWorkers.addAll(workers);
      }

      for (int j = 0; j < CLUST_NUM; j++) {
        String host = cm.getNextFreeHost();
        cm.allocateSlot(host);
        assertTrue(onlineWorkers.contains(host));
        onlineWorkers.remove(host);
      }
    }
  }
}
