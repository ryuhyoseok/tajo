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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.zookeeper.KeeperException;
import tajo.NConstants;
import tajo.QueryIdFactory;
import tajo.catalog.*;
import tajo.catalog.exception.AlreadyExistsTableException;
import tajo.catalog.exception.NoSuchTableException;
import tajo.catalog.proto.CatalogProtos.TableDescProto;
import tajo.catalog.statistics.TableStat;
import tajo.client.ClientService;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.ClientServiceProtos.*;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.engine.cluster.*;
import tajo.engine.cluster.event.WorkerEventType;
import tajo.rpc.NettyRpc;
import tajo.rpc.NettyRpcServer;
import tajo.rpc.RemoteException;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;
import tajo.storage.StorageManager;
import tajo.storage.StorageUtil;
import tajo.webapp.StaticHttpServer;
import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkServer;
import tajo.zookeeper.ZkUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

public class TajoMaster extends CompositeService implements ClientService {
  /**
   * Priority of the JobHistoryServer shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(TajoMaster.class);

  private TajoConf conf;
  private FileSystem defaultFS;

  private String clientServiceAddr;
  private ZkClient zkClient;
  private ZkServer zkServer = null;

  private Path basePath;
  private Path dataPath;

  private CatalogService catalog;
  private StorageManager storeManager;
  private GlobalEngine queryEngine;
  private WorkerCommunicator wc;
  private ClusterManager cm;
  private WorkerListener wl;
  private QueryManager qm;
  private AsyncDispatcher dispatcher;

  private InetSocketAddress clientServiceBindAddr;
  //private RPC.Server clientServiceServer;
  private NettyRpcServer server;

  private WorkerTracker tracker;

  //Web Server
  private StaticHttpServer webServer;

  public TajoMaster() throws Exception {
    super(TajoMaster.class.getName());
  }

  @Override
  public void init(Configuration _conf) {
    this.conf = (TajoConf) _conf;

    try {
      webServer = StaticHttpServer.getInstance(this ,"admin", null, 8080 ,
          true, null, conf, null);
      webServer.start();

      QueryIdFactory.reset();

      // Get the tajo base dir
      this.basePath = new Path(conf.getVar(ConfVars.ENGINE_BASE_DIR));
      LOG.info("Base dir is set " + basePath);
      // Get default DFS uri from the base dir
      this.defaultFS = basePath.getFileSystem(conf);
      LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");

      if (!defaultFS.exists(basePath)) {
        defaultFS.mkdirs(basePath);
        LOG.info("Tajo Base dir (" + basePath + ") is created.");
      }

      this.dataPath = new Path(conf.getVar(ConfVars.ENGINE_DATA_DIR));
      LOG.info("Tajo data dir is set " + dataPath);
      if (!defaultFS.exists(dataPath)) {
        defaultFS.mkdirs(dataPath);
        LOG.info("Data dir (" + dataPath + ") is created");
      }

      this.dispatcher = new AsyncDispatcher();
      addIfService(dispatcher);

      this.storeManager = new StorageManager(conf);

      // The below is some mode-dependent codes
      // If tajo is local mode
      final boolean mode = conf.getBoolVar(ConfVars.CLUSTER_DISTRIBUTED);
      if (!mode) {
        LOG.info("Enabled Pseudo Distributed Mode");
        conf.setVar(ConfVars.ZOOKEEPER_ADDRESS, "127.0.0.1:2181");
        this.zkServer = new ZkServer(conf);
        this.zkServer.start();


        // TODO - When the RPC framework supports all methods of the catalog
        // server, the below comments should be eliminated.
        // this.catalog = new LocalCatalog(conf);
      } else { // if tajo is distributed mode
        LOG.info("Enabled Distributed Mode");
        // connect to the catalog server
        // this.catalog = new CatalogClient(conf);
      }
      // This is temporal solution of the above problem.
      this.catalog = new LocalCatalog(conf);
      this.qm = new QueryManager();

      // connect the zkserver
      this.zkClient = new ZkClient(conf);

      this.wl = new WorkerListener(conf, qm, this);

      // Setup RPC server
      // Get the master address
      LOG.info(TajoMaster.class.getSimpleName() + " is bind to "
          + wl.getAddress());
      this.conf.setVar(TajoConf.ConfVars.MASTER_ADDRESS, wl.getAddress());

      String confClientServiceAddr = conf.getVar(ConfVars.CLIENT_SERVICE_ADDRESS);
      InetSocketAddress initIsa = NetUtils.createSocketAddr(confClientServiceAddr);
      this.server =
          NettyRpc
              .getProtoParamRpcServer(this,
                  ClientService.class, initIsa);

      this.server.start();
      this.clientServiceBindAddr = this.server.getBindAddress();
      this.clientServiceAddr = clientServiceBindAddr.getHostName() + ":" +
          clientServiceBindAddr.getPort();
      LOG.info("Tajo client service master is bind to " + this.clientServiceAddr);
      this.conf.setVar(ConfVars.CLIENT_SERVICE_ADDRESS, this.clientServiceAddr);

      becomeMaster();
      tracker = new WorkerTracker(zkClient);
      tracker.start();

      this.wc = new WorkerCommunicator(zkClient, tracker);
      this.wc.start();
      this.cm = new ClusterManager(conf, wc, tracker, catalog,
          dispatcher.getEventHandler());
      this.dispatcher.register(WorkerEventType.class, this.cm);

      this.queryEngine = new GlobalEngine(conf,
          catalog, storeManager, wc, qm, cm, dispatcher.getEventHandler());
      this.queryEngine.init();

      this.wl.start();
    } catch (Exception e) {

    }

    super.init(conf);
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  private void becomeMaster() throws IOException, KeeperException,
      InterruptedException {
    ZkUtil.createPersistentNodeIfNotExist(zkClient, NConstants.ZNODE_BASE);
    ZkUtil.upsertEphemeralNode(zkClient, NConstants.ZNODE_MASTER,
        wl.getAddress().getBytes());
    ZkUtil.createPersistentNodeIfNotExist(zkClient,
        NConstants.ZNODE_LEAFSERVERS);
    ZkUtil.createPersistentNodeIfNotExist(zkClient, NConstants.ZNODE_QUERIES);
    ZkUtil.upsertEphemeralNode(zkClient, NConstants.ZNODE_CLIENTSERVICE,
        clientServiceAddr.getBytes());
  }

  @Override
  public void start() {
    LOG.info("TajoMaster startup");
    super.start();
  }

  @Override
  public void stop() {
    if (wc != null) {
      this.wc.close();
    }
    this.wl.shutdown();
    tracker.close();
    this.server.shutdown();
    if (zkServer != null) {
      zkServer.shutdown();
    }

    try {
      webServer.stop();
    } catch (Exception e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    super.stop();
    LOG.info("TajoMaster main thread exiting");
  }

  public EventHandler getEventHandler() {
    return dispatcher.getEventHandler();
  }

  public String getMasterServerName() {
    return this.wl.getAddress();
  }

  public String getClientServiceServerName() {
    return this.clientServiceAddr;
  }

  public InetSocketAddress getRpcServerAddr() {
    return this.clientServiceBindAddr;
  }

  public boolean isMasterRunning() {
    return getServiceState() == STATE.STARTED;
  }

  public List<String> getOnlineServer() throws KeeperException,
      InterruptedException {
    return zkClient.getChildren(NConstants.ZNODE_LEAFSERVERS);
  }

  public CatalogService getCatalog() {
    return this.catalog;
  }

  public WorkerCommunicator getWorkerCommunicator() {
    return wc;
  }

  public ClusterManager getClusterManager() {
    return cm;
  }

  public QueryManager getQueryManager() {
    return this.qm;
  }

  public StorageManager getStorageManager() {
    return this.storeManager;
  }

  public WorkerTracker getTracker() {
    return tracker;
  }

  // TODO - to be improved
  public Collection<TaskStatusProto> getProgressQueries() {
    return this.qm.getAllProgresses();
  }

  /////////////////////////////////////////////////////////////////////////////
  // ClientService
  /////////////////////////////////////////////////////////////////////////////
  @Override
  public ExecuteQueryRespose executeQuery(ExecuteQueryRequest query) throws RemoteException {
    String path;
    long elapsed;
    try {
      long start = System.currentTimeMillis();
      path = queryEngine.executeQuery(query.getQuery());
      long end = System.currentTimeMillis();
      elapsed = end - start;
    } catch (Exception e) {
      throw new RemoteException(e);
    }

    LOG.info("Query execution time: " + elapsed);
    ExecuteQueryRespose.Builder build = ExecuteQueryRespose.newBuilder();
    build.setPath(path);
    build.setResponseTime(elapsed);
    return build.build();
  }

  @Override
  public AttachTableResponse attachTable(AttachTableRequest request)
      throws RemoteException {
    if (catalog.existsTable(request.getName()))
      throw new AlreadyExistsTableException(request.getName());

    Path path = new Path(request.getPath());

    LOG.info(path.toUri());

    TableMeta meta;
    try {
      meta = TableUtil.getTableMeta(conf, path);
    } catch (IOException e) {
      throw new RemoteException(e);
    }

    if (meta.getStat() == null) {
      long totalSize = 0;
      try {
        totalSize = calculateSize(new Path(path, "data"));
      } catch (IOException e) {
        LOG.error("Cannot calculate the size of the relation", e);
      }

      meta = new TableMetaImpl(meta.getProto());
      TableStat stat = new TableStat();
      stat.setNumBytes(totalSize);
      meta.setStat(stat);
    }

    TableDesc desc = new TableDescImpl(request.getName(), meta, path);
    catalog.addTable(desc);
    LOG.info("Table " + desc.getId() + " is attached (" + meta.getStat().getNumBytes() + ")");

    return AttachTableResponse.newBuilder().
        setDesc((TableDescProto) desc.getProto()).build();
  }

  @Override
  public void detachTable(StringProto name) throws RemoteException {
    if (!catalog.existsTable(name.getValue())) {
      throw new NoSuchTableException(name.getValue());
    }

    catalog.deleteTable(name.getValue());
    LOG.info("Table " + name + " is detached.");
  }

  private long calculateSize(Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    long totalSize = 0;
    for (FileStatus status : fs.listStatus(path)) {
      totalSize += status.getLen();
    }

    return totalSize;
  }

  @Override
  public CreateTableResponse createTable(CreateTableRequest request)
      throws RemoteException {
    if (catalog.existsTable(request.getName()))
      throw new AlreadyExistsTableException(request.getName());

    Path path = new Path(request.getPath());
    LOG.info(path.toUri());

    long totalSize = 0;
    try {
      totalSize = calculateSize(new Path(path, "data"));
    } catch (IOException e) {
      LOG.error("Cannot calculate the size of the relation", e);
    }

    TableMeta meta = new TableMetaImpl(request.getMeta());
    TableStat stat = new TableStat();
    stat.setNumBytes(totalSize);
    meta.setStat(stat);

    TableDesc desc = new TableDescImpl(request.getName(),meta, path);
    try {
      StorageUtil.writeTableMeta(conf, path, desc.getMeta());
    } catch (IOException e) {
      LOG.error("Cannot write the table meta file", e);
    }
    catalog.addTable(desc);
    LOG.info("Table " + desc.getId() + " is created (" + meta.getStat().getNumBytes() + ")");

    return CreateTableResponse.newBuilder().
        setDesc((TableDescProto) desc.getProto()).build();
  }

  @Override
  public BoolProto existTable(StringProto name) throws RemoteException {
    BoolProto.Builder res = BoolProto.newBuilder();
    return res.setValue(catalog.existsTable(name.getValue())).build();
  }

  @Override
  public void dropTable(StringProto name) throws RemoteException {
    if (!catalog.existsTable(name.getValue())) {
      throw new NoSuchTableException(name.getValue());
    }

    Path path = catalog.getTableDesc(name.getValue()).getPath();
    catalog.deleteTable(name.getValue());
    try {
      this.storeManager.delete(path);
    } catch (IOException e) {
      throw new RemoteException(e);
    }
    LOG.info("Drop Table " + name);
  }

  @Override
  public GetClusterInfoResponse getClusterInfo(GetClusterInfoRequest request) throws RemoteException {
    List<String> onlineServers;
    try {
      onlineServers = getOnlineServer();
      if (onlineServers == null) {
        throw new NullPointerException();
      }
    } catch (Exception e) {
      throw new RemoteException(e);
    }
    GetClusterInfoResponse.Builder builder = GetClusterInfoResponse.newBuilder();
    builder.addAllServerName(onlineServers);
    return builder.build();
  }

  @Override
  public GetTableListResponse getTableList(GetTableListRequest request) {
    Collection<String> tableNames = catalog.getAllTableNames();
    GetTableListResponse.Builder builder = GetTableListResponse.newBuilder();
    builder.addAllTables(tableNames);
    return builder.build();
  }

  @Override
  public TableDescProto getTableDesc(StringProto proto)
      throws RemoteException {
    String name = proto.getValue();
    if (!catalog.existsTable(name)) {
      throw new NoSuchTableException(name);
    }

    return (TableDescProto) catalog.getTableDesc(name).getProto();
  }

  public static void main(String[] args) throws Exception {
    StringUtils.startupShutdownMessage(TajoMaster.class, args, LOG);

    try {
      TajoMaster master = new TajoMaster();
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(master),
          SHUTDOWN_HOOK_PRIORITY);
      TajoConf conf = new TajoConf(new YarnConfiguration());
      master.init(conf);
      master.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting JobHistoryServer", t);
      System.exit(-1);
    }
  }
}