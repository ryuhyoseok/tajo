/*
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

package tajo.worker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.RpcCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import tajo.NConstants;
import tajo.QueryUnitAttemptId;
import tajo.TajoProtos.TaskAttemptState;
import tajo.common.Sleeper;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.MasterWorkerProtos.*;
import tajo.engine.query.QueryUnitRequestImpl;
import tajo.ipc.AsyncWorkerProtocol;
import tajo.ipc.MasterWorkerProtocol;
import tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService;
import tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService.Interface;
import tajo.ipc.MasterWorkerProtocol.WorkerId;
import tajo.ipc.protocolrecords.QueryUnitRequest;
import tajo.master.cluster.MasterAddressTracker;
import tajo.rpc.*;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.storage.StorageUtil;
import tajo.webapp.HttpServer;
import tajo.worker.dataserver.HttpDataServer;
import tajo.worker.dataserver.retriever.AdvancedDataRetriever;
import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkUtil;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker extends Thread implements AsyncWorkerProtocol {
  private static final Log LOG = LogFactory.getLog(Worker.class);

  private final TajoConf conf;

  // Server States
  private NettyRpcServer rpcServer;
  private InetSocketAddress isa;

  private volatile boolean stopped = false;
  private volatile boolean isOnline = false;

  private String serverName;

  // Cluster Management
  private ZkClient zkClient;
  private MasterAddressTracker masterAddrTracker;
  private ProtoAsyncRpcClient client;
  private MasterWorkerProtocolService.Interface master;

  // Query Processing
  private FileSystem localFS;
  private FileSystem defaultFS;
  private final File workDir;

  private TQueryEngine queryEngine;
  private QueryLauncher queryLauncher;
  private final int coreNum = Runtime.getRuntime().availableProcessors();
  private final ExecutorService fetchLauncher = 
      Executors.newFixedThreadPool(coreNum);  
  private final Map<QueryUnitAttemptId, Task> tasks = Maps.newConcurrentMap();
  private HttpDataServer dataServer;
  private AdvancedDataRetriever retriever;
  private String dataServerURL;
  private final LocalDirAllocator lDirAllocator;

  private Thread taskLauncher;
  
  //Web server
  private HttpServer webServer;

  private WorkerContext workerContext;

  private Reporter reporter;

  public Worker(final TajoConf conf) {
    this.conf = conf;
    lDirAllocator = new LocalDirAllocator(ConfVars.WORKER_TMP_DIR.varname);
    LOG.info(conf.getVar(ConfVars.WORKER_TMP_DIR));
    this.workDir = new File(conf.getVar(ConfVars.WORKER_TMP_DIR));
  }
  
  private void prepareServing() throws IOException {
    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));

    defaultFS = FileSystem.get(URI.create(
        conf.getVar(ConfVars.ENGINE_BASE_DIR)),conf);

    localFS = FileSystem.getLocal(conf);
    Path workDirPath = new Path(workDir.toURI());
    if (!localFS.exists(workDirPath)) {
      localFS.mkdirs(workDirPath);
      LOG.info("local temporal dir is created: " + localFS.exists(workDirPath));
      LOG.info("local temporal dir (" + workDir + ") is created");
    }

    String hostname = DNS.getDefaultHost(
        conf.get("nta.master.dns.interface", "default"),
        conf.get("nta.master.dns.nameserver", "default"));

    // Creation of a HSA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, 0);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + this.isa);
    }
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this,
        AsyncWorkerProtocol.class, initialIsa);
    this.rpcServer.start();
    
    // Set our address.
    this.isa = this.rpcServer.getBindAddress();
    this.serverName = this.isa.getHostName() + ":" + this.isa.getPort();
    
    this.zkClient = new ZkClient(this.conf);
    this.queryLauncher = new QueryLauncher();
    this.queryLauncher.start();
    this.queryEngine = new TQueryEngine(conf);
    
    this.retriever = new AdvancedDataRetriever();
    this.dataServer = new HttpDataServer(NetUtils.createSocketAddr(hostname, 0), 
        retriever);
    this.dataServer.start();
    
    InetSocketAddress dataServerAddr = this.dataServer.getBindAddress(); 
    this.dataServerURL = "http://" + dataServerAddr.getAddress().getHostAddress() + ":" 
        + dataServerAddr.getPort();
    LOG.info("dataserver listens on " + dataServerURL);

    this.workerContext = new WorkerContext();

    webServer = new HttpServer("admin", this.isa.getHostName() ,8080 ,
        true, null, conf, null);
    webServer.setAttribute("tajo.master.addr",
        conf.getVar(ConfVars.MASTER_ADDRESS));
    webServer.start();
  }

  private void participateCluster() throws Exception, InterruptedException,
      KeeperException {
    this.masterAddrTracker = new MasterAddressTracker(zkClient);
    this.masterAddrTracker.start();

    byte[] masterAddrBytes;
    String masterAddr;
    do {
      masterAddrBytes = masterAddrTracker.blockUntilAvailable(1000);
      LOG.info("Waiting for the Tajo master.....");
    } while (masterAddrBytes == null);

    masterAddr = new String(masterAddrBytes);

    LOG.info("Got the master address (" + masterAddr + ")");
    // if the znode already exists, it will be updated for notification.
    ZkUtil.upsertEphemeralNode(zkClient,
        ZkUtil.concat(NConstants.ZNODE_WORKERS, serverName));
    LOG.info("Created the znode " + NConstants.ZNODE_WORKERS + "/"
        + serverName);
    
    InetSocketAddress addr = NetUtils.createSocketAddr(masterAddr);
    this.client = new ProtoAsyncRpcClient(MasterWorkerProtocol.class, addr);
    this.master = client.getStub();

    this.reporter = new Reporter(this.master);
    this.reporter.startCommunicationThread();
  }

  class WorkerContext {
    public TajoConf getConf() {
      return conf;
    }

    public AdvancedDataRetriever getRetriever() {
      return retriever;
    }

    public MasterWorkerProtocolService.Interface getMaster() {
      return master;
    }

    public FileSystem getLocalFS() {
      return localFS;
    }

    public FileSystem getDefaultFS() {
      return defaultFS;
    }

    public LocalDirAllocator getLocalDirAllocator() {
      return lDirAllocator;
    }

    public TQueryEngine getTQueryEngine() {
      return queryEngine;
    }

    public Map<QueryUnitAttemptId, Task> getTasks() {
      return tasks;
    }

    public Task getTask(QueryUnitAttemptId taskId) {
      return tasks.get(taskId);
    }

    public ExecutorService getFetchLauncher() {
      return fetchLauncher;
    }

    public String getDataServerURL() {
      return dataServerURL;
    }
  }

  public void run() {
    LOG.info("Tajo Worker startup");

    try {

      try {
        prepareServing();
        participateCluster();

      } catch (Exception e) {
        abort(e.getMessage(), e);
      }

      final WorkerId workerId = WorkerId.newBuilder().
          setHostAddress(serverName).build();

      taskLauncher = new Thread(new Runnable() {
        @Override
        public void run() {

          CallFuture2<QueryUnitRequestProto> future = null;
          QueryUnitRequestProto taskRequest;

          while(!stopped) {
            try {

              while(!queryLauncher.hasAvailableSlot()) {
                Thread.sleep(1000);
              }

              future = new CallFuture2<>();
              master.getTask(null, workerId, future);

              taskRequest = future.get();
              requestQueryUnit(taskRequest);

            } catch (Throwable t) {
              LOG.error(t);
            }
          }
        }
      });
      taskLauncher.start();
      taskLauncher.join();

    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
    } finally {     
      for (Task t : tasks.values()) {
        if (t.getStatus() != TaskAttemptState.TA_SUCCEEDED) {
          t.abort();
        }
      }

      // remove the znode
      ZkUtil.concat(NConstants.ZNODE_WORKERS, serverName);

      try {
        webServer.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }

      rpcServer.shutdown();
      queryLauncher.shutdown();

      try {
        reporter.stopCommunicationThread();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      client.close();
      masterAddrTracker.stop();
      zkClient.close();
    }

    LOG.info("Worker (" + serverName + ") main thread exiting");
  }
  
  private void sendHeartbeat(long time) throws IOException {
    StatusReportProto.Builder report = StatusReportProto.newBuilder();
    report.setTimestamp(time);
    report.setServerName(serverName);
    
    // to send
    List<TaskStatusProto> list
      = new ArrayList<>();
    TaskStatusProto taskStatus;
    // to be removed
    List<QueryUnitAttemptId> tobeRemoved = Lists.newArrayList();
    
    // builds one status for each in-progress query
    TaskAttemptState taskState;

    for (Task task : tasks.values()) {
      if (task.getProgressFlag()) {
        taskState = task.getStatus();
        if (taskState == TaskAttemptState.TA_FAILED
            || taskState == TaskAttemptState.TA_KILLED
            || taskState == TaskAttemptState.TA_SUCCEEDED) {
          // TODO - in-progress queries should be kept until this leafserver
          // ensures that this report is delivered.
          tobeRemoved.add(task.getId());
        }

        taskStatus = task.getReport();
        task.resetProgressFlag();
        report.addStatus(taskStatus);
      } else {
        report.addPings(task.getId().getProto());
      }
    }

    master.statusUpdate(null, report.build(), new NullCallback());
  }

  private class ShutdownHook implements Runnable {
    @Override
    public void run() {
      shutdown("Shutdown Hook");
    }
  }

  public String getServerName() {
    return this.serverName;
  }

  /**
   * @return true if a stop has been requested.
   */
  public boolean isStopped() {
    return this.stopped;
  }

  public boolean isOnline() {
    return this.isOnline;
  }

  public void shutdown(final String msg) {

    for (Task task : tasks.values()) {
      if (task.getStatus() == TaskAttemptState.TA_PENDING ||
          task.getStatus() == TaskAttemptState.TA_RUNNING) {
        task.setState(TaskAttemptState.TA_FAILED);
      }
    }

    this.stopped = true;

    LOG.info("STOPPED: " + msg);
    synchronized (this) {
      notifyAll();
    }
  }

  public void abort(String reason, Throwable cause) {
    if (cause != null) {
      LOG.fatal("ABORTING worker " + serverName + ": " + reason, cause);
    } else {
      LOG.fatal("ABORTING worker " + serverName + ": " + reason);
    }
    // TODO - abortRequest : to be implemented
    shutdown(reason);
  }
  
  public static Path getQueryUnitDir(QueryUnitAttemptId quid) {
    Path workDir = 
        StorageUtil.concatPath(
            quid.getSubQueryId().toString(),
            String.valueOf(quid.getQueryUnitId().getId()),
            String.valueOf(quid.getId()));
    return workDir;
  }

  //////////////////////////////////////////////////////////////////////////////
  // AsyncWorkerProtocol
  //////////////////////////////////////////////////////////////////////////////

  static BoolProto TRUE_PROTO = BoolProto.newBuilder().setValue(true).build();

  @Override
  public BoolProto requestQueryUnit(QueryUnitRequestProto proto)
      throws Exception {
    QueryUnitRequest request = new QueryUnitRequestImpl(proto);
    Task task = new Task(workerContext, request);
    synchronized(tasks) {
      if (tasks.containsKey(task.getId())) {
        throw new IllegalStateException("Query unit (" + task.getId() + ") is already is submitted");
      }    
      tasks.put(task.getId(), task);
    }        
    if (task.hasFetchPhase()) {
      task.fetch(); // The fetch is performed in an asynchronous way.
    }
    task.init();
    queryLauncher.schedule(task);

    return TRUE_PROTO;
  }

  @Override
  public ServerStatusProto getServerStatus(NullProto request) {
    // serverStatus builder
    ServerStatusProto.Builder serverStatus = ServerStatusProto.newBuilder();
    // TODO: compute the available number of task slots
    serverStatus.setTaskNum(tasks.size());

    // system(CPU, memory) status builder
    ServerStatusProto.System.Builder systemStatus = ServerStatusProto.System
        .newBuilder();

    systemStatus.setAvailableProcessors(Runtime.getRuntime()
        .availableProcessors());
    systemStatus.setFreeMemory(Runtime.getRuntime().freeMemory());
    systemStatus.setMaxMemory(Runtime.getRuntime().maxMemory());
    systemStatus.setTotalMemory(Runtime.getRuntime().totalMemory());

    serverStatus.setSystem(systemStatus);

    // disk status builder
    File[] roots = File.listRoots();
    for (File root : roots) {
      ServerStatusProto.Disk.Builder diskStatus = ServerStatusProto.Disk
          .newBuilder();

      diskStatus.setAbsolutePath(root.getAbsolutePath());
      diskStatus.setTotalSpace(root.getTotalSpace());
      diskStatus.setFreeSpace(root.getFreeSpace());
      diskStatus.setUsableSpace(root.getUsableSpace());

      serverStatus.addDisk(diskStatus);
    }
    return serverStatus.build();
  }

  @VisibleForTesting
  Task getTask(QueryUnitAttemptId id) {
    return this.tasks.get(id);
  }
  
  private class QueryLauncher extends Thread {
    private final BlockingQueue<Task> blockingQueue
      = new ArrayBlockingQueue<>(coreNum);
    private final ExecutorService executor
      = Executors.newFixedThreadPool(coreNum);
    private boolean stopped = false;
    
    public void schedule(Task task) throws InterruptedException {
      this.blockingQueue.put(task);
    }

    public boolean hasAvailableSlot() {
      return blockingQueue.size() == 0;
    }
    
    public void shutdown() {
      stopped = true;
    }
    
    @Override
    public void run() {
      try {
        LOG.info("Started the query launcher (maximum concurrent tasks: " 
            + coreNum + ")");
        while (!Thread.interrupted() && !stopped) {
          // wait for add
          Task task = blockingQueue.poll(1000, TimeUnit.MILLISECONDS);
          
          // the futures keeps submitted tasks for force kill when
          // the leafserver shutdowns.
          if (task != null) {
            executor.submit(task);
          }          
        }
      } catch (Throwable t) {
        LOG.error(t);
      } finally {
        executor.shutdown();
      }
    }
  }

  @Override
  public CommandResponseProto requestCommand(CommandRequestProto request) {
    QueryUnitAttemptId uid;
    for (Command cmd : request.getCommandList()) {
      uid = new QueryUnitAttemptId(cmd.getId());
      Task task = tasks.get(uid);
      if (task == null) {
        LOG.warn("Unknown task: " + uid);
        return null;
      }
      TaskAttemptState state = task.getStatus();
      switch (cmd.getType()) {
      case FINALIZE:
        if (state == TaskAttemptState.TA_SUCCEEDED
        || state == TaskAttemptState.TA_FAILED
        || state == TaskAttemptState.TA_KILLED) {
          task.cleanUp();
          LOG.info("Query unit ( " + uid + ") is finalized");
        } else {
          task.kill();
          LOG.info("Query unit ( " + uid + ") is stopped");
        }
        break;
      case STOP:
        task.kill();
        tasks.remove(task.getId());
        LOG.info("Query unit ( " + uid + ") is stopped");
        break;
      default:
        break;
      }
    }
    return null;
  }

  protected class Reporter implements Runnable {
    private MasterWorkerProtocolService.Interface masterStub;
    private Thread pingThread;
    private Object lock = new Object();
    private static final int PROGRESS_INTERVAL = 3000;
    private StatusReportProto.Builder reportBuilder = StatusReportProto.newBuilder();

    public Reporter(Interface masterStub) {
      this.masterStub = masterStub;
    }

    @Override
    public void run() {
      final int MAX_RETRIES = 3;
      int remainingRetries = MAX_RETRIES;

      while (!stopped) {
        try {
          synchronized(lock) {
            lock.wait(PROGRESS_INTERVAL);
          }

          reportBuilder.setTimestamp(System.currentTimeMillis());
          reportBuilder.setServerName(serverName);

          // to send
          TaskStatusProto taskStatus;
          // to be removed
          List<QueryUnitAttemptId> tobeRemoved = Lists.newArrayList();

          // builds one status for each in-progress query
          TaskAttemptState taskState;

          for (Task task : tasks.values()) {
            if (task.getProgressFlag()) {
              taskState = task.getStatus();
              if (taskState == TaskAttemptState.TA_FAILED
                  || taskState == TaskAttemptState.TA_KILLED
                  || taskState == TaskAttemptState.TA_SUCCEEDED) {
                // TODO - in-progress queries should be kept until this leafserver
                // ensures that this report is delivered.
                tobeRemoved.add(task.getId());
              }

              taskStatus = task.getReport();
              task.resetProgressFlag();
              reportBuilder.addStatus(taskStatus);
            } else {
              reportBuilder.addPings(task.getId().getProto());
            }
          }

          masterStub.statusUpdate(null, reportBuilder.build(), NullCallback.get());
          reportBuilder.clear();

        } catch (Throwable t) {

          LOG.info("Communication exception: " + StringUtils.stringifyException(t));
          remainingRetries -=1;
          if (remainingRetries == 0) {
            ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
            LOG.warn("Last retry, killing ");
            System.exit(65);
          }
        }
      }
    }

    public void startCommunicationThread() {
      if (pingThread == null) {
        pingThread = new Thread(this, "communication thread");
        pingThread.setDaemon(true);
        pingThread.start();
      }
    }

    public void stopCommunicationThread() throws InterruptedException {
      if (pingThread != null) {
        // Intent of the lock is to not send an interupt in the middle of an
        // umbilical.ping or umbilical.statusUpdate
        synchronized(lock) {
          //Interrupt if sleeping. Otherwise wait for the RPC call to return.
          lock.notify();
        }

        pingThread.interrupt();
        pingThread.join();
      }
    }
  }

  public static void main(String[] args) throws IOException {
    TajoConf conf = new TajoConf();
    Worker worker = new Worker(conf);
    worker.start();
  }
}
