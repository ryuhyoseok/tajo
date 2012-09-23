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

package tajo.engine.cluster;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import tajo.QueryUnitAttemptId;
import tajo.TajoProtos.QueryUnitAttemptIdProto;
import tajo.common.Sleeper;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.MasterWorkerProtos.StatusReportProto;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.engine.query.StatusReportImpl;
import tajo.ipc.MasterWorkerProtocol;
import tajo.ipc.StatusReport;
import tajo.master.TajoMaster.MasterContext;
import tajo.master.event.TaskAttemptStatusUpdateEvent;
import tajo.rpc.NettyRpc;
import tajo.rpc.NettyRpcServer;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerListener extends Thread implements MasterWorkerProtocol {
  
  private final static Log LOG = LogFactory.getLog(WorkerListener.class);
  private MasterContext context;
  private final NettyRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private volatile boolean stopped = false;
  private AtomicInteger processed;
  private AtomicInteger pinged;
  private Sleeper sleeper;
  
  public WorkerListener(final MasterContext context) {
    this.context = context;

    String confMasterAddr = context.getConf().getVar(ConfVars.MASTER_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterAddr);
    if (initIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initIsa);
    }
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this, 
        MasterWorkerProtocol.class, initIsa);
    this.stopped = false;
    this.rpcServer.start();
    this.bindAddr = rpcServer.getBindAddress();
    this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();
    processed = new AtomicInteger(0);
    pinged = new AtomicInteger(0);
    sleeper = new Sleeper();
  }
  
  public InetSocketAddress getBindAddress() {
    return this.bindAddr;
  }
  
  public String getAddress() {
    return this.addr;
  }
  
  public boolean isStopped() {
    return this.stopped;
  }
  
  public void shutdown() {
    this.stopped = true;
  }

  static BoolProto TRUE_PROTO = BoolProto.newBuilder().setValue(true).build();

  @Override
  public BoolProto statusUpdate(StatusReportProto proto) {

    if (context.getClusterManager().getFailedWorkers().contains(
        proto.getServerName())) {
      LOG.info("**** Dead man ( " + proto.getServerName() + ") alive!!!!!!");
    }

    StatusReport report = new StatusReportImpl(proto);
    for (TaskStatusProto status : report.getProgressList()) {
      QueryUnitAttemptId uid = new QueryUnitAttemptId(status.getId());
      context.getEventHandler().handle(new TaskAttemptStatusUpdateEvent(uid, status));
      processed.incrementAndGet();
    }

    for (QueryUnitAttemptIdProto pingId : report.getPingList()) {
      QueryUnitAttemptId taskId = new QueryUnitAttemptId(pingId);
      context.getQuery(taskId.getQueryId()).getSubQuery(taskId.getSubQueryId()).
          getQueryUnit(taskId.getQueryUnitId()).getAttempt(taskId).resetExpireTime();
      pinged.incrementAndGet();
    }

    return TRUE_PROTO;
  }

  @Override
  public void run() {
    // rpc listen
    try {
      while (!this.stopped) {
        processed.set(0);
        pinged.set(0);
        sleeper.sleep(1000);
        LOG.info("processed: " + processed + ", pinged: " + pinged);
      }
    } catch (InterruptedException e) {
      LOG.error(ExceptionUtils.getFullStackTrace(e));
    } finally {
      rpcServer.shutdown();
    }
  }
}
