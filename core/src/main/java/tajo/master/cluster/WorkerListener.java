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

package tajo.master.cluster;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.service.AbstractService;
import tajo.QueryUnitAttemptId;
import tajo.TajoProtos.QueryUnitAttemptIdProto;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.MasterWorkerProtos.QueryUnitRequestProto;
import tajo.engine.MasterWorkerProtos.StatusReportProto;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.engine.query.StatusReportImpl;
import tajo.ipc.MasterWorkerProtocol;
import tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService;
import tajo.ipc.MasterWorkerProtocol.WorkerId;
import tajo.ipc.StatusReport;
import tajo.master.TajoMaster.MasterContext;
import tajo.master.event.TaskAttemptStatusUpdateEvent;
import tajo.rpc.ProtoAsyncRpcServer;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;

import java.net.InetSocketAddress;

public class WorkerListener extends AbstractService
    implements MasterWorkerProtocolService.Interface {
  
  private final static Log LOG = LogFactory.getLog(WorkerListener.class);
  private MasterContext context;
  private ProtoAsyncRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  
  public WorkerListener(final MasterContext context) throws Exception {
    super(WorkerListener.class.getName());
    this.context = context;
  }

  @Override
  public void init(Configuration conf) {

    String confMasterAddr = context.getConf().getVar(ConfVars.MASTER_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterAddr);
    if (initIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initIsa);
    }
    try {
      this.rpcServer = new ProtoAsyncRpcServer(MasterWorkerProtocol.class,
          this, initIsa);
    } catch (Exception e) {
      LOG.error(e);
    }
    this.rpcServer.start();
    this.bindAddr = rpcServer.getBindAddress();
    this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();

    // Setup RPC server
    // Get the master address
    LOG.info(WorkerListener.class.getSimpleName() + " is bind to " + addr);
    context.getConf().setVar(TajoConf.ConfVars.MASTER_ADDRESS, addr);

    super.init(conf);
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {
    rpcServer.shutdown();
  }
  
  public InetSocketAddress getBindAddress() {
    return this.bindAddr;
  }
  
  public String getAddress() {
    return this.addr;
  }

  static BoolProto TRUE_PROTO = BoolProto.newBuilder().setValue(true).build();

  @Override
  public void getTask(RpcController controller, WorkerId request,
                      RpcCallback<QueryUnitRequestProto> done) {
    LOG.info("Get TaskRequest");
  }

  @Override
  public void statusUpdate(RpcController controller, TaskStatusProto request,
                           RpcCallback<BoolProto> done) {
    QueryUnitAttemptId attemptId = new QueryUnitAttemptId(request.getId());
    context.getEventHandler().handle(new TaskAttemptStatusUpdateEvent(attemptId,
        request));
    done.run(TRUE_PROTO);
  }

  @Override
  public void ping(RpcController controller,
                   QueryUnitAttemptIdProto attemptIdProto,
                   RpcCallback<BoolProto> done) {
    QueryUnitAttemptId attemptId = new QueryUnitAttemptId(attemptIdProto);
    context.getQuery(attemptId.getQueryId()).getSubQuery(attemptId.getSubQueryId()).
        getQueryUnit(attemptId.getQueryUnitId()).getAttempt(attemptId).
        resetExpireTime();
    done.run(TRUE_PROTO);
  }

  @Deprecated
  public void statusUpdate(RpcController controller, StatusReportProto request,
                           RpcCallback<BoolProto> done) {
    if (context.getClusterManager().getFailedWorkers().contains(
        request.getServerName())) {
    }

    StatusReport report = new StatusReportImpl(request);
    for (TaskStatusProto status : report.getProgressList()) {
      QueryUnitAttemptId uid = new QueryUnitAttemptId(status.getId());
      context.getEventHandler().handle(new TaskAttemptStatusUpdateEvent(uid, status));
    }

    for (QueryUnitAttemptIdProto pingId : report.getPingList()) {
      QueryUnitAttemptId taskId = new QueryUnitAttemptId(pingId);
      context.getQuery(taskId.getQueryId()).getSubQuery(taskId.getSubQueryId()).
          getQueryUnit(taskId.getQueryUnitId()).getAttempt(taskId).resetExpireTime();
    }

    done.run(TRUE_PROTO);
  }
}
