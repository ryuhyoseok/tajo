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

package tajo.engine.query;

import tajo.TajoProtos.QueryUnitAttemptIdProto;
import tajo.engine.MasterWorkerProtos.StatusReportProto;
import tajo.engine.MasterWorkerProtos.StatusReportProtoOrBuilder;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.ipc.StatusReport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StatusReportImpl implements StatusReport {
  private StatusReportProto proto;
  private StatusReportProto.Builder builder;
  private boolean viaProto;
  private Long timestamp;
  private String serverName;
  private List<TaskStatusProto> inProgressQueries;
  private List<QueryUnitAttemptIdProto> pings;
  
  public StatusReportImpl() {
    builder = StatusReportProto.newBuilder();
  }
  
  public StatusReportImpl(long timestamp, String serverName,
                          List<TaskStatusProto> inProgress) {
    this();
    this.timestamp = timestamp;
    this.serverName = serverName;
    this.inProgressQueries = 
        new ArrayList<TaskStatusProto>(inProgress);
  }
  
  public StatusReportImpl(StatusReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  private void initProgress() {
    if (this.inProgressQueries != null) {
      return;
    }
    StatusReportProtoOrBuilder p = viaProto ? proto : builder;
    this.inProgressQueries = p.getStatusList();
  }

  private void initPings() {
    if (this.pings != null) {
      return;
    }
    StatusReportProtoOrBuilder p = viaProto ? proto : builder;
    this.pings = p.getPingsList();
  }
  
  public Long timestamp() {
    StatusReportProtoOrBuilder p = viaProto ? proto : builder;
    if (timestamp != null) {
      return this.timestamp;
    }
    if (!p.hasTimestamp()) {
      return null;
    }
    timestamp = p.getTimestamp();
    
    return timestamp;
  }
  
  public String getServerName() {
    StatusReportProtoOrBuilder p = viaProto ? proto : builder;
    if (serverName != null) {
      return this.serverName;
    }
    if (!p.hasServerName()) {
      return null;
    }
    serverName = p.getServerName();
    
    return serverName;
  }

  @Override
  public Collection<TaskStatusProto> getProgressList() {
    initProgress();
    return inProgressQueries;
  }

  @Override
  public Collection<QueryUnitAttemptIdProto> getPingList() {
    initPings();
    return pings;
  }

  @Override
  public void initFromProto() {    
  }

  @Override
  public StatusReportProto getProto() {
    mergeLocalToProto();
    
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = StatusReportProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }    
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
  
  private void mergeLocalToBuilder() {
    if (this.timestamp != null) {
      builder.setTimestamp(timestamp);
    }
    if (this.serverName != null) {
      builder.setServerName(serverName);
    }
    if (this.inProgressQueries != null) {
      builder.clearStatus();
      builder.addAllStatus(this.inProgressQueries);
    }    
  }
}
