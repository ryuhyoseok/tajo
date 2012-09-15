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

package tajo;

import tajo.TajoProtos.QueryUnitAttemptIdProto;
import tajo.TajoProtos.QueryUnitAttemptIdProtoOrBuilder;
import tajo.common.ProtoObject;

import java.text.NumberFormat;

public class QueryUnitAttemptId implements Comparable<QueryUnitAttemptId>,
    ProtoObject<QueryUnitAttemptIdProto> {

  private static final NumberFormat format = NumberFormat.getInstance();
  static {
    format.setGroupingUsed(false);
    format.setMinimumIntegerDigits(2);
  }

  private QueryUnitId queryUnitId = null;
  private int id = -1;
  private String finalId = null;

  private QueryUnitAttemptIdProto proto =
      QueryUnitAttemptIdProto.getDefaultInstance();
  private QueryUnitAttemptIdProto.Builder builder = null;
  private boolean viaProto = false;

  public QueryUnitAttemptId() {
    builder = QueryUnitAttemptIdProto.newBuilder();
  }

  public QueryUnitAttemptId(final QueryUnitId queryUnitId, final int id) {
    this.queryUnitId = queryUnitId;
    this.id = id;
  }

  public QueryUnitAttemptId(QueryUnitAttemptIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public QueryUnitAttemptId(final String finalId) {
    this.finalId = finalId;
    int i = finalId.lastIndexOf(QueryId.SEPARATOR);
    this.queryUnitId = new QueryUnitId(finalId.substring(0, i));
    this.id = Integer.valueOf(finalId.substring(i+1));
  }

  public int getId() {
    QueryUnitAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id != -1) {
      return this.id;
    }
    if (!p.hasId()) {
      return -1;
    }
    this.id = p.getId();
    return id;
  }

  public QueryUnitId getQueryUnitId() {
    QueryUnitAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.queryUnitId != null) {
      return this.queryUnitId;
    }
    if (!p.hasId()) {
      return null;
    }
    this.queryUnitId = new QueryUnitId(p.getQueryUnitId());
    return queryUnitId;
  }

  public QueryId getQueryId() {
    return this.getQueryUnitId().getQueryId();
  }

  public SubQueryId getSubQueryId() {
    return this.getQueryUnitId().getSubQueryId();
  }

  @Override
  public final String toString() {
    if (finalId == null) {
      finalId = this.getQueryUnitId() +
          QueryId.SEPARATOR + format.format(getId());
    }
    return this.finalId;
  }

  @Override
  public final boolean equals(final Object o) {
    if (o instanceof QueryUnitAttemptId) {
      QueryUnitAttemptId other = (QueryUnitAttemptId) o;
      return this.toString().equals(other.toString());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  @Override
  public int compareTo(QueryUnitAttemptId o) {
    return this.id - o.getId();
  }

  private void mergeProtoToLocal() {
    QueryUnitAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    if (queryUnitId == null) {
      queryUnitId = new QueryUnitId(p.getQueryUnitId());
    }
    if (id == -1) {
      id = p.getId();
    }
  }

  @Override
  public void initFromProto() {
    mergeProtoToLocal();
  }

  private void mergeLocalToBuilder() {
    if (builder == null) {
      builder = QueryUnitAttemptIdProto.newBuilder(proto);
    }
    if (this.queryUnitId != null) {
      builder.setQueryUnitId(queryUnitId.getProto());
    }
    if (this.id != -1) {
      builder.setId(id);
    }
  }

  @Override
  public QueryUnitAttemptIdProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    return proto;
  }
}
