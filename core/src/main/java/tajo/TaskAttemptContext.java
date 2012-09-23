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

package tajo;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.TajoProtos.TaskAttemptState;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf;
import tajo.ipc.protocolrecords.Fragment;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;


/**
 * 실행 중인 subquery에 대한 정보를 담는다.
 *
 */
public class TaskAttemptContext {
  private static final Log LOG = LogFactory.getLog(TaskAttemptContext.class);
  private final TajoConf conf;
  private final Map<String, List<Fragment>> fragmentMap = new HashMap<>();
  
  private TaskAttemptState state;
  private TableStat resultStats;
  private QueryUnitAttemptId queryId;
  private final File workDir;
  private boolean needFetch = false;
  private CountDownLatch doneFetchPhaseSignal;
  private float progress = 0;
  private Map<Integer, String> repartitions;
  private File fetchIn;
  private boolean stopped = false;
  private boolean interQuery = false;

  public TaskAttemptContext(TajoConf conf, final QueryUnitAttemptId queryId,
                     final Fragment[] fragments,
                     final File workDir) {
    this.conf = conf;
    this.queryId = queryId;
    
    for(Fragment t : fragments) {
      if (fragmentMap.containsKey(t.getId())) {
        fragmentMap.get(t.getId()).add(t);
      } else {
        List<Fragment> frags = new ArrayList<Fragment>();
        frags.add(t);
        fragmentMap.put(t.getId(), frags);
      }
    }

    this.workDir = workDir;
    this.repartitions = Maps.newHashMap();
    
    state = TaskAttemptState.TA_PENDING;
  }

  public TajoConf getConf() {
    return this.conf;
  }
  
  public TaskAttemptState getState() {
    return this.state;
  }
  
  public void setState(TaskAttemptState state) {
    this.state = state;
    LOG.info("Query status of " + getTaskId() + " is changed to " + state);
  }

  public boolean hasResultStats() {
    return resultStats != null;
  }

  public void setResultStats(TableStat stats) {
    this.resultStats = stats;
  }

  public TableStat getResultStats() {
    return this.resultStats;
  }
  
  public boolean isStopped() {
    return this.stopped;
  }

  public void setInterQuery() {
    this.interQuery = true;
  }

  public boolean isInterQuery() {
    return this.interQuery;
  }
  
  public void stop() {
    this.stopped = true;
  }
  
  public void addFetchPhase(int count, File fetchIn) {
    this.needFetch = true;
    this.doneFetchPhaseSignal = new CountDownLatch(count);
    this.fetchIn = fetchIn;
  }
  
  public File getFetchIn() {
    return this.fetchIn;
  }
  
  public boolean hasFetchPhase() {
    return this.needFetch;
  }
  
  public CountDownLatch getFetchLatch() {
    return doneFetchPhaseSignal;
  }
  
  public void addRepartition(int partKey, String path) {
    repartitions.put(partKey, path);
  }
  
  public Iterator<Entry<Integer,String>> getRepartitions() {
    return repartitions.entrySet().iterator();
  }
  
  public void changeFragment(String tableId, Fragment [] fragments) {
    fragmentMap.remove(tableId);
    for(Fragment t : fragments) {
      if (fragmentMap.containsKey(t.getId())) {
        fragmentMap.get(t.getId()).add(t);
      } else {
        List<Fragment> frags = new ArrayList<Fragment>();
        frags.add(t);
        fragmentMap.put(t.getId(), frags);
      }
    }
  }
  
  public File getWorkDir() {
    return this.workDir;
  }
  
  public QueryUnitAttemptId getTaskId() {
    return this.queryId;
  }
  
  public float getProgress() {
    return this.progress;
  }
  
  public void setProgress(float progress) {
    this.progress = progress;
  }

  public Fragment getTable(String id) {
    return fragmentMap.get(id).get(0);
  }

  public int getFragmentSize() {
    return fragmentMap.size();
  }

  public Collection<String> getInputTables() {
    return fragmentMap.keySet();
  }
  
  public Fragment [] getTables(String id) {
    return fragmentMap.get(id).toArray(new Fragment[fragmentMap.get(id).size()]);
  }
  
  public int hashCode() {
    return Objects.hashCode(queryId);
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof TaskAttemptContext) {
      TaskAttemptContext other = (TaskAttemptContext) obj;
      return queryId.equals(other.getTaskId());
    } else {
      return false;
    }
  }
}