/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.master.event;

import org.apache.hadoop.yarn.event.AbstractEvent;
import tajo.QueryUnitAttemptId;

public class ContainerAllocatorEvent
    extends AbstractEvent<ContainerAllocatorEventType> {
  private final QueryUnitAttemptId attemptId;
  private final boolean isLeafQuery;
  private final String[] hosts;
  private final String[] racks;

  public ContainerAllocatorEvent(final QueryUnitAttemptId attemptId,
                                 boolean isLeafQuery,
                                 final String [] hosts,
                                 final String [] racks,
                                 final ContainerAllocatorEventType eventType) {
    super(eventType);
    this.attemptId = attemptId;
    this.isLeafQuery = isLeafQuery;
    this.hosts = hosts;
    this.racks = racks;
  }

  public QueryUnitAttemptId getAttemptId() {
    return this.attemptId;
  }

  public boolean isLeafQuery() {
    return this.isLeafQuery;
  }

  public String [] getHosts() {
    return this.hosts;
  }

  public String [] getRacks() {
    return this.racks;
  }
}
