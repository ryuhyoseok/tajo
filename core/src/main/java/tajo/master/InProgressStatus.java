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

package tajo.master;

import tajo.TajoProtos.TaskAttemptState;

public class InProgressStatus {

  private float progress;
  private TaskAttemptState state;
  
  public InProgressStatus() {
    this.setProgress(0.f);
    this.setState(TaskAttemptState.TA_PENDING);
  }
  
  public InProgressStatus(float progress, TaskAttemptState state) {
    this.setProgress(progress);
    this.setState(state);
  }
  
  public void setProgress(float progress) {
    this.progress = progress;
  }
  
  public void setState(TaskAttemptState state) {
    this.state = state;
  }
  
  public float getProgress() {
    return this.progress;
  }
  
  public TaskAttemptState getState() {
    return this.state;
  }
  
  @Override
  public String toString() {
    return "(PROGRESS: " + this.progress + " STATUS: " + this.state + ")";
  }
}
