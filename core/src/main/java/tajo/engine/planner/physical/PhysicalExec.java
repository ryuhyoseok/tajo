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

package tajo.engine.planner.physical;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import tajo.SchemaObject;
import tajo.TaskAttemptContext;
import tajo.catalog.Schema;
import tajo.storage.Tuple;

import java.io.IOException;

public abstract class PhysicalExec implements SchemaObject {
  protected final TaskAttemptContext context;
  protected final Schema inSchema;
  protected final Schema outSchema;

  public PhysicalExec(final TaskAttemptContext context, final Schema inSchema,
                      final Schema outSchema) {
    this.context = context;
    this.inSchema = inSchema;
    this.outSchema = outSchema;
  }

  public final Schema getSchema() {
    return outSchema;
  }

  public abstract void init() throws IOException;

  public abstract Tuple next() throws IOException;

  public abstract void rescan() throws IOException;

  public abstract void close() throws IOException;
}
