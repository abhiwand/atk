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

package com.intel.mahout.math;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable to handle serialization of a vector and an associated Id
 */
public final class IdWithVectorWritable implements Writable {
  /** Data of type long */
  private long id = 0L;
  /** Data of type Vector */
  private final VectorWritable vectorWritable = new VectorWritable();

  /**
   * Default constructor
   */
  public IdWithVectorWritable() {
  }

  /**
   * Constructor
   *
   * @param id of type long
   * @param vector of type Vector
   */
  public IdWithVectorWritable(long id, Vector vector) {
    this.id = id;
    this.vectorWritable.set(vector);
  }

  /**
   * Setter
   *
   * @param id of type long
   */
  public void setId(long id) {
    this.id = id;
  }

  /**
   * Getter
   *
   * @return id of type long
   */
  public long getId() {
    return id;
  }

  /**
   * Getter
   *
   * @return vector of type Vector
   */
  public Vector getVector() {
    return vectorWritable.get();
  }

  /**
   * Setter
   *
   * @param vector of type Vector
   */
  public void setVector(Vector vector) {
    vectorWritable.set(vector);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    vectorWritable.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    vectorWritable.write(out);
  }

  /**
   * Read id and vector to DataInput
   *
   * @param in of type DataInput
   * @return IdWithVectorWritable
   * @throws IOException
   */
  public static IdWithVectorWritable read(DataInput in) throws IOException {
    IdWithVectorWritable writable = new IdWithVectorWritable();
    writable.readFields(in);
    return writable;
  }

  /**
   * Write id and vector to DataOutput
   *
   * @param out of type DataOutput
   * @param id of type long
   * @param ssv of type SequentailAccessSparseVector
   * @throws IOException
   */
  public static void write(DataOutput out, long id,
    SequentialAccessSparseVector ssv) throws IOException {
    new IdWithVectorWritable(id, ssv).write(out);
  }

}
