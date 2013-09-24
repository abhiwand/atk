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
 * Writable to handle serialization of a vector and an associated data
 */
public final class DoubleWithVectorWritable implements Writable {
  /** Data of type double */
  private double data = 0d;
  /** Data of type Vector */
  private final VectorWritable vectorWritable = new VectorWritable();

  /**
   * Default constructor
   */
  public DoubleWithVectorWritable() {
  }

  /**
   * Constructor
   *
   * @param data of type double
   * @param vector of type Vector
   */
  public DoubleWithVectorWritable(double data, Vector vector) {
    this.data = data;
    this.vectorWritable.set(vector);
  }

  /**
   * Setter
   *
   * @param data of type double
   */
  public void setData(double data) {
    this.data = data;
  }

  /**
   * Getter
   *
   * @return data of type double
   */
  public double getData() {
    return data;
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
    data = in.readDouble();
    vectorWritable.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(data);
    vectorWritable.write(out);
  }

  /**
   * Read data and vector to DataInput
   *
   * @param in of type DataInput
   * @return DoubleWithVectorWritable
   * @throws IOException
   */
  public static DoubleWithVectorWritable read(DataInput in) throws IOException {
    DoubleWithVectorWritable writable = new DoubleWithVectorWritable();
    writable.readFields(in);
    return writable;
  }

  /**
   * Write data and vector to DataOutput
   *
   * @param out of type DataOutput
   * @param data of type double
   * @param ssv of type SequentailAccessSparseVector
   * @throws IOException
   */
  public static void write(DataOutput out, double data,
    SequentialAccessSparseVector ssv) throws IOException {
    new DoubleWithVectorWritable(data, ssv).write(out);
  }

}
