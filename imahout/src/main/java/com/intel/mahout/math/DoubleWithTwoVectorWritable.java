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
 * Writable to handle serialization of a two vectors and the associated value
 */
public final class DoubleWithTwoVectorWritable implements Writable {
  /** Data of type double */
  private double data = 0d;
  /** Data of type vector on out edge */
  private final VectorWritable vectorOutWritable = new VectorWritable();
  /** Data of type vector on in edge */
  private final VectorWritable vectorInWritable = new VectorWritable();
  /**
   * Default constructor
   */
  public DoubleWithTwoVectorWritable() {
  }

  /**
   * Constructor
   *
   * @param data of type double
   * @param vectorOut of type Vector
   * @param vectorIn of type Vector
   */
  public DoubleWithTwoVectorWritable(double data, Vector vectorOut,
      Vector vectorIn) {
    this.data = data;
    this.vectorOutWritable.set(vectorOut);
    this.vectorInWritable.set(vectorIn);
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
   * @return vectorOut of type Vector
   */
  public Vector getVectorOut() {
    return vectorOutWritable.get();
  }

  /**
   * Setter
   *
   * @param vector of type Vector
   */
  public void setVectorOut(Vector vector) {
    vectorOutWritable.set(vector);
  }

  /**
   * Getter
   *
   * @return vectorIn of type Vector
   */
  public Vector getVectorIn() {
    return vectorInWritable.get();
  }

  /**
   * Setter
   *
   * @param vector of type Vector
   */
  public void setVectorIn(Vector vector) {
    vectorInWritable.set(vector);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    data = in.readDouble();
    vectorOutWritable.readFields(in);
    vectorInWritable.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(data);
    vectorOutWritable.write(out);
    vectorInWritable.write(out);
  }

  /**
   * Read data and vector to DataInput
   *
   * @param in of type DataInput
   * @return DoubleWithVectorWritable
   * @throws IOException
   */
  public static DoubleWithTwoVectorWritable read(DataInput in)
    throws IOException {
    DoubleWithTwoVectorWritable writable = new DoubleWithTwoVectorWritable();
    writable.readFields(in);
    return writable;
  }

  /**
   * Write data and vector to DataOutput
   *
   * @param out of type DataOutput
   * @param data of type double
   * @param ssvOut of type SequentialAccessSparseVector
   * @param ssvIn of type SequentialAccessSparseVector
   * @throws IOException
   */
  public static void write(DataOutput out, double data,
    SequentialAccessSparseVector ssvOut,
    SequentialAccessSparseVector ssvIn) throws IOException {
    new DoubleWithTwoVectorWritable(data, ssvOut, ssvIn).write(out);
  }

}
