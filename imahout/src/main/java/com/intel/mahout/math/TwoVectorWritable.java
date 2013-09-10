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
 * Writable to handle serialization of two vectors: prior and posterior
 */
public final class TwoVectorWritable implements Writable {
  /** prior vector */
  private final VectorWritable priorWritable = new VectorWritable();
  /** posterior vector */
  private final VectorWritable posteriorWritable = new VectorWritable();

  /**
   * Default constructor
   */
  public TwoVectorWritable() {
  }

  /**
   * Constructor
   * @param prior
   *        prior vector
   * @param posterior
   *        posterior vector
   */
  public TwoVectorWritable(Vector prior, Vector posterior) {
    this.priorWritable.set(prior);
    this.posteriorWritable.set(posterior);
  }

  /**
   * Getter
   * @return prior
   */
  public Vector getPriorVector() {
    return priorWritable.get();
  }

  /**
   * Setter
   * @param vector
   *        prior vector
   */
  public void setPriorVector(Vector vector) {
    priorWritable.set(vector);
  }

  /**
   * Getter
   * @return posterior
   */
  public Vector getPosteriorVector() {
    return posteriorWritable.get();
  }

  /**
   * Setter
   * @param vector
   *        posterior vector
   */
  public void setPosteriorVector(Vector vector) {
    posteriorWritable.set(vector);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    priorWritable.readFields(in);
    posteriorWritable.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    priorWritable.write(out);
    posteriorWritable.write(out);
  }

  /**
   * Read prior and posterior to DataInput
   * @param in
   *        DataInput
   * @return TwoVectorWritable
   * @throws IOException
   */
  public static TwoVectorWritable read(DataInput in) throws IOException {
    TwoVectorWritable writable = new TwoVectorWritable();
    writable.readFields(in);
    return writable;
  }

  /**
   * Write prior and posterior to DataOutput
   * @param out
   *        DataOutput
   * @param ssv1
   *        Vector of type SequentialAccessSparseVector
   * @param ssv2
   *        Vector of type SequentialAccessSparseVector
   * @throws IOException
   */
  public static void write(DataOutput out, SequentialAccessSparseVector ssv1,
    SequentialAccessSparseVector ssv2) throws IOException {
    new TwoVectorWritable(ssv1, ssv2).write(out);
  }

}
