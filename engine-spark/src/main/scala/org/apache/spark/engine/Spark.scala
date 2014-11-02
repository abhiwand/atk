/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.engine
import org.apache.spark.rdd.RDD
//implicit conversion for PairRDD
import org.apache.spark.SparkContext._

/**
 * This module contains implementations copied from Apache Spark
 */
object Spark {

  /**
   * This code is taken from Spark source. https://github.com/apache/spark/pull/1395
   * It will be removed when upgrading to newer version of Spark which has this method included.
   *
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Uses the given Partitioner to partition the output RDD.
   */
  def fullOuterJoin(left: RDD[(Any, Array[Any])], other: RDD[(Any, Array[Any])]): RDD[(Any, (Option[Array[Any]], Option[Array[Any]]))] = {
    left.cogroup(other).flatMapValues {
      case (vs, Seq()) => vs.map(v => (Some(v), None))
      case (Seq(), ws) => ws.map(w => (None, Some(w)))
      case (vs, ws) => for (v <- vs; w <- ws) yield (Some(v), Some(w))
    }.asInstanceOf[RDD[(Any, (Option[Array[Any]], Option[Array[Any]]))]]
  }
}
