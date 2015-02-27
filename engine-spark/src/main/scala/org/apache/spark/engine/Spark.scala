//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

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
import org.apache.spark.sql

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
  def fullOuterJoin(left: RDD[(Any, sql.Row)],
                    other: RDD[(Any, sql.Row)]): RDD[(Any, (Option[sql.Row], Option[sql.Row]))] = {
    left.cogroup(other).flatMapValues {
      case (vs, Seq()) => vs.map(v => (Some(v), None))
      case (Seq(), ws) => ws.map(w => (None, Some(w)))
      case (vs, ws) => for (v <- vs; w <- ws) yield (Some(v), Some(w))
    }.asInstanceOf[RDD[(Any, (Option[sql.Row], Option[sql.Row]))]]
  }
}
