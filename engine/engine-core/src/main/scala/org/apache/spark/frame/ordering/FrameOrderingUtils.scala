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

package org.apache.spark.frame.ordering

import org.apache.spark.rdd.RDD
import org.apache.spark.util
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.mllib.rdd.RDDFunctions._

import scala.reflect.ClassTag

object FrameOrderingUtils extends Serializable {

  /**
   * A modification of Spark's takeOrdered which uses a tree-reduce
   *
   * Spark's takeOrdered returns the first k (smallest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering.
   *
   * The current implementation of takeOrdered in Spark quickly exceeds Spark's
   * driver memory for large values of K. The tree-reduce works better when the
   * number of partitions is large, or the size of individual partitions is large.
   *
   * @param num k, the number of elements to return
   * @param ord the implicit ordering for T
   * @param reduceTreeDepth Depth of reduce tree (governs number of rounds of reduce tasks)
   * @return an array of top elements
   */
  def takeOrderedTree[T: ClassTag](rdd: RDD[T], num: Int, reduceTreeDepth: Int = 2)(implicit ord: Ordering[T]): Array[T] = {
    if (num == 0) {
      Array.empty[T]
    }
    else {
      val mapRDDs = rdd.mapPartitions { items =>
        // Priority keeps the largest elements, so let's reverse the ordering.
        val queue = new BoundedPriorityQueue[T](num)(ord.reverse)
        queue ++= util.collection.Utils.takeOrdered(items, num)(ord)
        Iterator.single(queue)
      }
      if (mapRDDs.partitions.size == 0) {
        Array.empty[T]
      }
      else {
        //TODO: Revisit when tree-reduce gets moved to org.apache.spark.rdd.RDD in Spark 1.3
        mapRDDs.treeReduce({ (queue1, queue2) =>
          queue1 ++= queue2
          queue1
        }, reduceTreeDepth).toArray.sorted(ord)
      }
    }
  }
}
