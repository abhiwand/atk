/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

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
