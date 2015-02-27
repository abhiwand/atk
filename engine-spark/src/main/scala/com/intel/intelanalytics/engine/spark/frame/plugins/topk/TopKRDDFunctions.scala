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

package com.intel.intelanalytics.engine.spark.frame.plugins.topk

import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.Rows._
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.NumericValidationUtils
import com.intel.intelanalytics.engine.spark.frame.plugins.statistics.descriptives.ColumnStatistics
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.PriorityQueue

/**
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
private[spark] object TopKRDDFunctions extends Serializable {

  case class CountPair(key: Any, value: Double) extends Ordered[CountPair] {
    def compare(that: CountPair) = this.value compare that.value
  }

  /**
   * Returns the top (or bottom) K distinct values by count for specified data column.
   *
   * @param frameRDD RDD for data frame
   * @param dataColumnIndex Index of data column
   * @param k Number of entries to return
   * @param useBottomK Return bottom K entries if true, else return top K
   * @param weightsColumnIndexOption Option for index of column providing the weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @return Top (or bottom) K distinct values by count for specified column
   */
  def topK(frameRDD: RDD[Row], dataColumnIndex: Int, k: Int, useBottomK: Boolean = false,
           weightsColumnIndexOption: Option[Int] = None,
           weightsTypeOption: Option[DataType] = None): RDD[Row] = {
    require(dataColumnIndex >= 0, "label column index must be greater than or equal to zero")

    val dataWeightPairs =
      ColumnStatistics.getDataWeightPairs(dataColumnIndex, weightsColumnIndexOption, weightsTypeOption, frameRDD)
        .filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) })

    val distinctCountRDD = dataWeightPairs.reduceByKey((a, b) => a + b)

    //Sort by descending order to get top K
    val isDescendingSort = !useBottomK

    // Efficiently get the top (or bottom) K entries by first sorting the top (or bottom) K entries in each partition
    // This function uses a tree map instead of a bounded priority queue (despite the added overhead)
    // because we need to keep key-value pairs
    val topKByPartition = distinctCountRDD.mapPartitions(countIterator => {
      Iterator(sortTopKByValue(countIterator, k, isDescendingSort))
    }).reduce({ (topPartition1, topPartition2) =>
      mergeTopKSortedSeqs(topPartition1, topPartition2, isDescendingSort, k)
    })

    // Get the overall top (or bottom) K entries from partitions
    // Works when K*num_partitions fits in memory of single machine.
    val topRows = topKByPartition.map(f => Array(f.key, f.value))
    frameRDD.sparkContext.parallelize(topRows)
  }

  /**
   * Returns top K entries sorted by value.
   *
   * The sort ordering may either be ascending or descending.
   *
   * @param inputIterator Iterator of key-value pairs to sort
   * @param k Number of top sorted entries to return
   * @param descending Sort in descending order if true, else sort in ascending order
   * @return Top K sorted entries
   */
  def sortTopKByValue(inputIterator: Iterator[(Any, Double)],
                      k: Int, descending: Boolean = false): Seq[CountPair] = {
    val ordering = if (descending) Ordering[CountPair].reverse else Ordering[CountPair]
    val priorityQueue = new PriorityQueue[CountPair]()(ordering)

    inputIterator.foreach(element => {
      priorityQueue.enqueue(CountPair(element._1, element._2))
      if (priorityQueue.size > k) priorityQueue.dequeue()
    })

    priorityQueue.reverse.dequeueAll
  }

  /**
   * Merge two sorted sequences while maintaining sort order, and return topK.
   *
   * @param sortedSeq1 First sorted sequence
   * @param sortedSeq2 Second sorted sequence
   * @param descending Sort in descending order if true, else sort in ascending order
   * @param k Number of top sorted entries to return
   * @return Merged sorted sequence with topK entries
   */
  private def mergeTopKSortedSeqs(sortedSeq1: Seq[CountPair], sortedSeq2: Seq[CountPair],
                                  descending: Boolean = false, k: Int): Seq[CountPair] = {
    // Previously tried recursive and non-recursive merge but Scala sort turned out to be the fastest.
    // Recursive merge was overflowing the stack for large K
    val ordering = if (descending) Ordering[CountPair].reverse else Ordering[CountPair]
    (sortedSeq1 ++ sortedSeq2).sorted(ordering).take(k)

  }
}
