package com.intel.intelanalytics.engine.spark.statistics

import com.intel.intelanalytics.engine.Rows._
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeMap

class TopKRDDFunctions {
  /**
   * Returns the top (or bottom) K distinct values by count for specified data column.
   *
   * @param frameRdd RDD for data frame
   * @param columnIndex Index of data column
   * @param k Number of entries to return
   * @param reverse Return bottom K entries if true, else return top K
   * @return Top (or bottom) K distinct values by count for specified column
   */
  def topK(frameRdd: RDD[Row], columnIndex: Int, k: Int, reverse: Boolean = false): RDD[Row] = {
    require(columnIndex >= 0, "label column index must be greater than or equal to zero")
    val groupedRDD = frameRdd.groupBy(row => row(columnIndex))
    val distinctCountRDD = groupedRDD.map({ case (distinctValue, rows) => (distinctValue, rows.size.toLong) })

    //Sort by descending order to get top K
    val isDescendingSort = !reverse

    // Efficiently get the top (or bottom) K entries by first sorting the top (or bottom) K entries in each partition
    val topKByPartition = distinctCountRDD.mapPartitions(countIterator => {
      sortTopKByValue(countIterator, k, isDescendingSort)
    })

    // Get the overall top (or bottom) K entries from partitions
    // Works when K*num_partitions fits in memory of single machine.
    // TODO: Figure out more efficient way to sort by value in Spark
    val topIterator = sortTopKByValue(topKByPartition.collect().toIterator, k, isDescendingSort)
    val topRows = for ((distinctValue, count) <- topIterator) yield Array(distinctValue, count)
    frameRdd.sparkContext.parallelize(topRows.toSeq)
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
  def sortTopKByValue[K, V: Ordering](inputIterator: Iterator[(K, V)],
                                      k: Int, descending: Boolean = false): Iterator[(K, V)] = {
    val ordering = if (descending) implicitly[Ordering[V]].reverse else implicitly[Ordering[V]]
    var treeMap = new TreeMap[V, K]()(ordering)

    inputIterator.foreach({
      case (key, value) =>
        treeMap += (value -> key)
        if (treeMap.size > k) treeMap = treeMap.dropRight(1)
    })

    val sortedK = for ((value, key) <- treeMap.toSeq) yield (key, value)
    sortedK.toIterator
  }
}
