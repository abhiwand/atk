package com.intel.intelanalytics.engine.spark.statistics

import com.intel.intelanalytics.engine.Rows._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.PriorityQueue

private[spark] object TopKRDDFunctions extends Serializable {

  case class CountPair(key: Any, value: Long) extends Ordered[CountPair] {
    def compare(that: CountPair) = this.value compare that.value
  }

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
    val distinctCountRDD = frameRdd.map(row => (row(columnIndex), 1l)).reduceByKey((a, b) => a + b)

    //Sort by descending order to get top K
    val isDescendingSort = !reverse

    // Efficiently get the top (or bottom) K entries by first sorting the top (or bottom) K entries in each partition
    // This function uses a tree map instead of a bounded priority queue (despite the added overhead)
    // because we need to keep key-value pairs
    val topKByPartition = distinctCountRDD.mapPartitions(countIterator => {
      Iterator.single(sortTopKByValue(countIterator, k, isDescendingSort))
    }).reduce({ (topPartition1, topPartition2) =>
      mergeSortedSeqs(topPartition1, topPartition2, isDescendingSort)
    })

    // Get the overall top (or bottom) K entries from partitions
    // Works when K*num_partitions fits in memory of single machine.
    val topRows = topKByPartition.take(k).map(f => Array(f.key, f.value))
    frameRdd.sparkContext.parallelize(topRows)
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
  def sortTopKByValue(inputIterator: Iterator[(Any, Long)],
                      k: Int, descending: Boolean = false): Seq[CountPair] = {
    val ordering = if (descending) Ordering[CountPair].reverse else Ordering[CountPair]
    val priorityQueue = new PriorityQueue[CountPair]()(ordering)

    inputIterator.foreach(element => {
      priorityQueue.enqueue(CountPair(element._1, element._2))
      if (priorityQueue.size > k) priorityQueue.dequeue()
    })

    priorityQueue.reverse.dequeueAll.toSeq
  }

  /**
   * Merge two sorted sequences while maintaining sort order.
   *
   * @param sortedSeq1 First sorted sequence
   * @param sortedSeq2 Second sorted sequence
   * @param descending Sort in descending order if true, else sort in ascending order
   * @return Merged sorted sequence
   */
  private def mergeSortedSeqs(sortedSeq1: Seq[CountPair], sortedSeq2: Seq[CountPair],
                              descending: Boolean = false): Seq[CountPair] = {
    val ordering = if (descending) Ordering[CountPair].reverse else Ordering[CountPair]

    (sortedSeq1, sortedSeq2) match {
      case (seq1, Nil) => seq1
      case (Nil, seq2) => seq2
      case (seq1, seq2) => {
        if (ordering.lteq(seq1.head, seq2.head))
          seq1.head +: mergeSortedSeqs(seq1.tail, seq2, descending)
        else
          seq2.head +: mergeSortedSeqs(seq1, seq2.tail, descending)
      }
    }
  }
}
