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
      mergeSortedSeqs(topPartition1, topPartition2, Seq(), k, isDescendingSort)
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
      if (priorityQueue.size > k)  priorityQueue.dequeue()
    })

    priorityQueue.reverse.dequeueAll.toSeq
  }

  private def mergeSortedSeqs(topKSeq1: Seq[CountPair], topKSeq2: Seq[CountPair], accumulator: Seq[CountPair],
                        k: Int, descending: Boolean = false): Seq[CountPair] = {
    val ordering = if (descending) implicitly[Ordering[CountPair]].reverse else implicitly[Ordering[CountPair]]

    (topKSeq1, topKSeq2) match {
      case (seq1, Nil) => accumulator ++ seq1
      case (Nil, seq2) => accumulator ++ seq2
      case (seq1, seq2) => {
        val (newTopKSeq1, newTopKSeq2, newAccumulator) = getNextMergeSeqs(seq1, seq2, accumulator, ordering)
        mergeSortedSeqs(newTopKSeq1, newTopKSeq2, newAccumulator, k, descending)
      }
    }
  }

  private def getNextMergeSeqs(seq1: Seq[CountPair],
                               seq2: Seq[CountPair],
                               accumulator: Seq[CountPair],
                               ordering: Ordering[CountPair]): (Seq[CountPair], Seq[CountPair], Seq[CountPair]) = {
    val (head1, tailSeq1) = (seq1.head, seq1.tail)
    val (head2, tailSeq2) = (seq2.head, seq2.tail)
    val (newSeq1, newSeq2, newAccumulator) = if (ordering.lteq(head1, head2))
      (tailSeq1, seq2, accumulator :+ head1)
    else
      (seq1, tailSeq2, accumulator :+ head2)
    (newSeq1, newSeq2, newAccumulator)
  }


}
