package com.intel.intelanalytics.engine.spark.statistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.reflect.ClassTag

/**
 * Calculate order statistics for any weighted RDD of data that provides an ordering function.
 * @param dataWeightPairs RDD of (data, weight) pairs
 * @param ordering Ordering on the data items
 * @tparam T The type of the data objects. It must have an ordering function in scope.
 */
class OrderStatistics[T: ClassTag](dataWeightPairs: RDD[(T, Double)])(implicit ordering: Ordering[T])
    extends Serializable {

  lazy val median: T = computeMedian

  /*
   * Computes the median via a sort and scan approach, although the nature of RDDs greatly complicates the "simple scan"
   *
   * TODO: investigate the use of a sort-free (aka "linear time") median algorithm, TRIB-3151
   */
  private def computeMedian: T = {

    val sortedDataWeightPairs: RDD[(T, Double)] = dataWeightPairs.sortByKey(ascending = true)

    def sumIterator(it: Iterator[(T, Double)]): Iterator[Double] =
      if (it.nonEmpty) Iterator(it.map({ case (x, w) => w }).reduce(_ + _)) else Iterator(0)

    val partitionWeights: Array[Double] = sortedDataWeightPairs.
      mapPartitions(sumIterator).collect()

    val totalWeight = partitionWeights.reduce(_ + _)

    require(totalWeight > 0, "Error: Cannot compute median in a distribution with 0 net weight.")

    var partitionIndexContainingMedian: Int = 0
    var weightSoFar: Double = 0

    while (weightSoFar + partitionWeights(partitionIndexContainingMedian) < totalWeight / 2) {
      partitionIndexContainingMedian += 1
      weightSoFar += partitionWeights(partitionIndexContainingMedian)
    }

    def partitionSelector(index: Int, partitionIterator: Iterator[(T, Double)]): Iterator[(T, Double)] = {
      if (index == partitionIndexContainingMedian) partitionIterator else Iterator[(T, Double)]()
    }

    val segmentContainingMedian: Array[(T, Double)] =
      sortedDataWeightPairs.mapPartitionsWithIndex(partitionSelector, true).collect()

    val weightPrecedingMedianInSegment = (totalWeight / 2) - weightSoFar
    var indexOfMedianInsideSegment: Int = 0
    var weightSoFarInsideSegment: Double = 0

    while (weightSoFarInsideSegment + segmentContainingMedian(indexOfMedianInsideSegment)._2
      < weightPrecedingMedianInSegment) {
      weightSoFarInsideSegment += segmentContainingMedian(indexOfMedianInsideSegment)._2
      indexOfMedianInsideSegment += 1
    }

    segmentContainingMedian(indexOfMedianInsideSegment)._1
  }

}
