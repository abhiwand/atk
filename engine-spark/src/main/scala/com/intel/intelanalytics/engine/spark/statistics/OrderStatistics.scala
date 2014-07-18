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

  /**
   * Option containing the median of the input distribution. The median is the least value X in the range of the
   * distribution so that the cumulative weight strictly below X is < 1/2  the total weight and the cumulative
   * distribution up to and including X is >= 1/2 the total weight.
   *
   * Values with non-positive weights are thrown out before the calculation is performed.
   * The option None is returned when the total weight is 0.
   */
  lazy val medianOption: Option[T] = computeMedian

  private val distributionUtils = new DistributionUtils[T]

  /*
   * Computes the median via a sort and scan approach, although the nature of RDDs greatly complicates the "simple scan"
   *
   * TODO: investigate the use of a sort-free (aka "linear time") median algorithm, TRIB-3151
   */
  private def computeMedian: Option[T] = {

    val sortedDataWeightPairs: RDD[(T, Double)] =
      dataWeightPairs.filter(distributionUtils.hasPositiveWeight).sortByKey(ascending = true)

    def sumIterator(it: Iterator[(T, Double)]): Iterator[Double] =
      if (it.nonEmpty) Iterator(it.map({ case (x, w) => w }).reduce(_ + _)) else Iterator(0)

    val partitionWeights: Array[Double] = sortedDataWeightPairs.
      mapPartitions(sumIterator).collect()

    val totalWeight = partitionWeights.reduce(_ + _)

    if (totalWeight <= 0) {
      None
    }
    else {

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

      Some(segmentContainingMedian(indexOfMedianInsideSegment)._1)
    }
  }

}
