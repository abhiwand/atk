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

    val sortedDataWeightPairs: RDD[(T, BigDecimal)] =
      dataWeightPairs.filter(distributionUtils.hasPositiveWeight).
        map({ case (data, weight) => (data, BigDecimal(weight)) }).sortByKey(ascending = true)

    val weightsOfPartitions: Array[BigDecimal] = sortedDataWeightPairs.mapPartitions(sumWeightsInPartition).collect()

    val totalWeight: BigDecimal = weightsOfPartitions.reduce(_ + _)

    if (totalWeight <= 0) {
      None
    }
    else {

      // the "median partition" is the partition the contains the median
      val (indexOfMedianPartition, weightInPrecedingPartitions) = findMedianPartition(weightsOfPartitions, totalWeight)
      val medianPartition: Array[(T, BigDecimal)] =
        sortedDataWeightPairs.mapPartitionsWithIndex(partitionSelector(indexOfMedianPartition), true).collect()

      // now we find where the median value of the dataset resides inside the median partition
      val weightPrecedingMedian = (totalWeight / 2) - weightInPrecedingPartitions
      var indexOfMedian: Int = 0
      var weightSoFar: BigDecimal = 0

      while (weightSoFar + medianPartition(indexOfMedian)._2 < weightPrecedingMedian) {
        weightSoFar += medianPartition(indexOfMedian)._2
        indexOfMedian += 1
      }

      Some(medianPartition(indexOfMedian)._1)
    }
  }

  // Sums the weights in an individual partition of an RDD[(T, BigDecimal)] where second coordinate is "weight'
  private def sumWeightsInPartition(it: Iterator[(T, BigDecimal)]): Iterator[BigDecimal] =
    if (it.nonEmpty) Iterator(it.map({ case (x, w) => w }).reduce(_ + _)) else Iterator(0)

  // Given a desired partition, an index of a partition and its iterator, this returns the iterator of the incoming
  // partition if it is the descired partition, and empty iterator if it is not the desired partition.
  private def partitionSelector(selectedPartition: Int)(index: Int, partitionIterator: Iterator[(T, BigDecimal)]): Iterator[(T, BigDecimal)] = {
    if (index == selectedPartition) partitionIterator else Iterator[(T, BigDecimal)]()
  }

  // Find the index of the partition that contains the median of the of the dataset, as well as the net weight of the
  // partitions that precede it.
  private def findMedianPartition(weightsOfPartitions: Array[BigDecimal], totalWeight: BigDecimal): (Int, BigDecimal) = {
    var currentPartition: Int = 0
    var weightInPrecedingPartitions: BigDecimal = 0
    while (weightInPrecedingPartitions + weightsOfPartitions(currentPartition) < totalWeight / 2) {
      currentPartition += 1
      weightInPrecedingPartitions += weightsOfPartitions(currentPartition)
    }
    (currentPartition, weightInPrecedingPartitions)
  }

}
