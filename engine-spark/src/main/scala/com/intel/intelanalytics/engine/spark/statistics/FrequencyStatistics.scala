package com.intel.intelanalytics.engine.spark.statistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd._

/**
 * Object for calculating the frequency statistics of a collection of (data,weight) pairs, represented as an
 * RDD of (T,Double) pairs, where T is a type parameter.
 *
 * All data items with weights <= 0 are excluded from these calculations.
 *
 * @param dataWeightPairs RDD containing pairs (data, weight) where the each "data" entry is unique.
 * @param nonValue The "mode" of an empty collection.
 * @tparam T Datatype of values.
 *
 * @return triple consisting of the mode, its weight, and the total weight of all values in the input
 */
class FrequencyStatistics[T: ClassManifest](dataWeightPairs: RDD[(T, Double)], nonValue: T) extends Serializable {

  /**
   * Option for an item with maximum weight. If there is no item with positive weight,
   * the value None is used for the mode.
   */
  lazy val mode: Option[T] = modeItsWeightTotalWeightTriple._1

  /**
   * Option for the weight of a mode of the input. It is either strictly positive, or,
   * if there is no item with positive weight, weightOfMode is None .
   */
  lazy val weightOfMode: Option[Double] = modeItsWeightTotalWeightTriple._2

  /**
   * Sum all weights.
   */
  lazy val totalWeight: Double = modeItsWeightTotalWeightTriple._3

  private lazy val modeItsWeightTotalWeightTriple: (Option[T], Option[Double], Double) = generateMode()

  private val distributionUtils = new DistributionUtils[T]()

  private def generateMode(): (Option[T], Option[Double], Double) = {

    val acumulatorParam = new FrequencyStatsAccumulatorParam(nonValue)
    val initialValue = FrequencyStatsCounter(nonValue, -1, 0)

    val accumulator =
      dataWeightPairs.sparkContext.accumulator[FrequencyStatsCounter[T]](initialValue)(acumulatorParam)

    val dataWeightPairsSupport = dataWeightPairs.filter(distributionUtils.hasPositiveWeight)

    dataWeightPairsSupport.foreach(
      { case (value, weightAtValue) => accumulator.add(FrequencyStatsCounter(value, weightAtValue, weightAtValue)) })

    if (accumulator.value.totalWeight == 0) {
      (None, None, 0)
    }
    else {
      (Some(accumulator.value.mode), Some(accumulator.value.weightOfMode), accumulator.value.totalWeight)
    }
  }
}

/**
 * Class for accumulating frequency statistics in one pass over the data.
 * @param mode Value with the most weight seen so far.
 * @param weightOfMode The weight of the mode.
 * @param totalWeight Sum of the weights of all values seen so far.
 * @tparam T Type of the input data. (In particular, the type of the mode.)
 */
private case class FrequencyStatsCounter[T](mode: T, weightOfMode: Double, totalWeight: Double) extends Serializable

/**
 * Configures the spark accumulator for gathering frequency statistics.
 * @param nonValue The "mode" of an empty collection; the caller should ensure that frequency statistics are gathered
 *                 only on non-empty collections and thus the particular value of nonValue is irrelevant.
 * @tparam T The type of the input data.
 */
private class FrequencyStatsAccumulatorParam[T](nonValue: T) extends AccumulatorParam[FrequencyStatsCounter[T]] with Serializable {

  override def zero(initialValue: FrequencyStatsCounter[T]) = FrequencyStatsCounter(nonValue, -1, 0)

  override def addInPlace(r1: FrequencyStatsCounter[T], r2: FrequencyStatsCounter[T]): FrequencyStatsCounter[T] = {
    if (r1.weightOfMode > r2.weightOfMode) {
      FrequencyStatsCounter(r1.mode, r1.weightOfMode, r1.totalWeight + r2.totalWeight)
    }
    else {
      FrequencyStatsCounter(r2.mode, r2.weightOfMode, r1.totalWeight + r2.totalWeight)
    }
  }
}
