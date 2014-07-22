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
 * @tparam T Datatype of values.
 *
 * @return triple consisting of the mode, its weight, and the total weight of all values in the input
 */
class FrequencyStatistics[T: ClassManifest](dataWeightPairs: RDD[(T, Double)]) extends Serializable {

  /**
   * Option for an item with maximum weight. If there is no item with positive weight,
   * the value None is used for the mode.
   */
  lazy val mode: Option[T] = modeItsWeightTotalWeightTriple._1

  /**
   * Option for the weight of a mode of the input. It is either strictly positive, or,
   * if there is no item with positive weight, weightOfMode is 0 .
   */
  lazy val weightOfMode: Double = modeItsWeightTotalWeightTriple._2

  /**
   * Sum all weights.
   */
  lazy val totalWeight: Double = modeItsWeightTotalWeightTriple._3

  private lazy val modeItsWeightTotalWeightTriple: (Option[T], Double, Double) = generateMode()

  private val distributionUtils = new DistributionUtils[T]()

  private def generateMode(): (Option[T], Double, Double) = {

    val acumulatorParam = new FrequencyStatsAccumulatorParam[T]()
    val initialValue = FrequencyStatsCounter[T](None, 0, 0)

    val accumulator =
      dataWeightPairs.sparkContext.accumulator[FrequencyStatsCounter[T]](initialValue)(acumulatorParam)

    val dataWeightPairsSupport = dataWeightPairs.filter(distributionUtils.hasPositiveWeight)

    dataWeightPairsSupport.foreach(
      { case (value, weightAtValue) => accumulator.add(FrequencyStatsCounter(Some(value), weightAtValue, weightAtValue)) })

    (accumulator.value.mode, accumulator.value.weightOfMode, accumulator.value.totalWeight)

  }
}

/**
 * Class for accumulating frequency statistics in one pass over the data.
 * @param mode Option for value with the most weight seen so far. None when run over empty data.
 * @param weightOfMode The weight of the mode. 0 when run over empty data.
 * @param totalWeight Sum of the weights of all values seen so far.
 * @tparam T Type of the input data. (In particular, the type of the mode.)
 */
private case class FrequencyStatsCounter[T](mode: Option[T], weightOfMode: Double, totalWeight: Double)
  extends Serializable

/**
 * Configures the spark accumulator for gathering frequency statistics.
 * @tparam T The type of the input data.
 */
private class FrequencyStatsAccumulatorParam[T] extends AccumulatorParam[FrequencyStatsCounter[T]] with Serializable {

  override def zero(initialValue: FrequencyStatsCounter[T]) = FrequencyStatsCounter(None, 0, 0)

  override def addInPlace(stats1: FrequencyStatsCounter[T], stats2: FrequencyStatsCounter[T]): FrequencyStatsCounter[T] = {
    if (stats1.mode.isEmpty) {
      stats2
    }
    else if (stats2.mode.isEmpty) {
      stats1
    }
    else {
      if (stats1.weightOfMode > stats2.weightOfMode) {
        FrequencyStatsCounter(stats1.mode, stats1.weightOfMode, stats1.totalWeight + stats2.totalWeight)
      }
      else {
        FrequencyStatsCounter(stats2.mode, stats2.weightOfMode, stats1.totalWeight + stats2.totalWeight)
      }
    }
  }
}
