package com.intel.intelanalytics.engine.spark.statistics

/**
 * Library for creating, cleaning and processing weighted data.
 * @tparam T Datatype for data elements.
 */
class DistributionUtils[T] extends Serializable {

  /**
   * True iff a double is a finite number.
   * @param double A Double.
   * @return True iff the double is a finite number..
   */
  def isFiniteNumber(double: Double) = { !double.isNaN && !double.isInfinite }

  /**
   * True iff the pair has weight that is a finite number > 0.
   * @param dataWeightPair A (data, weight) pair.
   * @return
   */
  def hasPositiveWeight(dataWeightPair: (T, Double)) = {
    val weight = dataWeightPair._2

    isFiniteNumber(weight) && (weight > 0)
  }

  /**
   * True if both the data and the weight are finite numbers.
   * @param dataWeightPair A (data, weight) pair.
   * @return
   */
  def isFiniteDataWeightDoublePair(dataWeightPair: (Double, Double)) = {

    isFiniteNumber(dataWeightPair._1) && isFiniteNumber(dataWeightPair._2)
  }
}