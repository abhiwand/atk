package com.intel.intelanalytics.engine.spark.statistics


/**
 * Library for creating, cleaning and processing weighted data.
 * @tparam T Datatype for data elements.
 */
class DistributionUtils[T] extends Serializable{

  def hasPositiveWeight(dataWeightPair : (T, Double)) = {
    val weight = dataWeightPair._2

    if (weight equals Double.NaN) false else (weight > 0)
  }
}