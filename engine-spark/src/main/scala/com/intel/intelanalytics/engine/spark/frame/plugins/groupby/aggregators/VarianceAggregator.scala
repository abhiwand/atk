//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package com.intel.intelanalytics.engine.spark.frame.plugins.groupby.aggregators

import com.intel.intelanalytics.domain.schema.DataTypes
import com.intel.intelanalytics.domain.schema.DataTypes.DataType

/**
 * Counter used to compute sample variance incrementally.
 *
 * @param count Current count
 * @param mean Current mean
 * @param m2 Sum of squares of differences from the current mean
 */
case class VarianceCounter(count: Long, mean: CompensatedSum, m2: CompensatedSum) {
  require(count >= 0, "Count should be greater than zero")
}

/**
 * Counter used to calculate sums using the Kahan summation algorithm
 *
 * The Kahan summation algorithm (also known as compensated summation) reduces the numerical errors that 
 * occur when adding a sequence of finite precision floating point numbers. Numerical errors arise due to
 * truncation and rounding. These errors can lead to numerical instability when calculating variance.
 *
 * @see http://en.wikipedia.org/wiki/Kahan_summation_algorithm
 *      
 * @param value Numeric value being summed
 * @param delta Correction term for reducing numeric errors
 */
case class CompensatedSum(value: Double = 0d, delta: Double = 0d)

/**
 * Abstract class used to incrementally the compute sample variance and standard deviation
 *
 * This class uses the Kahan summation algorithm to avoid numeric instability when computing variance.
 * The algorithm is described in: "Scalable and Numerically Stable Descriptive Statistics in SystemML",
 * Tian et al, International Conference on Data Engineering 2012
 * 
 * @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
abstract class AbstractVarianceAggregator extends GroupByAggregator {

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = VarianceCounter

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = Double

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero: VarianceCounter = VarianceCounter(0L, CompensatedSum(), CompensatedSum())

  /**
   * Converts column value to Double
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = {
    if (columnValue != null)
      DataTypes.toDouble(columnValue)
    else
      Double.NaN
  }

  /**
   * Adds map value to incremental variance
   */
  override def add(varianceCounter: AggregateType, mapValue: ValueType): AggregateType = {
    if (mapValue.isNaN) {
      // omit value from calculation
      //TODO: Log to IAT EventContext once we figure out how to pass it to Spark workers
      println(s"WARN: Omitting NaNs from variance calculation in group-by")
      varianceCounter
    }
    else {
      val count = varianceCounter.count + 1L
      val delta = mapValue - varianceCounter.mean.value
      val mean = varianceCounter.mean.value + (delta / count)
      val m2 = varianceCounter.m2.value + (delta * (mapValue - mean))
      VarianceCounter(count, CompensatedSum(mean), CompensatedSum(m2))
    }
  }

  /**
   * Combines two Variance counters from two Spark partitions to update the incremental variance
   *
   * Uses the Kahan summation algorithm described in: "Scalable and Numerically Stable Descriptive Statistics in SystemML",
   * Tian et al, International Conference on Data Engineering 2012
   */
  override def merge(counter1: VarianceCounter, counter2: VarianceCounter): VarianceCounter = {
    val count = counter1.count + counter2.count
    val deltaMean = counter2.mean.value - counter1.mean.value
    val mean = incrementCompensatedSum(counter1.mean, CompensatedSum(deltaMean *counter2.count/count))
    val m2_sum = incrementCompensatedSum(counter1.m2, counter2.m2)
    val m2 = incrementCompensatedSum(m2_sum, CompensatedSum(deltaMean*deltaMean*counter1.count*counter2.count/count))
    VarianceCounter(count, mean, m2)
  }

  /**
   * Calculates the variance using the counts in VarianceCounter
   */
  def calculateVariance(counter: VarianceCounter): Double = {
    if (counter.count > 1) {
      counter.m2.value / (counter.count - 1)
    }
    else Double.NaN
  }

  // Increments the Kahan sum by adding two sums, and updating the correction term for reducing numeric errors
  private def incrementCompensatedSum(sum1: CompensatedSum, sum2: CompensatedSum): CompensatedSum = {
    val correctedSum2 = sum2.value + (sum1.delta + sum2.delta)
    val sum = sum1.value + correctedSum2
    val delta = correctedSum2 - (sum - sum1.value)
    CompensatedSum(sum, delta)
  }
}

/**
 * Aggregator for computing variance
 */
case class VarianceAggregator() extends AbstractVarianceAggregator {
  /**
   * Returns the variance
   */
  override def getResult(varianceCounter: VarianceCounter): Any = {
    val variance = super.calculateVariance(varianceCounter)
    if (variance.isNaN) null else variance //TODO: Revisit when data types support NaN
  }
}

/**
 * Aggregator for computing standard deviation
 */
case class StandardDeviationAggregator() extends AbstractVarianceAggregator {

  /**
   * Returns the standard deviation
   */
  override def getResult(varianceCounter: VarianceCounter): Any = {
    val variance = super.calculateVariance(varianceCounter)
    if (variance.isNaN) null else Math.sqrt(variance) //TODO: Revisit when data types support NaN
  }
}
