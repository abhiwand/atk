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
 * Counter used to compute variance incrementally.
 *
 * @param count Current count
 * @param mean Current mean
 * @param m2 Sum of squares of differences from the current mean
 */
case class VarianceCounter(count: Long, mean: Double, m2: Double) {
  require(count >= 0, "Count should be greater than zero")
}

/**
 * Abstract class used to incrementally the compute variance and standard deviation using Spark's aggregateByKey().
 *
 * @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
abstract class AbstractVarianceAggregator extends GroupByAggregator {

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = VarianceCounter

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = Double

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero: VarianceCounter = VarianceCounter(0L, 0d, 0d)

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
  override def add(variance: AggregateType, mapValue: ValueType): AggregateType = {
    if (mapValue.isNaN) { // omit value from calculation
      //TODO: Log to IAT EventContext once we figure out how to pass it to Spark workers
      println(s"WARN: Omitting NaNs from variance calculation in group-by")
      variance
    }
    else {
      val count = variance.count + 1L
      val delta = mapValue - variance.mean
      val mean = variance.mean + (delta / count)
      val m2 = variance.m2 + (delta * (mapValue - mean))
      VarianceCounter(count, mean, m2)
    }
  }

  /**
   * Combines two VarianceCounter objects used to compute the variance incrementally.
   */
  override def merge(variance1: VarianceCounter, variance2: VarianceCounter): VarianceCounter = {
    val count = variance1.count + variance2.count
    val mean = ((variance1.mean * variance1.count) + (variance2.mean * variance2.count)) / count
    val deltaMean = variance1.mean - variance2.mean
    val m2 = variance1.m2 + variance2.m2 + (deltaMean * deltaMean * count) / count
    VarianceCounter(count, mean, m2)
  }

  /**
   * Calculates the variance using the counts in VarianceCounter
   */
  def calculateVariance(variance: VarianceCounter): Double = {
    if (variance.count > 1) {
      variance.m2 / (variance.count - 1)
    }
    else Double.NaN
  }
}

/**
 * Aggregator for computing variance
 */
case class VarianceAggregator() extends AbstractVarianceAggregator {
  /**
   * Returns the variance
   */
  override def getResult(variance: VarianceCounter): Any = {
    super.calculateVariance(variance)
  }
}

/**
 * Aggregator for computing standard deviation
 */
case class StandardDeviationAggregator() extends AbstractVarianceAggregator {

  /**
   * Returns the standard deviation
   */
  override def getResult(variance: VarianceCounter): Any = {
    val stddev = super.calculateVariance(variance)
    Math.sqrt(stddev)
  }
}
