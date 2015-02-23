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
 * Counter used to compute the arithmetic mean incrementally.
 */
case class MeanCounter(count: Long, sum: Double) {
  require(count >= 0, "Count should be greater than zero")
}

/**
 *  Aggregator for incrementally computing the mean column value using Spark's aggregateByKey()
 *
 *  @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
case class MeanAggregator() extends GroupByAggregator {

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = MeanCounter

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = Double

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero: MeanCounter = MeanCounter(0L, 0d)

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
   * Adds map value to incremental mean counter
   */
  override def add(mean: AggregateType, mapValue: ValueType): AggregateType = {
    if (mapValue.isNaN) { // omit value from calculation
      //TODO: Log to IAT EventContext once we figure out how to pass it to Spark workers
      println(s"WARN: Omitting NaNs from mean calculation in group-by")
      mean
    }
    else {
      val sum = mean.sum + mapValue
      val count = mean.count + 1L
      MeanCounter(count, sum)
    }
  }

  /**
   * Merge two mean counters
   */
  override def merge(mean1: AggregateType, mean2: AggregateType) = {
    val count = mean1.count + mean2.count
    val sum = mean1.sum + mean2.sum
    MeanCounter(count, sum)
  }
  override def getResult(result: AggregateType): Any = result.sum / result.count

}
