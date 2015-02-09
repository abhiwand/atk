//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014-2015 Intel Corporation All Rights Reserved.
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
 *  Aggregator for computing the sum of column values using Spark's aggregateByKey()
 *
 *  @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
class SumAggregator[T: Numeric] extends GroupByAggregator {

  val num = implicitly[Numeric[T]]

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = T

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = T

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero: T = num.zero

  /**
   * Converts column value to a Numeric
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = {
    if (columnValue == null) {
      num.zero
    }
    else if (columnDataType.isIntegral) {
      DataTypes.toLong(columnValue).asInstanceOf[ValueType]
    }
    else {
      DataTypes.toDouble(columnValue).asInstanceOf[ValueType]
    }
  }

  /**
   * Adds map value to sum
   */
  override def add(sum: AggregateType, mapValue: ValueType): T = num.plus(sum, mapValue)

  /**
   * Adds two sums
   */
  override def merge(sum1: AggregateType, sum2: AggregateType) = num.plus(sum1, sum2)
}
