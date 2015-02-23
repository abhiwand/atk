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

import com.intel.intelanalytics.domain.schema.DataTypes.DataType

/**
 * Aggregator for counting distinct column values using Spark's aggregateByKey().
 *
 * @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
case class DistinctCountAggregator() extends GroupByAggregator {

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = Set[Any]

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = Any

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero = Set.empty[Any]

  /**
   * Outputs column value
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = columnValue

  /**
   * Add map value to set which stores distinct column values.
   */
  override def add(set: AggregateType, mapValue: ValueType): AggregateType = set + mapValue

  /**
   * Merge two sets.
   */
  override def merge(set1: AggregateType, set2: AggregateType) = set1 ++ set2

  /**
   * Returns count of distinct column values
   */
  override def getResult(set: AggregateType): Any = set.size.toLong // toLong needed to avoid casting errors when writing to Parquet
}
