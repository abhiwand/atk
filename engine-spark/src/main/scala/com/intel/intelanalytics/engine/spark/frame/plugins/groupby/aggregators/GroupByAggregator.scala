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
 *  Trait for group-by aggregators
 *
 *  This trait defines methods needed to implement aggregators, e.g. counts and sums,
 *  using Spark's aggregateByKey(). aggregateByKey() aggregates the values of each key using
 *  an initial "zero value", an operation which merges an input value V into an aggregate value U,
 *  and an operation for merging two U's.
 *
 *  @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
trait GroupByAggregator extends Serializable {

  /**
   * A type that represents the aggregate results (e.g., count)
   *
   * Corresponds to type U in Spark's aggregateByKey()
   */
  type AggregateType

  /**
   * A type that represents output of the map function  (e.g., ones when doing count)
   *
   * Corresponds to type V in Spark's aggregateByKey()
   */
  type ValueType

  /**
   * The 'empty' or 'zero' or default value for the aggregator
   */
  def zero: AggregateType

  /**
   * Map function that transforms column values in each row into the input expected by Spark's aggregateByKey()
   *
   * For example, for counts, the map function would output 'one' for each map value.
   *
   * @param columnValue Column value
   * @param columnDataType Column data type
   * @return Input value for aggregator
   */
  def mapFunction(columnValue: Any, columnDataType: DataType): ValueType

  /**
   * Adds map value to aggregate value
   *
   * This function corresponds to Spark's aggregateByKey() operation which merges
   * the map value V into an aggregate value U. For example, incrementing the current count by one.
   *
   * @param aggregateValue Current aggregate value
   * @param mapValue Input value
   * @return Updated aggregate value
   */
  def add(aggregateValue: AggregateType, mapValue: ValueType): AggregateType

  /**
   * Combines two aggregate values
   *
   * This function corresponds to Spark's aggregateByKey() operation which merges for merging two aggregate values (U's)
   * For example, adding two counts
   *
   * @param aggregateValue1 First aggregate value
   * @param aggregateValue2 Second aggregate value
   * @return Combined aggregate value
   */
  def merge(aggregateValue1: AggregateType, aggregateValue2: AggregateType): AggregateType

  /**
   * Returns the results of the aggregator
   *
   * For some aggregators, this involves casting the result to a Scala Any type.
   * Other aggregators output an intermediate value, so this method computes the
   * final result. For example, the arithmetic mean is represented as a count and sum,
   * so this method computes the mean by dividing the count by the sum.
   *
   * @param result Results of aggregator (might be an intermediate value)
   * @return Final result
   */
  def getResult(result: AggregateType): Any = result

}
