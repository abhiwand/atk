//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2014 Intel Corporation All Rights Reserved.
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

package com.intel.intelanalytics.engine.spark.frame.plugins.groupby

import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.domain.schema.{Column, DataTypes}
import com.intel.intelanalytics.engine.spark.frame.plugins.groupby.GroupByMonoids._

private[spark] object GroupByAccumulators extends Serializable {

  case class ColumnAccumulator(column: Column, accumulator: GroupByAccumulator)

  trait GroupByAccumulator extends Serializable {

    type U

    /**
     * A monoid that should be used to generate initial "zero" values, as well as the type
     * of combinations of said values.
     */
    def monoid: Monoid[U]

    /**
     * A type that represents the raw values that get accumulated into the Monoid type
     */
    type V

    /**
     * The 'empty' or 'zero' or default value
     */
    val zero: U = monoid.zero

    /**
     * Map function used to convert the column data into the intermediate value
     * needed by the accumulator.
     *
     * For example, for counts, the map function would output one for each input value.
     *
     * @param columnValue Column value
     * @param columnDataType Column data type
     * @return Intermediate value
     */
    def mapValue(columnValue: Any, columnDataType: DataType): V

    /**
     * Adds the output of the map stage to the accumulator (e.g., increment the current count by one)
     *
     * @param accumulator Current accumulator value
     * @param inputValue Input value
     * @return Updated accumulator value
     */
    def accumulateValue(accumulator: U, inputValue: V): U

    /**
     * Combine the values of two accumulators (e.g., add two counts)
     *
     * @param accumulator1 First accumulator value
     * @param accumulator2 Second accumulator value
     * @return Combined accumulator value
     */
    def mergeAccumulators(accumulator1: U, accumulator2: U): U = monoid.op(accumulator1, accumulator2)

    /**
     * Returns the results of the accumulator
     *
     * For some accumulators, this involves casting the result to a Scala Any type.
     * Other accumulators output an intermediate value, so this method computes the
     * final result. For example, the arithmetic mean is represented as a count and sum,
     * so this method computes the mean by dividing the count by the sum.
     * 
     * @param result Results of accumulator (might be an intermediate value)
     * @return Final result
     */
    def getResult(result: U): Any = result

  }

  /**
   * Counts the number of values
   */
  object CountAccumulator extends GroupByAccumulator {
    override type U = Long
    override type V = Long

    override def monoid = new NumericAddition[Long]()

    override def mapValue(columnValue: Any, columnDataType: DataType): V = 1L

    override def accumulateValue(count: U, inputValue: V): U = count + inputValue
  }

  /**
   * Counts the number of distinct values
   */
  object DistinctCountAccumulator extends GroupByAccumulator {

    override type U = Set[Any]
    override type V = Any

    def monoid = AnySet

    override def mapValue(columnValue: Any, columnDataType: DataType): V = columnValue

    override def accumulateValue(set: U, columnValue: V): U = set + columnValue

    override def getResult(result: U): Any = result.size
  }

  /**
   * Computes the sum of values
   */
  class SumAccumulator[T: Numeric] extends GroupByAccumulator {

    val num = implicitly[Numeric[T]]

    override type U = T
    override type V = T

    override def monoid = new NumericAddition[T]()

    override def mapValue(columnValue: Any, columnDataType: DataType): V = {
      if (columnDataType == DataTypes.int32 || columnDataType == DataTypes.int64)
        DataTypes.toLong(columnValue).asInstanceOf[V]
      else
        DataTypes.toDouble(columnValue).asInstanceOf[V]
    }

    override def accumulateValue(sum: U, inputValue: V): T = num.plus(sum, inputValue)
  }

  /**
   * Computes the maximum value
   */
  object MaxAccumulator extends GroupByAccumulator {
    override type U = Any
    override type V = Any

    override def monoid = MaxMonoid

    override def mapValue(columnValue: Any, columnDataType: DataType): V = columnValue

    override def accumulateValue(max: U, inputValue: V): U = monoid.op(max, inputValue)

  }

  /**
   * Computes the minimum value
   */
  object MinAccumulator extends GroupByAccumulator {
    override type U = Any
    override type V = Any

    override def monoid = MinMonoid

    override def mapValue(columnValue: Any, columnDataType: DataType): V = columnValue

    override def accumulateValue(max: U, inputValue: V): U = monoid.op(max, inputValue)
  }

  /**
   * Computes the arithmetic mean by key
   */
  object MeanAccumulator extends GroupByAccumulator {
    override type U = MeanCounter
    override type V = Double

    override def monoid = ArithmeticMean

    override def mapValue(columnValue: Any, columnDataType: DataType): V = DataTypes.toDouble(columnValue)

    override def accumulateValue(mean: U, inputValue: V): U = {
      val sum = mean.sum + inputValue
      val count = mean.count + 1L
      MeanCounter(count, sum)
    }

    override def getResult(result: U): Any = result.sum / result.count

  }

  /**
   * Abstract class used to compute variance and standard deviation.
   */
  abstract class AbstractVarianceAccumulator extends GroupByAccumulator {
    override type U = VarianceCounter
    override type V = Double

    override def monoid = Variance

    override def mapValue(columnValue: Any, columnDataType: DataType): V = DataTypes.toDouble(columnValue)

    override def accumulateValue(variance: U, inputValue: V): U = {
      val count = variance.count + 1L
      val delta = inputValue - variance.mean
      val mean = variance.mean + (delta / count)
      val m2 = variance.m2 + (delta * (inputValue - mean))
      VarianceCounter(count, mean, m2)
    }

    def computeVariance(result: VarianceCounter): Double = {
      if (result.count > 1) {
        result.m2 / (result.count - 1)
      }
      else Double.NaN
    }
  }

  /**
   * Computes variance by key
   */
  object VarianceAccumulator extends AbstractVarianceAccumulator {
    override def getResult(result: VarianceCounter): Any = {
      super.computeVariance(result)
    }
  }


  /**
   * Computes standard deviation by key
   */
  object StandardDeviationAccumulator extends AbstractVarianceAccumulator {

    override def getResult(result: VarianceCounter): Any = {
      val variance = super.computeVariance(result)
      Math.sqrt(variance)
    }
  }

}

