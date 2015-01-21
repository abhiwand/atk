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

import com.intel.intelanalytics.domain.schema.DataTypes

/**
 * Monoids that define how to combine accumulators such as counts, and sums in the group by functions.
 *
 * Monoids are a simple algebraic structure that defines things that can be combined to form
 * new things of the same type.
 */
private[spark] object GroupByMonoids extends Serializable {

  //TODO: Move to another file? Find an existing library that defines Monoid already?
  /**
   * A monoid is an algebraic structure with a single associative binary operation and an identity element.
   */
  trait Monoid[T] {

    /**
     * The 'empty' or 'zero' or default value
     */
    def zero: T

    /**
     * Combines two values of the same type using the monoid's operator, and produces
     * another item of the same type.
     */
    def op(left: T, right: T): T
  }

  /**
   * A generic Monoid for adding two numbers
   *
   * @tparam T Numeric type
   */
  class NumericAddition[T: Numeric]() extends Monoid[T] {
    val num = implicitly[Numeric[T]]

    override def zero: T = num.zero

    /**
     * Adds two numbers.
     */
    override def op(left: T, right: T): T = num.plus(left, right)
  }

  /**
   * A generic Monoid instance for Sets.
   */
  class SetMonoid[T] extends Monoid[Set[T]] {

    override def zero: Set[T] = Set.empty

    /**
     * Creates a new set by combining all the elements of the two input sets.
     */
    override def op(left: Set[T], right: Set[T]): Set[T] = left.union(right)
  }

  object AnySet extends SetMonoid[Any]

  /**
   * A Monoid instance that returns the maximum value for supported data types.
   */
  object MaxMonoid extends Monoid[Any] {

    override def zero: Any = null

    /**
     * Returns the maximum value for supported data types
     */
    override def op(left: Any, right: Any): Any = {
      if (left != null && DataTypes.compare(left, right) >= 0)
        left
      else right
    }
  }

  /**
   * A Monoid instance that returns the minimum value for supported data types.
   */
  object MinMonoid extends Monoid[Any] {

    override def zero: Any = null

    /**
     * Returns the minimum value for supported data types
     */
    override def op(left: Any, right: Any): Any = {
      if (left != null && DataTypes.compare(left, right) <= 0)
        left
      else right
    }
  }

  /**
   * Counter used to compute the arithmetic mean incrementally.
   */
  case class MeanCounter(count: Long, sum: Double) {
    require(count >= 0, "Count should be greater than zero")
  }

  /**
   * A Monoid instance for computing the arithmetic mean incrementally.
   */
  object ArithmeticMean extends Monoid[MeanCounter] {

    override def zero: MeanCounter = MeanCounter(0L, 0d)

    /**
     * Computes the total count and sum for two MeanCounter objects.
     */
    override def op(left: MeanCounter, right: MeanCounter): MeanCounter = {
      val count = left.count + right.count
      val sum = left.sum + right.sum
      MeanCounter(count, sum)
    }
  }

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
   * A Monoid instance for computing variance incrementally
   */
  object Variance extends Monoid[VarianceCounter] {

    override def zero: VarianceCounter = VarianceCounter(0L, 0d, 0d)

    /**
     * Combines two VarianceCounter objects used to compute the variance incrementally.
     */
    override def op(left: VarianceCounter, right: VarianceCounter): VarianceCounter = {
      val count = left.count + right.count
      val mean = ((left.mean * left.count) + (right.mean * right.count)) / count
      val deltaMean = left.mean - right.mean
      val m2 = left.m2 + right.m2 + (deltaMean * deltaMean * count) / count
      VarianceCounter(count, mean, m2)
    }
  }

}
