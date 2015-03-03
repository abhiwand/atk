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
import org.scalatest.{ Matchers, FlatSpec }
import MatcherUtils._

class VarianceAggregatorTest extends FlatSpec with Matchers {
  val epsilon = 0.000000001

  "VarianceAggregator" should "output column value" in {
    val aggregator = VarianceAggregator()

    aggregator.mapFunction(10L, DataTypes.int64) should be(10d +- epsilon)
    aggregator.mapFunction(45d, DataTypes.float64) should be(45d +- epsilon)
    aggregator.mapFunction(0, DataTypes.int64) should be(0d +- epsilon)
    aggregator.mapFunction(null, DataTypes.int64).isNaN() should be(true)
  }
  "VarianceAggregator" should "throw an IllegalArgumentException if column value is not numeric" in {
    intercept[IllegalArgumentException] {
      val aggregator = VarianceAggregator()
      aggregator.mapFunction("test", DataTypes.string)
    }
  }
  "VarianceAggregator" should "increment the Variance counter" in {
    val aggregator = VarianceAggregator()

    val varianceCounter = VarianceCounter(5, CompensatedSum(15d), CompensatedSum(20d))
    val expectedResult = VarianceCounter(6, CompensatedSum(85 / 6d), CompensatedSum(245 / 6d))

    aggregator.add(varianceCounter, 10d) should equalWithTolerance(expectedResult, epsilon)
  }
  "VarianceAggregator" should "ignore NaN values in" in {
    val aggregator = VarianceAggregator()

    val varianceCounter = VarianceCounter(5, CompensatedSum(15d), CompensatedSum(20d))
    val expectedResult = VarianceCounter(5, CompensatedSum(15d), CompensatedSum(20d))

    aggregator.add(varianceCounter, Double.NaN) should equalWithTolerance(expectedResult, epsilon)
  }
  "VarianceAggregator" should "merge two Variance counters" in {
    val aggregator = VarianceAggregator()

    val varianceCounter1 = VarianceCounter(10, CompensatedSum(12d, 0.1), CompensatedSum(2d, 0.3))
    val varianceCounter2 = VarianceCounter(8, CompensatedSum(15d, 0.2), CompensatedSum(5d, 0.4))
    val expectedResult = VarianceCounter(18, CompensatedSum(13d + (13d / 30), 0d), CompensatedSum(47.7d, 0d))
    aggregator.merge(varianceCounter1, varianceCounter2) should equalWithTolerance(expectedResult, epsilon)
  }
  "VarianceAggregator" should "return variance" in {
    val aggregator = VarianceAggregator()
    aggregator.getResult(VarianceCounter(5, CompensatedSum(10d), CompensatedSum(8d))).asInstanceOf[Double] should be(2d +- epsilon)
  }
  "StandardDeviationAggregator" should "return standard deviation" in {
    val aggregator = StandardDeviationAggregator()
    aggregator.getResult(VarianceCounter(5, CompensatedSum(10d), CompensatedSum(8d))).asInstanceOf[Double] should be(Math.sqrt(2d) +- epsilon)
  }

}
