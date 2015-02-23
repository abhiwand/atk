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

    aggregator.add(VarianceCounter(5, 15d, 20d), 10d) should equalWithTolerance(VarianceCounter(6, 85 / 6d, 245 / 6d), epsilon)
    aggregator.add(VarianceCounter(10, -5d, 3.5d), 0.5d) should equalWithTolerance(VarianceCounter(11, -4.5d, 31d), epsilon)
  }
  "VarianceAggregator" should "ignore NaN values in" in {
    val aggregator = VarianceAggregator()

    aggregator.add(VarianceCounter(5, 15d, 20d), Double.NaN) should equalWithTolerance(VarianceCounter(5, 15d, 20d), epsilon)
  }
  "VarianceAggregator" should "merge two Variance counters" in {
    val aggregator = VarianceAggregator()
    aggregator.merge(VarianceCounter(10, 12d, 2d), VarianceCounter(8, 15d, 5d)) should equalWithTolerance(VarianceCounter(18, 40 / 3d, 16d), epsilon)
  }
  "VarianceAggregator" should "return variance" in {
    val aggregator = VarianceAggregator()
    aggregator.getResult(VarianceCounter(5, 10d, 8d)).asInstanceOf[Double] should be(2d +- epsilon)
  }
  "StandardDeviationAggregator" should "return standard deviation" in {
    val aggregator = StandardDeviationAggregator()
    aggregator.getResult(VarianceCounter(5, 10d, 8d)).asInstanceOf[Double] should be(Math.sqrt(2d) +- epsilon)
  }

}
