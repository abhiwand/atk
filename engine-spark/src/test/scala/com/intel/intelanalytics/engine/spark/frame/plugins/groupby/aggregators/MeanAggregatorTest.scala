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

class MeanAggregatorTest extends FlatSpec with Matchers {
  val epsilon = 0.000000001

  "MeanAggregator" should "output column value" in {
    val aggregator = MeanAggregator()

    aggregator.mapFunction(10L, DataTypes.int64) should be(10d +- epsilon)
    aggregator.mapFunction(45d, DataTypes.float64) should be(45d +- epsilon)
    aggregator.mapFunction(0, DataTypes.int64) should be(0d +- epsilon)
    aggregator.mapFunction(null, DataTypes.int64).isNaN() should be(true)
  }
  "MeanAggregator" should "throw an IllegalArgumentException if column value is not numeric" in {
    intercept[IllegalArgumentException] {
      val aggregator = MeanAggregator()
      aggregator.mapFunction("test", DataTypes.string)
    }
  }
  "MeanAggregator" should "increment the mean counter" in {
    val aggregator = MeanAggregator()

    aggregator.add(MeanCounter(5, 15d), 4d) should equalWithTolerance(MeanCounter(6, 19d), epsilon)
    aggregator.add(MeanCounter(10, -5d), 4d) should equalWithTolerance(MeanCounter(11, -1d), epsilon)
  }
  "MeanAggregator" should "ignore NaN values in" in {
    val aggregator = MeanAggregator()

    aggregator.add(MeanCounter(5, 15d), Double.NaN) should equalWithTolerance(MeanCounter(5, 15d), epsilon)
    aggregator.add(MeanCounter(10, -5d), 4d) should equalWithTolerance(MeanCounter(11, -1d), epsilon)
  }
  "MeanAggregator" should "merge two mean counters" in {
    val aggregator = MeanAggregator()
    aggregator.merge(MeanCounter(10, -5d), MeanCounter(4, 15d)) should equalWithTolerance(MeanCounter(14, 10d), epsilon)
  }
  "MeanAggregator" should "return mean value" in {
    val aggregator = MeanAggregator()
    aggregator.getResult(MeanCounter(4, 25)).asInstanceOf[Double] should be(6.25d +- epsilon)
  }

}
