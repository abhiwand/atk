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
import com.intel.testutils.MatcherUtils._

import org.scalatest.{ Matchers, FlatSpec }

class HistogramAggregatorTest extends FlatSpec with Matchers {
  val epsilon = 0.000001

  "HistogramAggregator" should "return the bin index for the column value based on the cutoffs" in {
    val aggregator = HistogramAggregator(List(0, 3, 6))

    aggregator.mapFunction(null, DataTypes.float64) should equal(-1)
    aggregator.mapFunction(-3, DataTypes.float64) should equal(0)
    aggregator.mapFunction(2.3, DataTypes.float64) should equal(0)
    aggregator.mapFunction(4.5d, DataTypes.float64) should equal(1)
    aggregator.mapFunction(10, DataTypes.float64) should equal(1)
  }

  "HistogramAggregator" should "increment count for the bin corresponding to the bin index" in {
    val aggregator = HistogramAggregator(List(0, 3, 6))

    aggregator.add(Array(1d, 2d, 3d), 2) should equalWithTolerance(Array(1d, 2d, 4d), epsilon)
  }

  "HistogramAggregator" should "not increment count if bin index is out-of-range" in {
    val aggregator = HistogramAggregator(List(0, 3, 6))

    aggregator.add(Array(1d, 2d, 3d), -1) should equalWithTolerance(Array(1d, 2d, 3d), epsilon)
  }

  "HistogramAggregator" should "sum two binned lists" in {
    val aggregator = HistogramAggregator(List(0, 3, 6))

    aggregator.merge(Array(1d, 2d, 3d), Array(4d, 0d, 6d)) should equalWithTolerance(Array(5d, 2d, 9d), epsilon)
  }

  "HistogramAggregator" should "return a vector with the percentage of observations found in each bin" in {
    val aggregator = HistogramAggregator(List(0, 3, 6))
    val histogramDensity = aggregator.getResult(Array(5d, 2d, 9d)).asInstanceOf[Vector[Double]]

    histogramDensity.toArray should equalWithTolerance(Array(5 / 16d, 2 / 16d, 9 / 16d), epsilon)
  }

}
