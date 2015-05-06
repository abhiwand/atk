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

class MinAggregatorTest extends FlatSpec with Matchers {
  "MinAggregator" should "output column value" in {
    val aggregator = MinAggregator()

    aggregator.mapFunction("test1", DataTypes.string) should equal("test1")
    aggregator.mapFunction(10L, DataTypes.int64) should equal(10L)

  }
  "MinAggregator" should "return the minimum value in" in {
    val aggregator = MinAggregator()

    aggregator.add(10, 15) should equal(10)
    aggregator.add(-4, -10) should equal(-10)
    aggregator.add(100, 0) should equal(0)
    aggregator.add("test1", "abc") should equal("abc")
  }
  "MinAggregator" should "merge two minimum values" in {
    val aggregator = MinAggregator()

    aggregator.merge(23, 15) should equal(15)
    aggregator.merge(67, -10) should equal(-10)
    aggregator.merge(100, 0) should equal(0)
    aggregator.merge("abc", "def") should equal("abc")
  }
  "MinAggregator" should "ignore null values" in {
    val aggregator = MinAggregator()

    aggregator.add(100, null) should equal(100)
    aggregator.add(null, 10) should equal(10)
    aggregator.merge(-10, null) should equal(-10)
    aggregator.merge(null, 30) should equal(30)
  }

}
