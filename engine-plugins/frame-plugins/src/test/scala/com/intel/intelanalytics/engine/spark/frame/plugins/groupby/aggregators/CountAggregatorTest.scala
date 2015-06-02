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
import org.scalatest.{ FlatSpec, Matchers }

class CountAggregatorTest extends FlatSpec with Matchers {
  "CountAggregator" should "output 'one' for each column value" in {
    val aggregator = CountAggregator()

    aggregator.mapFunction("test", DataTypes.string) should equal(1L)
    aggregator.mapFunction(23d, DataTypes.float64) should equal(1L)
  }
  "CountAggregator" should "increment count" in {
    val aggregator = CountAggregator()

    aggregator.add(20L, 1L) should equal(21L)
    aggregator.add(20L, 2L) should equal(22L)
  }
  "CountAggregator" should "sum two counts" in {
    val aggregator = CountAggregator()

    aggregator.merge(5L, 100L) should equal(105L)
  }

}
