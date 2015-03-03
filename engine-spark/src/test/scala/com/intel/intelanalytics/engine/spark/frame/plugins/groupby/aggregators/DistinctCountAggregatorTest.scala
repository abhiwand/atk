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

class DistinctCountAggregatorTest extends FlatSpec with Matchers {
  "DistinctCountAggregator" should "output column value" in {
    val aggregator = DistinctCountAggregator()

    aggregator.mapFunction("test1", DataTypes.string) should equal("test1")
    aggregator.mapFunction(10L, DataTypes.float64) should equal(10L)

  }
  "DistinctCountAggregator" should "add value to set" in {
    val aggregator = DistinctCountAggregator()
    val set: Set[Any] = Set("test1", "test2", "test3")
    aggregator.add(set, "test4") should contain theSameElementsAs Set("test1", "test2", "test3", "test4")
    aggregator.add(set, "test1") should contain theSameElementsAs Set("test1", "test2", "test3")
  }
  "DistinctCountAggregator" should "merge two sets" in {
    val aggregator = DistinctCountAggregator()

    val set1: Set[Any] = Set("test1", "test2", "test3")
    val set2: Set[Any] = Set(1, 2, 4)
    aggregator.merge(set1, set2) should contain theSameElementsAs Set("test1", "test2", "test3", 1, 2, 4)
    aggregator.merge(Set.empty[Any], set2) should contain theSameElementsAs Set(1, 2, 4)
  }
  "DistinctCountAggregator" should "return count of distinct values" in {
    val aggregator = DistinctCountAggregator()

    val set1: Set[Any] = Set("test1", "test2", "test3")
    aggregator.getResult(set1) should equal(3)
    aggregator.getResult(Set.empty[Any]) should equal(0)
  }

}
