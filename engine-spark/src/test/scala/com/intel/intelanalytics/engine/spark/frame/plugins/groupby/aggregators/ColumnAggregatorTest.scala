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

import com.intel.intelanalytics.domain.frame.GroupByAggregationArgs
import com.intel.intelanalytics.domain.schema.{ DataTypes, Column }
import org.scalatest.{ Matchers, FlatSpec }

class ColumnAggregatorTest extends FlatSpec with Matchers {

  "getHistogramColumnAggregator" should "return the column aggregator for histogram" in {
    val groupByArguments = GroupByAggregationArgs("HISTOGRAM={\"cutoffs\": [0,2,4] }", "col_2", "col_histogram")

    val columnAggregator = ColumnAggregator.getHistogramColumnAggregator(groupByArguments, 5)
    val expectedResults = ColumnAggregator(Column("col_histogram", DataTypes.vector), 5, HistogramAggregator(List(0d, 2d, 4d)))

    columnAggregator should equal(expectedResults)
  }

  "getHistogramColumnAggregator" should "throw an IllegalArgumentException if cutoffs are missing" in {
    val groupByArguments = GroupByAggregationArgs("HISTOGRAM", "col_2", "col_histogram")

    intercept[IllegalArgumentException] {
      ColumnAggregator.getHistogramColumnAggregator(groupByArguments, 5)
    }
  }

  "getHistogramColumnAggregator" should "throw an IllegalArgumentException if cutoffs are not numeric" in {
    val groupByArguments = GroupByAggregationArgs("HISTOGRAM={\"cutoffs\": [\"a\",\"b\"] }", "col_2", "col_histogram")

    intercept[IllegalArgumentException] {
      ColumnAggregator.getHistogramColumnAggregator(groupByArguments, 5)
    }
  }

  "parseHistogramCutoffs" should "return the cutoffs" in {
    val cutoffs = ColumnAggregator.parseHistogramCutoffs("{\"cutoffs\": [4,5,6] }")
    cutoffs should equal(List(4d, 5d, 6d))
  }

  "parseHistogramCutoffs" should "throw an IllegalArgumentException if cutoffs are not numeric" in {
    intercept[IllegalArgumentException] {
      ColumnAggregator.parseHistogramCutoffs("{\"cutoffs\": [\"a\",\"b\"] }")
    }
  }

  "parseHistogramCutoffs" should "throw an IllegalArgumentException if cutoffs are missing" in {
    intercept[IllegalArgumentException] {
      ColumnAggregator.parseHistogramCutoffs("{\"missing\": [4,5,6] }")
    }
  }

  "parseHistogramCutoffs" should "throw an IllegalArgumentException if cutoffs are not valid json" in {
    intercept[IllegalArgumentException] {
      ColumnAggregator.parseHistogramCutoffs("=={\"missing\": [4,5,6] }")
    }
  }
}
