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
import com.intel.intelanalytics.domain.schema.{ Column, DataTypes }

import scala.util.Try

/**
 * Column and corresponding aggregator.
 */
case class ColumnAggregator(column: Column, columnIndex: Int, aggregator: GroupByAggregator)

object ColumnAggregator {

  /**
   * Get the column aggregator for histogram
   *
   * @param aggregationArgs Aggregation arguments
   * @param columnIndex Index of new column
   * @return Column aggregator for histogram
   */
  def getHistogramColumnAggregator(aggregationArgs: GroupByAggregationArgs, columnIndex: Int): ColumnAggregator = {
    val functionName = aggregationArgs.function
    require(functionName.matches("""HISTOGRAM\s*=\s*\{.*\}"""), s"Unsupported aggregation function for histogram: ${functionName}")

    val newColumnName = aggregationArgs.newColumnName.split("=")(0)
    val cutoffs = parseHistogramCutoffs(functionName.split("=")(1))
    ColumnAggregator(Column(newColumnName, DataTypes.vector), columnIndex, HistogramAggregator(cutoffs))
  }

  /**
   * Parse cutoffs for the histogram
   *
   * @param cutoffJson Json string with list of cutoffs
   * @return List of cutoffs
   */
  def parseHistogramCutoffs(cutoffJson: String): List[Double] = {
    import spray.json._
    import spray.json.DefaultJsonProtocol._

    val jsObject = Try(cutoffJson.parseJson.asJsObject).getOrElse({
      throw new IllegalArgumentException(s"cutoffs should be valid JSON: ${cutoffJson}")
    })

    jsObject.fields.get("cutoffs") match {
      case Some(x) => Try(x.convertTo[List[Double]]).getOrElse(throw new IllegalArgumentException(s"cutoffs should be numeric"))
      case _ => throw new IllegalArgumentException(s"cutoffs required for group_by histogram")
    }
  }
}
