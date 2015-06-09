/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package com.intel.intelanalytics.engine.spark.frame.plugins.statistics.descriptives

import com.intel.intelanalytics.domain.frame.{ FrameReference, ColumnSummaryStatisticsArgs, ColumnSummaryStatisticsReturn }
import com.intel.intelanalytics.domain.schema.DataTypes.DataType
import com.intel.intelanalytics.engine.plugin.Invocation
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Calculate summary statistics of the specified column.
 */
class ColumnSummaryStatisticsPlugin extends SparkCommandPlugin[ColumnSummaryStatisticsArgs, ColumnSummaryStatisticsReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/column_summary_statistics"

  /**
   * Calculate summary statistics of the specified column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments Input specification for column summary statistics.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ColumnSummaryStatisticsArgs)(implicit invocation: Invocation): ColumnSummaryStatisticsReturn = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val frameRef: FrameReference = arguments.frame
    val frame = frames.expectFrame(frameRef)
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val valueDataType: DataType = frame.schema.columnTuples(columnIndex)._2
    val usePopulationVariance = arguments.usePopulationVariance.getOrElse(false)
    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columnTuples(weightsColumnIndex)._2))
    }

    // run the operation and return the results
    val rdd = frames.loadLegacyFrameRdd(sc, frameRef)
    ColumnStatistics.columnSummaryStatistics(columnIndex,
      valueDataType,
      weightsColumnIndexOption,
      weightsDataTypeOption,
      rdd,
      usePopulationVariance)
  }
}
