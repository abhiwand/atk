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

package com.intel.intelanalytics.engine.spark.frame.plugins.groupby

import com.intel.intelanalytics.domain.frame.{ GroupByArgs, FrameEntity }
import com.intel.intelanalytics.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import com.intel.intelanalytics.engine.spark.plugin.{ SparkCommandPlugin }
import com.intel.intelanalytics.domain.CreateEntityArgs

// Implicits needed for JSON conversion
import spray.json._
import com.intel.intelanalytics.domain.DomainJsonProtocol._

/**
 * Create a Summarized Frame with Aggregations (Avg, Count, Max, Min, Mean, Sum, Stdev, ...)
 */
@PluginDoc(oneLine = "Summarized Frame with Aggregations.",
  extended = "Create a Summarized Frame with Aggregations (Avg, Count, Max, Min, Mean, Sum, Stdev, ...).",
  returns = "Summarized Frame.")
class GroupByPlugin extends SparkCommandPlugin[GroupByArgs, FrameEntity] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/group_by"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * Create a Summarized Frame with Aggregations (Avg, Count, Max, Min, Mean, Sum, Stdev, ...)
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: GroupByArgs)(implicit invocation: Invocation): FrameEntity = {
    // dependencies (later to be replaced with dependency injection)
    val frames = engine.frames

    // validate arguments
    val originalFrame = frames.loadFrameData(sc, frames.expectFrame(arguments.frame))
    val frameSchema = originalFrame.frameSchema
    val groupByColumns = arguments.groupByColumns.map(columnName => frameSchema.column(columnName))

    // run the operation and save results
    val groupByRdd = GroupByAggregationFunctions.aggregation(originalFrame, groupByColumns, arguments.aggregations)

    frames.tryNewFrame(CreateEntityArgs(description = Some("created by group_by command"))) {
      newFrame => frames.saveFrameData(newFrame.toReference, groupByRdd)
    }
  }
}
import com.intel.intelanalytics.engine.plugin.{ PluginDoc, ArgDoc }
