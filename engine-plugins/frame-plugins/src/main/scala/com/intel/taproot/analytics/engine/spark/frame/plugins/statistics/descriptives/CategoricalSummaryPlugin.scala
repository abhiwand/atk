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

package com.intel.taproot.analytics.engine.spark.frame.plugins.statistics.descriptives

import com.intel.taproot.analytics.domain.frame._
import com.intel.taproot.analytics.engine.plugin.{ Invocation, PluginDoc }
import com.intel.taproot.analytics.engine.spark.frame.SparkFrameData
import com.intel.taproot.analytics.engine.spark.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._
import com.intel.taproot.analytics.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "Compute the categorical summary for specified columns in a frame.",
  extended = "",
  returns = "")
class CategoricalSummaryPlugin extends SparkCommandPlugin[CategoricalSummaryArgs, CategoricalSummaryReturn] {

  /**
   * The name of the command, e.g. frame/categorical_summary
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/categorical_summary"

  /**
   * Calculate categorical summary of the specified column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments Input specification for categorical  summary.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CategoricalSummaryArgs)(implicit invocation: Invocation): CategoricalSummaryReturn = {

    val sourceFrame: SparkFrameData = resolve(arguments.frame)
    val sourceRdd = sourceFrame.data

    val default_top_k = configuration.getInt("top_k")
    val default_threshold = configuration.getDouble("threshold")

    // Select each column and invoke summary statistics
    val selectedRdds: List[(FrameRdd, CategoricalColumnInput)] =
      arguments.columnInput.map(elem => (sourceRdd.selectColumn(elem.column), elem))

    val output = for { rdd <- selectedRdds }
      yield CategoricalSummaryImpl.getSummaryStatistics(
      rdd._1,
      sourceFrame.meta.rowCount.get.asInstanceOf[Double],
      rdd._2.topK,
      rdd._2.threshold,
      default_top_k,
      default_threshold)

    CategoricalSummaryReturn(output)
  }

}

